import os
import json
import argparse
import sqlite3
from typing import Optional, Dict, Any, List

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from neo4j import GraphDatabase


# -----------------------------
# Neo4j
# -----------------------------
def neo4j_connect(uri: str, user: str, password: str):
    return GraphDatabase.driver(uri, auth=(user, password))


# -----------------------------
# SQLite mapping (eid -> node_id)
# -----------------------------
def init_sqlite_map(sqlite_path: str):
    os.makedirs(os.path.dirname(sqlite_path), exist_ok=True)
    conn = sqlite3.connect(sqlite_path)
    cur = conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA synchronous=NORMAL;")
    cur.execute("PRAGMA temp_store=MEMORY;")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS id_map (
            eid TEXT PRIMARY KEY,
            node_id INTEGER NOT NULL
        );
    """)
    conn.commit()
    return conn


def insert_id_map(conn: sqlite3.Connection, pairs: List[tuple]):
    # pairs: [(eid, node_id), ...]
    cur = conn.cursor()
    cur.executemany("INSERT OR REPLACE INTO id_map(eid, node_id) VALUES (?, ?);", pairs)
    conn.commit()


def lookup_node_ids(conn: sqlite3.Connection, eids: List[str]) -> Dict[str, int]:
    # returns dict for found eids
    if not eids:
        return {}
    cur = conn.cursor()
    # chunk to avoid SQLite variable limit
    out: Dict[str, int] = {}
    CH = 800
    for i in range(0, len(eids), CH):
        chunk = eids[i:i+CH]
        q_marks = ",".join(["?"] * len(chunk))
        cur.execute(f"SELECT eid, node_id FROM id_map WHERE eid IN ({q_marks});", chunk)
        for eid, nid in cur.fetchall():
            out[eid] = nid
    return out


# -----------------------------
# Parquet writers (append)
# -----------------------------
def parquet_append(writer_holder: Dict[str, pq.ParquetWriter], path: str, table: pa.Table, **writer_kwargs):
    """
    writer_holder: dict that holds writer by path.
    Creates ParquetWriter on first append, then appends.
    """
    if path not in writer_holder:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        writer_holder[path] = pq.ParquetWriter(path, table.schema, **writer_kwargs)
    writer_holder[path].write_table(table)


def close_writers(writer_holder: Dict[str, pq.ParquetWriter]):
    for w in writer_holder.values():
        w.close()
    writer_holder.clear()


# -----------------------------
# Helpers
# -----------------------------
def flatten_props_df(props: pd.Series, keep_keys: Optional[List[str]] = None, prefix: str = "") -> pd.DataFrame:
    if keep_keys is None:
        norm = pd.json_normalize(props)
        return norm.add_prefix(prefix) if prefix else norm
    data = {}
    for k in keep_keys:
        col = props.apply(lambda d: d.get(k, np.nan) if isinstance(d, dict) else np.nan)
        data[f"{prefix}{k}"] = col
    return pd.DataFrame(data)


# -----------------------------
# Cursor-based fetchers (NO SKIP)
# -----------------------------
NODE_QUERY_CURSOR = """
MATCH (n)
WHERE elementId(n) > $cursor
RETURN elementId(n) AS neo_eid, labels(n) AS labels, properties(n) AS props
ORDER BY neo_eid
LIMIT $limit
"""

EDGE_QUERY_CURSOR = """
MATCH (a)-[r]->(b)
WHERE id(r) > $cursor
RETURN id(r) AS rel_id,
       elementId(a) AS src_eid,
       elementId(b) AS dst_eid
ORDER BY rel_id
LIMIT $limit
"""


def export_nodes_stream(
    driver,
    out_dir: str,
    sqlite_conn: sqlite3.Connection,
    batch_size: int,
    node_prop_keys: Optional[List[str]],
    database: Optional[str] = None,
    compression: str = "zstd",
):
    writers: Dict[str, pq.ParquetWriter] = {}

    nodes_parts_path = os.path.join(out_dir, "nodes_parts", "nodes.parquet")
    idmap_parts_path = os.path.join(out_dir, "id_map_parts", "id_map.parquet")

    cursor = ""
    next_node_id = 0
    total = 0

    with driver.session(database=database) as session:
        while True:
            res = session.run(NODE_QUERY_CURSOR, cursor=cursor, limit=batch_size)
            batch = [r.data() for r in res]
            try:
                res.consume()
            except Exception:
                pass

            if not batch:
                break

            df = pd.DataFrame(batch)
            # assign contiguous node ids in the same order we receive (ORDER BY neo_eid)
            n = len(df)
            df["node_id"] = np.arange(next_node_id, next_node_id + n, dtype=np.int64)
            next_node_id += n
            total += n

            # node_type = first label
            df["node_type"] = df["labels"].apply(lambda ls: ls[0] if isinstance(ls, list) and ls else "")

            # optional flattened props
            feat_df = flatten_props_df(df["props"], keep_keys=node_prop_keys, prefix="x_") if node_prop_keys else pd.DataFrame()

            out_nodes = pd.concat([df[["node_id", "neo_eid", "node_type"]], feat_df], axis=1)

            # write nodes chunk
            table_nodes = pa.Table.from_pandas(out_nodes, preserve_index=False)
            parquet_append(
                writers,
                nodes_parts_path,
                table_nodes,
                compression=compression,
                use_dictionary=True,
            )

            # write id_map chunk (neo_eid -> node_id) also as parquet (conveniente para joins externos)
            out_idmap = df[["node_id", "neo_eid"]]
            table_idmap = pa.Table.from_pandas(out_idmap, preserve_index=False)
            parquet_append(
                writers,
                idmap_parts_path,
                table_idmap,
                compression=compression,
                use_dictionary=True,
            )

            # insert into sqlite for edge mapping later
            pairs = list(zip(df["neo_eid"].astype(str).tolist(), df["node_id"].astype(int).tolist()))
            insert_id_map(sqlite_conn, pairs)

            cursor = str(df["neo_eid"].iloc[-1])
            print(f"[nodes] total={total:,} last_cursor={cursor}", flush=True)

    close_writers(writers)
    return total


def export_edges_stream(
    driver,
    out_dir: str,
    sqlite_conn: sqlite3.Connection,
    batch_size: int,
    database: Optional[str] = None,
    compression: str = "zstd",
    undirected: bool = False,
    self_loops: bool = False,
    dedup_edges: bool = False,
    num_nodes: Optional[int] = None,
):
    writers: Dict[str, pq.ParquetWriter] = {}
    edges_parts_path = os.path.join(out_dir, "edges_parts", "edges.parquet")

    cursor = ""
    total = 0

    # for optional dedup (lightweight): keep seen set can blow RAM; so dedup_edges here is "chunk-level dedup"
    # If you need global dedup, do it later with DuckDB/Polars.
    with driver.session(database=database) as session:
        while True:
            res = session.run(EDGE_QUERY_CURSOR, cursor=cursor, limit=batch_size)
            batch = [r.data() for r in res]
            try:
                res.consume()
            except Exception:
                pass

            if not batch:
                break

            df = pd.DataFrame(batch)
            cursor = str(df["rel_eid"].iloc[-1])

            # map src/dst via sqlite
            src_eids = df["src_eid"].astype(str).tolist()
            dst_eids = df["dst_eid"].astype(str).tolist()
            need = list(dict.fromkeys(src_eids + dst_eids))  # unique preserve order
            m = lookup_node_ids(sqlite_conn, need)

            df["src"] = [m.get(e) for e in src_eids]
            df["dst"] = [m.get(e) for e in dst_eids]
            df = df.dropna(subset=["src", "dst"]).copy()
            df["src"] = df["src"].astype(np.int64)
            df["dst"] = df["dst"].astype(np.int64)

            edges = df[["src", "dst"]]

            if undirected:
                rev = edges.rename(columns={"src": "dst", "dst": "src"})
                edges = pd.concat([edges, rev], ignore_index=True)

            if self_loops:
                if num_nodes is None:
                    raise ValueError("self_loops=True requires num_nodes.")
                loops = pd.DataFrame({"src": np.arange(num_nodes, dtype=np.int64),
                                      "dst": np.arange(num_nodes, dtype=np.int64)})
                edges = pd.concat([edges, loops], ignore_index=True)

            if dedup_edges:
                edges = edges.drop_duplicates()

            total += len(edges)
            table_edges = pa.Table.from_pandas(edges, preserve_index=False)

            parquet_append(
                writers,
                edges_parts_path,
                table_edges,
                compression=compression,
                use_dictionary=True,
            )

            print(f"[edges] total={total:,} last_cursor={cursor}", flush=True)

    close_writers(writers)
    return total


# -----------------------------
# Optional: merge parts to single parquet
# -----------------------------
def merge_to_single_parquet(in_parts_file: str, out_file: str, compression: str = "zstd"):
    """
    in_parts_file: path to the appended parquet file produced above (already a single file).
    If instead you have many files in a folder, use merge_folder_to_single_parquet().
    """
    # If your export used a single appended file (as in this script), this is unnecessary.
    # Kept for symmetry.
    os.makedirs(os.path.dirname(out_file), exist_ok=True)
    table = pq.read_table(in_parts_file)
    pq.write_table(table, out_file, compression=compression)


def merge_folder_to_single_parquet(folder: str, out_file: str, compression: str = "zstd"):
    """
    Merge multiple parquet files in a folder into one parquet file.
    """
    import glob
    files = sorted(glob.glob(os.path.join(folder, "*.parquet")))
    if not files:
        raise FileNotFoundError(f"No parquet files found in {folder}")

    os.makedirs(os.path.dirname(out_file), exist_ok=True)

    writer = None
    try:
        for f in files:
            t = pq.read_table(f)
            if writer is None:
                writer = pq.ParquetWriter(out_file, t.schema, compression=compression, use_dictionary=True)
            writer.write_table(t)
    finally:
        if writer is not None:
            writer.close()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--uri", required=True)
    p.add_argument("--user", required=True)
    p.add_argument("--password", required=True)
    p.add_argument("--database", default=None)
    p.add_argument("--out_dir", required=True)

    p.add_argument("--node_batch", type=int, default=50_000)
    p.add_argument("--edge_batch", type=int, default=200_000)
    p.add_argument("--compression", default="zstd")

    p.add_argument("--node_prop_keys", default="")  # comma-separated
    p.add_argument("--undirected", action="store_true")
    p.add_argument("--self_loops", action="store_true")
    p.add_argument("--dedup_edges", action="store_true")

    args = p.parse_args()
    os.makedirs(args.out_dir, exist_ok=True)

    node_keys = [k.strip() for k in args.node_prop_keys.split(",") if k.strip()] or None

    sqlite_path = os.path.join(args.out_dir, "id_map.sqlite")
    sqlite_conn = init_sqlite_map(sqlite_path)

    driver = neo4j_connect(args.uri, args.user, args.password)

    try:
        print("== Exporting nodes (cursor, elementId) ==", flush=True)
        num_nodes = export_nodes_stream(
            driver=driver,
            out_dir=args.out_dir,
            sqlite_conn=sqlite_conn,
            batch_size=args.node_batch,
            node_prop_keys=node_keys,
            database=args.database,
            compression=args.compression,
        )

        print("== Exporting edges (cursor, elementId) ==", flush=True)
        num_edges = export_edges_stream(
            driver=driver,
            out_dir=args.out_dir,
            sqlite_conn=sqlite_conn,
            batch_size=args.edge_batch,
            database=args.database,
            compression=args.compression,
            undirected=args.undirected,
            self_loops=args.self_loops,
            dedup_edges=args.dedup_edges,
            num_nodes=num_nodes if args.self_loops else None,
        )

        meta = {
            "database": args.database,
            "num_nodes": int(num_nodes),
            "num_edges": int(num_edges),
            "compression": args.compression,
            "undirected": bool(args.undirected),
            "self_loops": bool(args.self_loops),
            "dedup_edges": bool(args.dedup_edges),
            "node_prop_keys": node_keys or [],
            "notes": "nodes_parts/nodes.parquet and edges_parts/edges.parquet are appended parquet files (single file each). id_map.sqlite stores neo_eid->node_id.",
        }
        with open(os.path.join(args.out_dir, "meta.json"), "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=2)

        print("✅ Export complete", flush=True)
        print(f"Out: {args.out_dir}", flush=True)
        print(f"N/E: {num_nodes:,} / {num_edges:,}", flush=True)
        print(f"SQLite id_map: {sqlite_path}", flush=True)

    finally:
        try:
            sqlite_conn.close()
        except Exception:
            pass
        driver.close()


if __name__ == "__main__":
    main()

"""
python neo4j_to_parquet_stream.py \
  --uri bolt://localhost:7687 \
  --user neo4j \
  --password "Banco.69" \
  --database saml-d \
  --out_dir ../../graph_export_stream \
  --node_batch 50000 \
  --edge_batch 200000 \
  --compression zstd

"""
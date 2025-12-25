import os
import json
import argparse
from typing import List, Tuple, Optional

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from neo4j import GraphDatabase


def neo4j_connect(uri: str, user: str, password: str):
    return GraphDatabase.driver(uri, auth=(user, password))


def fetch_all_nodes(
    driver,
    node_query: str,
    batch_size: int = 50_000,
    database: Optional[str] = None,
) -> pd.DataFrame:
    rows = []
    skip = 0
    with driver.session(database=database) as session:
        while True:
            q = f"""{node_query} SKIP $skip LIMIT $limit"""
            result = session.run(q, skip=skip, limit=batch_size)
            batch = [r.data() for r in result]

            # 🔇 limpia warnings deprecados
            try:
                result.consume()
            except Exception:
                pass

            if not batch:
                break
            rows.extend(batch)
            skip += batch_size
            print(f"[nodes] acumulados={len(rows):,} (último batch={len(batch):,})", flush=True)

    df = pd.DataFrame(rows)
    if df.empty:
        raise RuntimeError("No nodes returned. Check node_query and database.")
    if "neo_id" not in df.columns:
        raise RuntimeError("node_query must return column 'neo_id'.")
    if "labels" not in df.columns:
        df["labels"] = [[] for _ in range(len(df))]
    if "props" not in df.columns:
        df["props"] = [{} for _ in range(len(df))]
    return df


def fetch_all_edges(
    driver,
    edge_query: str,
    batch_size: int = 200_000,
    database: Optional[str] = None,
) -> pd.DataFrame:
    rows = []
    skip = 0
    with driver.session(database=database) as session:
        while True:
            q = f"""{edge_query} SKIP $skip LIMIT $limit"""
            result = session.run(q, skip=skip, limit=batch_size)
            batch = [r.data() for r in result]

            # 🔇 limpia warnings deprecados
            try:
                result.consume()
            except Exception:
                pass

            if not batch:
                break
            rows.extend(batch)
            skip += batch_size
            print(f"[edges] acumulados={len(rows):,} (último batch={len(batch):,})", flush=True)

    df = pd.DataFrame(rows)
    if df.empty:
        raise RuntimeError("No edges returned. Check edge_query and database.")
    if "src_neo" not in df.columns or "dst_neo" not in df.columns:
        raise RuntimeError("edge_query must return columns 'src_neo' and 'dst_neo'.")
    if "rel_type" not in df.columns:
        df["rel_type"] = ""
    if "props" not in df.columns:
        df["props"] = [{} for _ in range(len(df))]
    return df


def flatten_props(props_series: pd.Series, keep_keys: Optional[List[str]] = None, prefix: str = "") -> pd.DataFrame:
    if keep_keys is not None:
        data = {
            f"{prefix}{k}": props_series.apply(lambda d: d.get(k, np.nan) if isinstance(d, dict) else np.nan)
            for k in keep_keys
        }
        return pd.DataFrame(data)
    normed = pd.json_normalize(props_series)
    return normed.add_prefix(prefix) if prefix else normed


def build_csr(num_nodes: int, src: np.ndarray, dst: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
    order = np.argsort(src, kind="mergesort")
    src_sorted = src[order]
    dst_sorted = dst[order]

    indptr = np.zeros(num_nodes + 1, dtype=np.int64)
    np.add.at(indptr, src_sorted + 1, 1)
    np.cumsum(indptr, out=indptr)

    indices = dst_sorted.astype(np.int64, copy=False)
    return indptr, indices


def make_undirected(src: np.ndarray, dst: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
    return np.concatenate([src, dst]), np.concatenate([dst, src])


def add_self_loops(num_nodes: int, src: np.ndarray, dst: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
    loops = np.arange(num_nodes, dtype=np.int64)
    return np.concatenate([src, loops]), np.concatenate([dst, loops])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--uri", required=True, help="bolt://localhost:7687")
    parser.add_argument("--user", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--database", default=None, help="DB name (Neo4j 4/5). Ej: neo4j. Default si se omite.")
    parser.add_argument("--out_dir", required=True)

    parser.add_argument(
        "--node_query",
        default="MATCH (n) RETURN id(n) AS neo_id, labels(n) AS labels, properties(n) AS props",
    )
    parser.add_argument(
        "--edge_query",
        default="MATCH (a)-[r]->(b) RETURN id(a) AS src_neo, id(b) AS dst_neo, type(r) AS rel_type, properties(r) AS props",
    )

    parser.add_argument("--node_batch", type=int, default=50_000)
    parser.add_argument("--edge_batch", type=int, default=200_000)

    parser.add_argument("--node_prop_keys", default="")
    parser.add_argument("--edge_prop_keys", default="")

    parser.add_argument("--undirected", action="store_true")
    parser.add_argument("--self_loops", action="store_true")
    parser.add_argument("--dedup_edges", action="store_true")

    args = parser.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)

    node_keys = [k.strip() for k in args.node_prop_keys.split(",") if k.strip()] or None
    edge_keys = [k.strip() for k in args.edge_prop_keys.split(",") if k.strip()] or None

    driver = neo4j_connect(args.uri, args.user, args.password)

    try:
        nodes_df = fetch_all_nodes(driver, args.node_query, args.node_batch, database=args.database)
        nodes_df = nodes_df.drop_duplicates(subset=["neo_id"]).reset_index(drop=True)
        nodes_df["node_id"] = np.arange(len(nodes_df), dtype=np.int64)

        id_map = dict(zip(nodes_df["neo_id"].astype(np.int64), nodes_df["node_id"].astype(np.int64)))

        node_feat_df = flatten_props(nodes_df["props"], keep_keys=node_keys, prefix="x_") if node_keys else pd.DataFrame()
        nodes_df["node_type"] = nodes_df["labels"].apply(lambda ls: ls[0] if isinstance(ls, list) and ls else "")

        out_nodes = pd.concat([nodes_df[["node_id", "neo_id", "node_type"]], node_feat_df], axis=1)

        edges_df = fetch_all_edges(driver, args.edge_query, args.edge_batch, database=args.database)
        edges_df["src"] = edges_df["src_neo"].map(id_map)
        edges_df["dst"] = edges_df["dst_neo"].map(id_map)
        edges_df = edges_df.dropna(subset=["src", "dst"]).reset_index(drop=True)
        edges_df["src"] = edges_df["src"].astype(np.int64)
        edges_df["dst"] = edges_df["dst"].astype(np.int64)

        # si quieres features de arista, aquí:
        # edge_feat_df = flatten_props(edges_df["props"], keep_keys=edge_keys, prefix="e_") if edge_keys else pd.DataFrame()

        src = edges_df["src"].to_numpy(np.int64, copy=False)
        dst = edges_df["dst"].to_numpy(np.int64, copy=False)

        if args.undirected:
            src, dst = make_undirected(src, dst)

        num_nodes = len(out_nodes)
        if args.self_loops:
            src, dst = add_self_loops(num_nodes, src, dst)

        if args.dedup_edges:
            tmp = pd.DataFrame({"src": src, "dst": dst}).drop_duplicates()
            src = tmp["src"].to_numpy(np.int64)
            dst = tmp["dst"].to_numpy(np.int64)

        edges_final = pd.DataFrame({"src": src, "dst": dst})

        pq.write_table(pa.Table.from_pandas(out_nodes, preserve_index=False), os.path.join(args.out_dir, "nodes.parquet"))
        pq.write_table(pa.Table.from_pandas(edges_final, preserve_index=False), os.path.join(args.out_dir, "edges.parquet"))
        pq.write_table(pa.Table.from_pandas(out_nodes[["node_id", "neo_id"]], preserve_index=False),
                       os.path.join(args.out_dir, "id_map.parquet"))

        indptr, indices = build_csr(num_nodes, src, dst)
        np.save(os.path.join(args.out_dir, "csr_indptr.npy"), indptr)
        np.save(os.path.join(args.out_dir, "csr_indices.npy"), indices)

        meta = {
            "database": args.database,
            "num_nodes": int(num_nodes),
            "num_edges": int(len(indices)),
            "undirected": bool(args.undirected),
            "self_loops": bool(args.self_loops),
            "node_prop_keys": node_keys or [],
            "edge_prop_keys": edge_keys or [],
        }
        with open(os.path.join(args.out_dir, "meta.json"), "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=2)

        print("✅ Export complete")
        print(f"DB:    {args.database or '(default)'}")
        print(f"N/E:   {num_nodes} / {len(indices)}")
        print(f"Out:   {args.out_dir}")

    finally:
        driver.close()


if __name__ == "__main__":
    main()


""" Uso
python neo4j_to_parquet_and_csr.py \
  --uri bolt://localhost:7687 \
  --user neo4j \
  --password "TU_PASSWORD" \
  --out_dir ./graph_export \
  --database "TU_BASE_DE_DATOS"
"""

""" Grafo no dirigido y self loops
python neo4j_to_parquet_and_csr.py \
  --uri bolt://localhost:7687 \
  --user neo4j \
  --password "TU_PASSWORD" \
  --out_dir ./graph_export \
  --undirected \
  --self_loops
"""
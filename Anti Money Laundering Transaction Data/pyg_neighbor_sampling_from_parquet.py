import os
import argparse
import numpy as np
import pandas as pd
import torch

from torch_geometric.data import Data
from torch_geometric.loader import NeighborLoader


def load_graph_from_parquet(nodes_path: str, edges_path: str, feat_cols=None, device="cpu"):
    nodes = pd.read_parquet(nodes_path)
    edges = pd.read_parquet(edges_path)

    # Sanity: node_id must be 0..N-1
    node_id = nodes["node_id"].to_numpy()
    n = len(nodes)
    if node_id.min() != 0 or node_id.max() != n - 1 or len(np.unique(node_id)) != n:
        raise ValueError("node_id is not contiguous 0..N-1 (or has duplicates).")

    # edge_index
    src = edges["src"].to_numpy(dtype=np.int64, copy=False)
    dst = edges["dst"].to_numpy(dtype=np.int64, copy=False)
    edge_index = torch.from_numpy(np.vstack([src, dst])).long()

    # node features: pick numeric columns (or user specified)
    if feat_cols is None:
        ignore = {"node_id", "neo_eid", "node_type"}
        cand = [c for c in nodes.columns if c not in ignore]
        # keep numeric only
        feat_cols = [c for c in cand if pd.api.types.is_numeric_dtype(nodes[c])]

    if feat_cols:
        x = torch.tensor(nodes[feat_cols].to_numpy(dtype=np.float32, copy=False))
    else:
        # fallback: constant feature
        x = torch.ones((n, 1), dtype=torch.float32)

    data = Data(x=x, edge_index=edge_index, num_nodes=n)
    return data.to(device), feat_cols


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--out_dir", required=True, help="export folder")
    p.add_argument("--nodes", default="nodes_parts/nodes.parquet")
    p.add_argument("--edges", default="edges_parts/edges.parquet")
    p.add_argument("--batch_size", type=int, default=1024)
    p.add_argument("--num_neighbors", default="15,10", help="comma list per hop, e.g. 15,10")
    p.add_argument("--seed", type=int, default=123)
    p.add_argument("--device", default="cpu")

    args = p.parse_args()
    nodes_path = os.path.join(args.out_dir, args.nodes)
    edges_path = os.path.join(args.out_dir, args.edges)

    num_neighbors = [int(x.strip()) for x in args.num_neighbors.split(",") if x.strip()]

    torch.manual_seed(args.seed)
    np.random.seed(args.seed)

    data, feat_cols = load_graph_from_parquet(nodes_path, edges_path, feat_cols=None, device=args.device)
    print(f"Loaded graph: num_nodes={data.num_nodes:,} num_edges={data.edge_index.size(1):,}")
    print(f"Using features: {feat_cols if feat_cols else '[constant ones]'}")

    # Deterministic input nodes for sampling:
    # choose the first K node_ids so the batch content is reproducible
    input_nodes = torch.arange(min(data.num_nodes, 50000), device=data.x.device)

    loader = NeighborLoader(
        data,
        input_nodes=input_nodes,
        num_neighbors=num_neighbors,
        batch_size=args.batch_size,
        shuffle=False,  # deterministic order
        num_workers=0,
        persistent_workers=False,
    )

    # Show first 3 batches (human-friendly)
    for i, batch in enumerate(loader):
        # batch.n_id are global node ids included in the sampled subgraph
        # batch.edge_index uses local indexing; batch.n_id maps local->global
        print(f"\nBatch {i}:")
        print(f"  seed nodes in batch: {batch.batch_size}")
        print(f"  sampled subgraph nodes: {batch.num_nodes}")
        print(f"  sampled subgraph edges: {batch.edge_index.size(1)}")
        print(f"  first 10 global node ids in subgraph: {batch.n_id[:10].tolist()}")
        if i >= 2:
            break


if __name__ == "__main__":
    main()

"""
python pyg_neighbor_sampling_from_parquet.py \
  --out_dir ../../graph_export \
  --batch_size 512 \
  --num_neighbors 15,10 \
  --seed 7

"""
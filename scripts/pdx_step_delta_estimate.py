#!/usr/bin/env python3
"""Estimate the .pdx step-table size under delta + zigzag + varint encoding,
for several candidate node orderings.

Usage:
    python3 scripts/pdx_step_delta_estimate.py graph.gfa.gz.pdx [--ndx graph.gfa.gz.ndx] [--block 128]

Requires numpy.

The input must store packed uint32 steps: any v4 .pdx, or a v5 .pdx built with
--step_encoding packed32. With a legacy .ndx the node ids are FNV-1a hash
ranks, so consecutive steps on a path have essentially random ids; with an
.ndx v2 they are already grouped by community and locality key, and the
"current" row predicts the delta_varint step table that index_gfa writes
(which uses a cheaper 0.018 B/step block index than the 8 bytes per block
charged here).

For each ordering this script remaps every step to the new node id, computes
the zigzag delta to the previous step, packs the strand into the low bit, and
counts LEB128 bytes. Blocks are global over the step table: every `--block`
steps the value is stored absolute (delta from 0) and one uint64 byte offset is
charged per block, which keeps O(block) random access by global step index.

Orderings evaluated (when the inputs allow it):
  current           the .pdx node ids (hash ranks for a legacy .ndx)
  first_visit       order of first appearance in the step table
  comm+first_visit  community id, then first appearance   (needs .ndx)
  name              numeric segment name                  (needs numeric names)
  comm+name         community id, then numeric name       (needs .ndx + numeric names)
  mincomm+name      communities ordered by smallest name, then name
"""

from __future__ import annotations

import argparse
import os
import struct

import numpy as np

HEADER_STRUCT = struct.Struct("<8sIIQQQQQQQQQQ")
HEADER_V5_STRUCT = struct.Struct("<QII")  # node_id_space, step and posting encodings
MAGIC = b"GFPATH1\x00"
SUPPORTED_VERSIONS = (4, 5)
NODE_MASK = 0x7FFFFFFF
NDX_V2_MAGIC = b"GFANDX02"
NDX_V2_HEADER_STRUCT = struct.Struct("<8sIIQQQQQQ")

NODE_DT = np.dtype([("name_offset", "<u8"), ("name_len", "<u8"),
                    ("posting_begin", "<u8"), ("posting_count", "<u8")])
NDX_DT = np.dtype([("hash", "<u8"), ("hash32", "<u4"), ("comm", "<u4")])
VARINT_LIMITS = np.array([1 << 7, 1 << 14, 1 << 21, 1 << 28, 1 << 35], dtype=np.uint64)


def fmt_bytes(value: float) -> str:
    for unit in ["B", "KiB", "MiB", "GiB", "TiB"]:
        if value < 1024.0 or unit == "TiB":
            return f"{value:.0f} {unit}" if unit == "B" else f"{value:.2f} {unit}"
        value /= 1024.0
    return f"{value:.2f} TiB"


def read_header(path: str) -> dict:
    with open(path, "rb") as handle:
        raw = handle.read(HEADER_STRUCT.size + HEADER_V5_STRUCT.size)
    fields = HEADER_STRUCT.unpack(raw[:HEADER_STRUCT.size])
    names = ["magic", "version", "reserved", "path_count", "node_count", "step_count",
             "posting_count", "path_table_offset", "node_table_offset", "step_table_offset",
             "posting_table_offset", "strings_offset", "strings_size"]
    header = dict(zip(names, fields))
    if header["magic"] != MAGIC:
        raise SystemExit("Invalid .pdx magic")
    if header["version"] not in SUPPORTED_VERSIONS:
        raise SystemExit(f"Unsupported .pdx version {header['version']}")
    if header["version"] == 5:
        _, step_encoding, _ = HEADER_V5_STRUCT.unpack(raw[HEADER_STRUCT.size:])
        if step_encoding != 0:
            raise SystemExit("This .pdx stores delta_varint steps; rebuild it with "
                             "--step_encoding packed32 to estimate other orderings")
    return header


def community_by_node(ndx_path: str, n_nodes: int) -> np.ndarray:
    """Community id per .pdx node id, for legacy and v2 .ndx files."""
    size = os.path.getsize(ndx_path)
    if size % 16 == 0:
        ndx = np.memmap(ndx_path, dtype=NDX_DT, mode="r")
        if len(ndx) != n_nodes:
            raise SystemExit(f".ndx has {len(ndx)} entries but .pdx has {n_nodes} nodes")
        return np.asarray(ndx["comm"]).astype(np.int64)
    with open(ndx_path, "rb") as handle:
        raw = handle.read(NDX_V2_HEADER_STRUCT.size)
    magic, version, _, count, ncom, _, starts_offset, _, _ = NDX_V2_HEADER_STRUCT.unpack(raw)
    if magic != NDX_V2_MAGIC or version != 2 or count != n_nodes:
        raise SystemExit(f"{ndx_path} is not a v2 .ndx for this .pdx")
    starts = np.fromfile(ndx_path, dtype="<u4", count=ncom + 1, offset=starts_offset).astype(np.int64)
    return np.repeat(np.arange(ncom, dtype=np.int64), np.diff(starts))


def varint_bytes(values: np.ndarray) -> np.ndarray:
    """LEB128 byte length for each non-negative value."""
    out = np.ones(values.shape, dtype=np.int64)
    for limit in VARINT_LIMITS:
        out += values >= limit
    return out


def first_visit_rank(steps: np.memmap, node_count: int, chunk: int) -> np.ndarray:
    """Global step index of each node's first occurrence (step_count if never visited)."""
    first = np.full(node_count, np.iinfo(np.int64).max, dtype=np.int64)
    for lo in range(0, len(steps), chunk):
        ids = (np.asarray(steps[lo:lo + chunk]) & NODE_MASK).astype(np.int64)
        uniq, idx = np.unique(ids, return_index=True)
        unseen = first[uniq] == np.iinfo(np.int64).max
        first[uniq[unseen]] = idx[unseen] + lo
    return first


def numeric_names(pdx: str, header: dict, chunk: int = 5_000_000) -> np.ndarray | None:
    """Parse node names as integers; return None if any name is non-numeric."""
    n = header["node_count"]
    nodes = np.memmap(pdx, dtype=NODE_DT, mode="r",
                      offset=header["node_table_offset"], shape=(n,))
    blob = np.memmap(pdx, dtype=np.uint8, mode="r",
                     offset=header["strings_offset"], shape=(max(header["strings_size"], 1),))
    out = np.empty(n, dtype=np.int64)
    for lo in range(0, n, chunk):
        rec = np.asarray(nodes[lo:lo + chunk])
        off = rec["name_offset"].astype(np.int64)
        ln = rec["name_len"].astype(np.int64)
        maxlen = int(ln.max()) if len(ln) else 0
        if maxlen == 0 or maxlen > 18 or (ln == 0).any():
            return None
        cols = np.arange(maxlen, dtype=np.int64)
        valid = cols[None, :] < ln[:, None]
        idx = np.minimum(off[:, None] + cols[None, :], len(blob) - 1)
        digits = blob[idx].astype(np.int64) - 48
        if ((digits < 0) | (digits > 9))[valid].any():
            return None
        val = np.zeros(len(rec), dtype=np.int64)
        for j in range(maxlen):
            val = np.where(valid[:, j], val * 10 + digits[:, j], val)
        out[lo:lo + chunk] = val
    return out


def order_to_new_id(order: np.ndarray) -> np.ndarray:
    """Convert a permutation (new position -> old rank) into old rank -> new id."""
    new_id = np.empty(len(order), dtype=np.int64)
    new_id[order] = np.arange(len(order), dtype=np.int64)
    return new_id


def encoded_cost(steps: np.memmap, new_id: np.ndarray | None, block: int, chunk: int) -> dict:
    hist = np.zeros(6, dtype=np.int64)
    small = 0
    prev_last = 0
    n = len(steps)
    for lo in range(0, n, chunk):
        raw = np.asarray(steps[lo:lo + chunk]).astype(np.int64)
        ids = raw & NODE_MASK
        if new_id is not None:
            ids = new_id[ids]
        rev = raw >> 31
        prev = np.empty_like(ids)
        prev[0] = prev_last
        prev[1:] = ids[:-1]
        if block > 0:
            first = (-lo) % block
            prev[first::block] = 0          # block starts are stored absolute
        elif lo == 0:
            prev[0] = 0
        d = ids - prev
        z = ((d << 1) ^ (d >> 63)).astype(np.uint64)
        v = (z << np.uint64(1)) | rev.astype(np.uint64)
        b = varint_bytes(v)
        hist += np.bincount(b, minlength=6)[:6]
        small += int((np.abs(d) <= 31).sum())
        prev_last = int(ids[-1])
    step_bytes = int((hist * np.arange(6)).sum())
    ckpt_bytes = (n + block - 1) // block * 8 if block > 0 else 0
    return {"step_bytes": step_bytes, "ckpt_bytes": ckpt_bytes, "hist": hist, "small": small}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("pdx", help="input .pdx")
    ap.add_argument("--ndx", help="matching .ndx (default: <pdx without .pdx>.ndx if present)")
    ap.add_argument("--block", type=int, default=128,
                    help="steps per random-access block; 0 = no checkpoints [128]")
    ap.add_argument("--chunk", type=int, default=1 << 25, help="steps per processing chunk")
    args = ap.parse_args()

    h = read_header(args.pdx)
    n_nodes, n_steps = h["node_count"], h["step_count"]
    if n_steps == 0:
        raise SystemExit("No steps in this .pdx")
    steps = np.memmap(args.pdx, dtype="<u4", mode="r",
                      offset=h["step_table_offset"], shape=(n_steps,))

    ndx_path = args.ndx
    if ndx_path is None and args.pdx.endswith(".pdx"):
        guess = args.pdx[:-4] + ".ndx"
        ndx_path = guess if os.path.exists(guess) else None
    comm = community_by_node(ndx_path, n_nodes) if ndx_path else None

    posting_bytes = h["strings_offset"] - h["posting_table_offset"]
    node_table_bytes = h["step_table_offset"] - h["node_table_offset"]
    print(f"File: {args.pdx}  ({fmt_bytes(os.path.getsize(args.pdx))})")
    print(f"nodes {n_nodes:,}   steps {n_steps:,}   paths {h['path_count']:,}   "
          f"steps/node {n_steps / max(n_nodes, 1):.1f}")
    print(f"current step table : {fmt_bytes(n_steps * 4)}  (4.00 B/step)")
    print(f"current postings   : {fmt_bytes(posting_bytes)}  "
          f"({posting_bytes / max(h['posting_count'], 1):.2f} B/posting)")
    print(f"current node table : {fmt_bytes(node_table_bytes)}  "
          f"({node_table_bytes / n_steps:.2f} B per step equivalent)")
    print(f"block size         : {args.block if args.block else 'none'}")
    print()

    orderings: list[tuple[str, np.ndarray | None]] = [("current", None)]
    first = first_visit_rank(steps, n_nodes, args.chunk)
    orderings.append(("first_visit", order_to_new_id(np.argsort(first, kind="stable"))))
    if comm is not None:
        orderings.append(("comm+first_visit", order_to_new_id(np.lexsort((first, comm)))))
    names = numeric_names(args.pdx, h)
    if names is not None:
        orderings.append(("name", order_to_new_id(np.argsort(names, kind="stable"))))
        if comm is not None:
            orderings.append(("comm+name", order_to_new_id(np.lexsort((names, comm)))))
            # Communities ordered by their smallest name instead of Louvain id.
            ncomm = int(comm.max()) + 1
            min_name = np.full(ncomm, np.iinfo(np.int64).max, dtype=np.int64)
            np.minimum.at(min_name, comm, names)
            comm_rank = np.empty(ncomm, dtype=np.int64)
            comm_rank[np.argsort(min_name, kind="stable")] = np.arange(ncomm, dtype=np.int64)
            orderings.append(("mincomm+name", order_to_new_id(np.lexsort((names, comm_rank[comm])))))
    else:
        print("(segment names are not all numeric; skipping name-based orderings)\n")

    print(f"{'ordering':<18}{'B/step':>8}{'total':>12}{'vs 4B':>8}{'|d|<=31':>9}"
          f"{'1B':>7}{'2B':>7}{'3B':>7}{'4B':>7}{'5B':>7}")
    for label, new_id in orderings:
        r = encoded_cost(steps, new_id, args.block, args.chunk)
        total = r["step_bytes"] + r["ckpt_bytes"]
        frac = r["hist"][1:6] / n_steps * 100
        print(f"{label:<18}{total / n_steps:>8.2f}{fmt_bytes(total):>12}"
              f"{n_steps * 4 / total:>7.2f}x{r['small'] / n_steps * 100:>8.1f}%"
              + "".join(f"{f:>6.1f}%" for f in frac))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

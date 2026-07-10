#!/usr/bin/env python3
"""Build a rank-aligned .lnx node-length sidecar for an existing gfaidx index.

The .lnx payload is a uint32 array where element i is the segment length for
node rank i in the matching .ndx file. The script only needs the indexed GFA
and the matching .ndx, so existing large graphs can get .lnx files without
rerunning gfaidx index_gfa.
"""

from __future__ import annotations

import argparse
import gzip
import mmap
import os
import struct
import sys
from pathlib import Path


MAGIC = b"GFALNX01"
VERSION = 1
VALUE_WIDTH = 4
HEADER_STRUCT = struct.Struct("<8sIIQ")
NDX_ENTRY_STRUCT = struct.Struct("<QII")
UINT32_MAX = (1 << 32) - 1


def fnv1a64(data: bytes) -> int:
    value = 1469598103934665603
    for byte in data:
        value ^= byte
        value = (value * 1099511628211) & 0xFFFFFFFFFFFFFFFF
    return value


def fnv1a32(data: bytes) -> int:
    value = 2166136261
    for byte in data:
        value ^= byte
        value = (value * 16777619) & 0xFFFFFFFF
    return value


class NodeHashIndex:
    """Mmap-backed lookup over the existing sorted .ndx hash table."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.file = path.open("rb")
        self.size_bytes = path.stat().st_size
        if self.size_bytes % NDX_ENTRY_STRUCT.size != 0:
            raise RuntimeError(f"Invalid .ndx size: {path}")
        self.count = self.size_bytes // NDX_ENTRY_STRUCT.size
        self.map = mmap.mmap(self.file.fileno(), 0, access=mmap.ACCESS_READ)

    def close(self) -> None:
        self.map.close()
        self.file.close()

    def entry(self, rank: int) -> tuple[int, int, int]:
        return NDX_ENTRY_STRUCT.unpack_from(self.map, rank * NDX_ENTRY_STRUCT.size)

    def lookup_rank(self, node_name: bytes) -> int | None:
        query_hash = fnv1a64(node_name)
        query_hash32 = fnv1a32(node_name)

        low = 0
        high = self.count
        while low < high:
            mid = low + (high - low) // 2
            mid_hash, _, _ = self.entry(mid)
            if mid_hash < query_hash:
                low = mid + 1
            elif mid_hash > query_hash:
                high = mid
            else:
                left = mid
                while left > 0 and self.entry(left - 1)[0] == query_hash:
                    left -= 1
                rank = left
                while rank < self.count:
                    current_hash, current_hash32, _ = self.entry(rank)
                    if current_hash != query_hash:
                        break
                    if current_hash32 == query_hash32:
                        return rank
                    rank += 1
                return None
        return None


def open_gfa(path: Path):
    with path.open("rb") as probe:
        magic = probe.read(2)
    if magic == b"\x1f\x8b":
        return gzip.open(path, "rb")
    return path.open("rb")


def parse_s_line(line: bytes) -> tuple[bytes, int] | None:
    fields = line.rstrip(b"\n\r").split(b"\t")
    if len(fields) < 3 or fields[0] != b"S":
        return None

    node_name = fields[1]
    sequence = fields[2]
    if sequence != b"*":
        length = len(sequence)
    else:
        length = None
        for field in fields[3:]:
            if field.startswith(b"LN:i:"):
                try:
                    length = int(field[5:])
                except ValueError as exc:
                    raise RuntimeError(f"Invalid LN:i length for node {node_name!r}") from exc
                break
        if length is None:
            raise RuntimeError(f"Could not derive length for node {node_name!r}")

    if length < 0 or length > UINT32_MAX:
        raise RuntimeError(f"Length for node {node_name!r} does not fit uint32")
    return node_name, length


def set_seen(seen: bytearray, rank: int) -> bool:
    byte_index = rank // 8
    bit = rank % 8
    mask = 1 << bit
    already_seen = bool(seen[byte_index] & mask)
    seen[byte_index] |= mask
    return already_seen


def build_lnx(gfa: Path, ndx: Path, out: Path, force: bool, progress_every: int) -> None:
    if out.exists() and not force:
        raise RuntimeError(f"Output already exists: {out}")
    if not gfa.exists():
        raise RuntimeError(f"GFA does not exist: {gfa}")
    if not ndx.exists():
        raise RuntimeError(f"Node index does not exist: {ndx}")

    node_index = NodeHashIndex(ndx)
    temp = out.with_name(f".{out.name}.gfaidx_tmp_{os.getpid()}")
    total_size = HEADER_STRUCT.size + node_index.count * VALUE_WIDTH
    seen = bytearray((node_index.count + 7) // 8)
    seen_count = 0
    line_count = 0

    try:
        with temp.open("w+b") as handle:
            handle.truncate(total_size)
            handle.write(HEADER_STRUCT.pack(MAGIC, VERSION, VALUE_WIDTH, node_index.count))
            with mmap.mmap(handle.fileno(), total_size, access=mmap.ACCESS_WRITE) as out_map:
                with open_gfa(gfa) as gfa_handle:
                    for raw_line in gfa_handle:
                        line_count += 1
                        if progress_every and line_count % progress_every == 0:
                            print(f"Read {line_count} lines", file=sys.stderr)
                        if not raw_line.startswith(b"S\t"):
                            continue
                        parsed = parse_s_line(raw_line)
                        if parsed is None:
                            continue
                        node_name, length = parsed
                        rank = node_index.lookup_rank(node_name)
                        if rank is None:
                            raise RuntimeError(f"Node {node_name!r} was not found in {ndx}")
                        if set_seen(seen, rank):
                            raise RuntimeError(f"Duplicate node rank for node {node_name!r}")
                        struct.pack_into("<I", out_map, HEADER_STRUCT.size + rank * VALUE_WIDTH, length)
                        seen_count += 1
                out_map.flush()

        if seen_count != node_index.count:
            raise RuntimeError(
                f"GFA/.ndx node-set mismatch: saw {seen_count} S lines, "
                f"expected {node_index.count}"
            )
        if force:
            os.replace(temp, out)
        else:
            os.rename(temp, out)
    except Exception:
        try:
            temp.unlink()
        except FileNotFoundError:
            pass
        raise
    finally:
        node_index.close()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build a .lnx node-length sidecar from an indexed GFA and matching .ndx"
    )
    parser.add_argument("gfa", type=Path, help="input indexed GFA, usually <graph>.gfa.gz")
    parser.add_argument("--ndx", type=Path, help="matching node index; defaults to <gfa>.ndx")
    parser.add_argument("--out", type=Path, help="output .lnx; defaults to <gfa>.lnx")
    parser.add_argument("--force", action="store_true", help="overwrite an existing output file")
    parser.add_argument(
        "--progress-every",
        type=int,
        default=1_000_000,
        help="print progress every N lines; 0 disables progress logging",
    )
    args = parser.parse_args()

    ndx = args.ndx if args.ndx is not None else Path(str(args.gfa) + ".ndx")
    out = args.out if args.out is not None else Path(str(args.gfa) + ".lnx")
    build_lnx(args.gfa, ndx, out, args.force, args.progress_every)
    print(f"Wrote {out}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

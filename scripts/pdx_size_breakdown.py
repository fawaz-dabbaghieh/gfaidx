#!/usr/bin/env python3
"""Print a size breakdown for a .pdx path index file.

Usage:
    python3 scripts/pdx_size_breakdown.py graph.pdx [more.pdx ...] [--tsv]

The script reads the .pdx header (v4 or v5) and reports how much space each
top-level section occupies. --tsv prints one machine-readable row per file.
"""

from __future__ import annotations

import argparse
import os
import struct
from dataclasses import dataclass


HEADER_STRUCT = struct.Struct("<8sIIQQQQQQQQQQ")
HEADER_SIZE = HEADER_STRUCT.size
# v5 header extension: node_id_space, step/posting encodings, step data bytes,
# step superblock index offset and count, block layout, reserved.
HEADER_V5_STRUCT = struct.Struct("<QIIQQQII16x")
MAGIC = b"GFPATH1\x00"
STEP_ENCODINGS = {0: "packed32", 1: "delta_varint"}
POSTING_ENCODINGS = {0: "path_groups", 1: "path_slots"}

PATH_RECORD_SIZE = 128
NODE_RECORD_SIZE = 32
STEP_RECORD_SIZE = 4


@dataclass
class Header:
    magic: bytes
    version: int
    reserved: int
    path_count: int
    node_count: int
    step_count: int
    posting_count: int
    path_table_offset: int
    node_table_offset: int
    step_table_offset: int
    posting_table_offset: int
    strings_offset: int
    strings_size: int
    node_id_space: int = 0
    step_encoding: int = 0
    posting_encoding: int = 0
    step_data_bytes: int = 0
    step_index_offset: int = 0
    step_superblock_count: int = 0


def format_bytes(value: int) -> str:
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    size = float(value)
    unit = units[0]
    for unit in units:
        if size < 1024.0 or unit == units[-1]:
            break
        size /= 1024.0
    if unit == "B":
        return f"{value} B"
    return f"{size:.2f} {unit}"


def pct(value: int, total: int) -> str:
    if total == 0:
        return "0.00%"
    return f"{100.0 * value / total:.2f}%"


def read_header(path: str) -> Header:
    with open(path, "rb") as handle:
        raw = handle.read(HEADER_SIZE + HEADER_V5_STRUCT.size)
    if len(raw) < HEADER_SIZE:
        raise RuntimeError("File is too small to contain a valid .pdx header")
    header = Header(*HEADER_STRUCT.unpack(raw[:HEADER_SIZE]))
    if header.magic != MAGIC:
        raise RuntimeError("Invalid .pdx magic")
    if header.version == 4:
        header.step_data_bytes = header.step_count * STEP_RECORD_SIZE
        header.step_index_offset = header.posting_table_offset
        return header
    if header.version != 5:
        raise RuntimeError(f"Unsupported .pdx version: {header.version}")
    (header.node_id_space, header.step_encoding, header.posting_encoding,
     header.step_data_bytes, header.step_index_offset, header.step_superblock_count,
     _, _) = HEADER_V5_STRUCT.unpack(raw[HEADER_SIZE:])
    return header


def tsv_row(path: str, header: Header, file_size: int) -> str:
    steps = max(header.step_count, 1)
    postings = max(header.posting_count, 1)
    step_table = header.posting_table_offset - header.step_table_offset
    posting_table = header.strings_offset - header.posting_table_offset
    node_table = header.step_table_offset - header.node_table_offset
    return "\t".join([
        path, str(header.version),
        STEP_ENCODINGS.get(header.step_encoding, str(header.step_encoding)),
        POSTING_ENCODINGS.get(header.posting_encoding, str(header.posting_encoding)),
        str(header.node_count), str(header.step_count), str(header.posting_count),
        str(file_size), str(node_table), str(step_table), str(posting_table),
        f"{step_table / steps:.4f}", f"{posting_table / postings:.4f}",
    ])


TSV_COLUMNS = ["pdx", "version", "step_encoding", "posting_encoding", "nodes", "steps",
               "postings", "file_bytes", "node_table_bytes", "step_table_bytes",
               "posting_bytes", "bytes_per_step", "bytes_per_posting"]


def main() -> int:
    parser = argparse.ArgumentParser(description="Print a size breakdown for a .pdx file")
    parser.add_argument("pdx", nargs="+", help="input .pdx file(s)")
    parser.add_argument("--tsv", action="store_true", help="print one TSV row per file")
    args = parser.parse_args()

    if args.tsv:
        print("\t".join(TSV_COLUMNS))
        for path in args.pdx:
            print(tsv_row(path, read_header(path), os.path.getsize(path)))
        return 0
    for i, path in enumerate(args.pdx):
        if i:
            print()
        print_breakdown(path)
    return 0


def print_breakdown(pdx: str) -> None:
    file_size = os.path.getsize(pdx)
    header = read_header(pdx)

    header_size = header.path_table_offset
    path_table_size = header.node_table_offset - header.path_table_offset
    node_table_size = header.step_table_offset - header.node_table_offset
    step_table_size = header.posting_table_offset - header.step_table_offset
    step_data_size = header.step_data_bytes
    step_index_size = header.posting_table_offset - header.step_index_offset
    posting_table_size = header.strings_offset - header.posting_table_offset
    strings_size = header.strings_size

    accounted = (
        header_size
        + path_table_size
        + node_table_size
        + step_table_size
        + posting_table_size
        + strings_size
    )
    trailing = file_size - accounted

    print(f"File: {pdx}")
    print(f"File size: {format_bytes(file_size)}")
    print(f"Format version: {header.version}")
    if header.version >= 5:
        print(f"Step encoding: {STEP_ENCODINGS.get(header.step_encoding, header.step_encoding)}")
        print(f"Posting encoding: {POSTING_ENCODINGS.get(header.posting_encoding, header.posting_encoding)}")
        print(f"Node id space: {header.node_id_space:#018x}"
              + ("  (legacy hash-rank ids)" if header.node_id_space == 0 else ""))
    print()
    print("Counts")
    print(f"  paths:    {header.path_count}")
    print(f"  nodes:    {header.node_count}")
    print(f"  steps:    {header.step_count}")
    print(f"  postings: {header.posting_count}")
    print()
    print("Record sizes")
    print(f"  path record:    {PATH_RECORD_SIZE} B")
    print(f"  node record:    {NODE_RECORD_SIZE} B")
    if header.step_encoding == 0:
        print(f"  step record:    {STEP_RECORD_SIZE} B")
    else:
        print("  step record:    delta varint in 128-step blocks")
    print("  posting record: compressed per-node blocks")
    print()
    print("Section offsets")
    print(f"  header:         0")
    print(f"  path table:     {header.path_table_offset}")
    print(f"  node table:     {header.node_table_offset}")
    print(f"  step table:     {header.step_table_offset}")
    print(f"  posting table:  {header.posting_table_offset}")
    print(f"  strings:        {header.strings_offset}")
    print()
    print("Section sizes")
    print(f"  header:         {format_bytes(header_size):>12}  {pct(header_size, file_size):>8}")
    print(f"  path table:     {format_bytes(path_table_size):>12}  {pct(path_table_size, file_size):>8}")
    print(f"  node table:     {format_bytes(node_table_size):>12}  {pct(node_table_size, file_size):>8}")
    print(f"  step table:     {format_bytes(step_table_size):>12}  {pct(step_table_size, file_size):>8}"
          f"  ({step_table_size / max(header.step_count, 1):.3f} B/step)")
    if header.step_encoding != 0:
        print(f"    step data:    {format_bytes(step_data_size):>12}")
        print(f"    block index:  {format_bytes(step_index_size):>12}")
    print(f"  posting table:  {format_bytes(posting_table_size):>12}  {pct(posting_table_size, file_size):>8}"
          f"  ({posting_table_size / max(header.posting_count, 1):.3f} B/posting)")
    print(f"  strings:        {format_bytes(strings_size):>12}  {pct(strings_size, file_size):>8}")
    if trailing != 0:
        label = "unaccounted" if trailing > 0 else "over-accounted"
        print(f"  {label}:     {format_bytes(abs(trailing)):>12}  {pct(abs(trailing), file_size):>8}")
    print()
    print("Derived totals")
    print(f"  accounted:      {format_bytes(accounted)}")
    print(f"  step+posting:   {format_bytes(step_table_size + posting_table_size)}")
    print(f"  metadata-ish:   {format_bytes(header_size + path_table_size + node_table_size + strings_size)}")


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Print a size breakdown for a .pdx path index file.

Usage:
    python3 scripts/pdx_size_breakdown.py graph.pdx

The script reads the .pdx header, infers record sizes from the on-disk format
version, and reports how much space each top-level section occupies.
"""

from __future__ import annotations

import argparse
import os
import struct
import sys
from dataclasses import dataclass


HEADER_STRUCT = struct.Struct("<8sIIQQQQQQQQQQ")
HEADER_SIZE = HEADER_STRUCT.size
MAGIC = b"GFPATH1\x00"

PATH_RECORD_SIZE_V1 = 64
PATH_RECORD_SIZE_V2_V3 = 128
NODE_RECORD_SIZE = 32
STEP_RECORD_SIZE_V1_V2 = 8
STEP_RECORD_SIZE_V3 = 4
POSTING_RECORD_SIZE = 8


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
        raw = handle.read(HEADER_SIZE)
    if len(raw) != HEADER_SIZE:
        raise RuntimeError("File is too small to contain a valid .pdx header")
    header = Header(*HEADER_STRUCT.unpack(raw))
    if header.magic != MAGIC:
        raise RuntimeError("Invalid .pdx magic")
    if header.version not in (1, 2, 3):
        raise RuntimeError(f"Unsupported .pdx version: {header.version}")
    return header


def infer_record_sizes(version: int) -> tuple[int, int]:
    path_record_size = PATH_RECORD_SIZE_V1 if version == 1 else PATH_RECORD_SIZE_V2_V3
    step_record_size = STEP_RECORD_SIZE_V3 if version == 3 else STEP_RECORD_SIZE_V1_V2
    return path_record_size, step_record_size


def main() -> int:
    parser = argparse.ArgumentParser(description="Print a size breakdown for a .pdx file")
    parser.add_argument("pdx", help="input .pdx file")
    args = parser.parse_args()

    file_size = os.path.getsize(args.pdx)
    header = read_header(args.pdx)
    path_record_size, step_record_size = infer_record_sizes(header.version)

    path_table_size = header.path_count * path_record_size
    node_table_size = header.node_count * NODE_RECORD_SIZE
    step_table_size = header.step_count * step_record_size
    posting_table_size = header.posting_count * POSTING_RECORD_SIZE
    header_size = HEADER_SIZE
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

    print(f"File: {args.pdx}")
    print(f"File size: {format_bytes(file_size)}")
    print(f"Format version: {header.version}")
    print()
    print("Counts")
    print(f"  paths:    {header.path_count}")
    print(f"  nodes:    {header.node_count}")
    print(f"  steps:    {header.step_count}")
    print(f"  postings: {header.posting_count}")
    print()
    print("Record sizes")
    print(f"  path record:    {path_record_size} B")
    print(f"  node record:    {NODE_RECORD_SIZE} B")
    print(f"  step record:    {step_record_size} B")
    print(f"  posting record: {POSTING_RECORD_SIZE} B")
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
    print(f"  step table:     {format_bytes(step_table_size):>12}  {pct(step_table_size, file_size):>8}")
    print(f"  posting table:  {format_bytes(posting_table_size):>12}  {pct(posting_table_size, file_size):>8}")
    print(f"  strings:        {format_bytes(strings_size):>12}  {pct(strings_size, file_size):>8}")
    if trailing != 0:
        label = "unaccounted" if trailing > 0 else "over-accounted"
        print(f"  {label}:     {format_bytes(abs(trailing)):>12}  {pct(abs(trailing), file_size):>8}")
    print()
    print("Derived totals")
    print(f"  accounted:      {format_bytes(accounted)}")
    print(f"  step+posting:   {format_bytes(step_table_size + posting_table_size)}")
    print(f"  metadata-ish:   {format_bytes(header_size + path_table_size + node_table_size + strings_size)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

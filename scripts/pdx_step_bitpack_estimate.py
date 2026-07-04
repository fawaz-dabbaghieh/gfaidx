#!/usr/bin/env python3
"""Estimate .pdx step-table savings from bit-packing node id + orientation.

Usage:
    python3 scripts/pdx_step_bitpack_estimate.py graph.pdx

The current .pdx step table stores one 32-bit word per step: 31 bits for the
rank-aligned node id and 1 orientation bit. This script only reads the .pdx
header and estimates how much smaller the step table and whole file would be if
each step used the minimum fixed bit width required by this graph.
"""

from __future__ import annotations

import argparse
import math
import os
import struct
from dataclasses import dataclass


HEADER_STRUCT = struct.Struct("<8sIIQQQQQQQQQQ")
HEADER_SIZE = HEADER_STRUCT.size
MAGIC = b"GFPATH1\x00"
SUPPORTED_VERSION = 4
CURRENT_STEP_BITS = 32


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
    """Format a byte count with binary units."""
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
    """Format a percentage while handling an empty denominator."""
    if total == 0:
        return "0.00%"
    return f"{100.0 * value / total:.2f}%"


def read_header(path: str) -> Header:
    """Read and validate the current .pdx header."""
    with open(path, "rb") as handle:
        raw = handle.read(HEADER_SIZE)
    if len(raw) != HEADER_SIZE:
        raise RuntimeError("File is too small to contain a valid .pdx header")
    header = Header(*HEADER_STRUCT.unpack(raw))
    if header.magic != MAGIC:
        raise RuntimeError("Invalid .pdx magic")
    if header.version != SUPPORTED_VERSION:
        raise RuntimeError(f"Unsupported .pdx version: {header.version}")
    return header


def bits_for_node_ids(node_count: int) -> int:
    """Return the minimum bits needed for node ids in [0, node_count)."""
    if node_count <= 1:
        return 1
    return (node_count - 1).bit_length()


def packed_bytes(step_count: int, bits_per_step: int, alignment_bytes: int) -> int:
    """Return bytes needed for fixed-width packed steps plus optional padding."""
    raw = (step_count * bits_per_step + 7) // 8
    if alignment_bytes <= 1:
        return raw
    return ((raw + alignment_bytes - 1) // alignment_bytes) * alignment_bytes


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Estimate .pdx step-table savings from bit-packing"
    )
    parser.add_argument("pdx", help="input .pdx file")
    parser.add_argument(
        "--align",
        type=int,
        default=8,
        help="byte alignment for the packed step table estimate [default: 8]",
    )
    args = parser.parse_args()

    if args.align < 1:
        raise RuntimeError("--align must be at least 1")

    file_size = os.path.getsize(args.pdx)
    header = read_header(args.pdx)
    current_step_bytes = header.posting_table_offset - header.step_table_offset
    expected_step_bytes = header.step_count * (CURRENT_STEP_BITS // 8)
    other_file_bytes = file_size - current_step_bytes

    node_bits = bits_for_node_ids(header.node_count)
    bits_per_step = node_bits + 1  # One extra bit stores orientation.
    estimated_step_bytes = packed_bytes(header.step_count, bits_per_step, args.align)
    saved_step_bytes = current_step_bytes - estimated_step_bytes
    estimated_file_size = other_file_bytes + estimated_step_bytes
    saved_file_bytes = file_size - estimated_file_size

    print(f"File: {args.pdx}")
    print(f"File size: {format_bytes(file_size)}")
    print(f"Format version: {header.version}")
    print()
    print("Counts")
    print(f"  nodes: {header.node_count}")
    print(f"  steps: {header.step_count}")
    print()
    print("Current step table")
    print(f"  current bits/step:      {CURRENT_STEP_BITS}")
    print(f"  current step bytes:     {format_bytes(current_step_bytes)}")
    if current_step_bytes != expected_step_bytes:
        print(f"  expected 4B * steps:    {format_bytes(expected_step_bytes)}")
    print()
    print("Bit-packed estimate")
    print(f"  node id bits:           {node_bits}")
    print(f"  orientation bits:       1")
    print(f"  estimated bits/step:    {bits_per_step}")
    print(f"  alignment:              {args.align} B")
    print(f"  estimated step bytes:   {format_bytes(estimated_step_bytes)}")
    print(f"  step table saved:       {format_bytes(saved_step_bytes)}  {pct(saved_step_bytes, current_step_bytes)}")
    print()
    print("Whole-file estimate")
    print(f"  estimated file size:    {format_bytes(estimated_file_size)}")
    print(f"  file bytes saved:       {format_bytes(saved_file_bytes)}  {pct(saved_file_bytes, file_size)}")
    print()
    print("Notes")
    print("  This is a fixed-width bit-packing estimate for the step table only.")
    print("  It does not change the posting table or add any offset/index overhead.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

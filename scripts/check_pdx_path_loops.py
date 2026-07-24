#!/usr/bin/env python3
"""Check whether one indexed P/W path visits any graph node more than once.

A repeated node rank means the path is not node-simple: the steps between two
visits form a closed subwalk. This checks path traversal, not merely whether the
graph contains a self-link.

Example:
    python3 scripts/check_pdx_path_loops.py graph.gfa.gz.pdx CHM13#0#chr1
"""

from __future__ import annotations

import argparse
import struct
import sys
from dataclasses import dataclass
from typing import BinaryIO, Iterator


MAGIC = b"GFPATH1\x00"
SUPPORTED_VERSION = 4
NODE_MASK = 0x7FFFFFFF
REVERSE_BIT = 0x80000000

HEADER_STRUCT = struct.Struct("<8sIIQQQQQQQQQQ")
PATH_RECORD_STRUCT = struct.Struct("<c7x" + "Q" * 13 + "qq")
NODE_RECORD_STRUCT = struct.Struct("<QQQQ")
STEP_STRUCT = struct.Struct("<I")


@dataclass
class Header:
    path_count: int
    node_count: int
    step_count: int
    path_table_offset: int
    node_table_offset: int
    step_table_offset: int
    posting_table_offset: int
    strings_offset: int
    strings_size: int


@dataclass
class PathRecord:
    path_id: int
    record_type: str
    name: str
    step_begin: int
    step_count: int


def read_exact_at(handle: BinaryIO, offset: int, size: int) -> bytes:
    """Read an exact byte range without depending on the current file offset."""
    handle.seek(offset)
    data = handle.read(size)
    if len(data) != size:
        raise RuntimeError(
            f"Short read at byte offset {offset}: expected {size}, got {len(data)}"
        )
    return data


def read_header(handle: BinaryIO) -> Header:
    """Read and validate the current version-4 PDX header."""
    values = HEADER_STRUCT.unpack(read_exact_at(handle, 0, HEADER_STRUCT.size))
    if values[0] != MAGIC:
        raise RuntimeError("Invalid .pdx magic")
    if values[1] != SUPPORTED_VERSION:
        raise RuntimeError(f"Unsupported .pdx version: {values[1]}")

    header = Header(
        path_count=values[3],
        node_count=values[4],
        step_count=values[5],
        path_table_offset=values[7],
        node_table_offset=values[8],
        step_table_offset=values[9],
        posting_table_offset=values[10],
        strings_offset=values[11],
        strings_size=values[12],
    )
    if header.path_table_offset + header.path_count * PATH_RECORD_STRUCT.size > (
        header.node_table_offset
    ):
        raise RuntimeError("The .pdx path table is truncated or overlaps the node table")
    if header.step_table_offset + header.step_count * STEP_STRUCT.size > (
        header.posting_table_offset
    ):
        raise RuntimeError("The .pdx step table is truncated or overlaps the posting table")
    return header


def read_index_string(
    handle: BinaryIO, header: Header, offset: int, length: int
) -> str:
    """Read one UTF-8 name from the PDX string section."""
    if offset > header.strings_size or length > header.strings_size - offset:
        raise RuntimeError("A .pdx string reference is outside the string table")
    raw = read_exact_at(handle, header.strings_offset + offset, length)
    return raw.decode("utf-8", errors="replace")


def find_path(handle: BinaryIO, header: Header, requested_name: str) -> PathRecord:
    """Find one exact canonical path name and reject ambiguous duplicates."""
    table_size = header.path_count * PATH_RECORD_STRUCT.size
    table = read_exact_at(handle, header.path_table_offset, table_size)
    matches: list[PathRecord] = []

    for path_id in range(header.path_count):
        values = PATH_RECORD_STRUCT.unpack_from(
            table, path_id * PATH_RECORD_STRUCT.size
        )
        name = read_index_string(handle, header, values[1], values[2])
        if name != requested_name:
            continue

        record = PathRecord(
            path_id=path_id,
            record_type=values[0].decode("ascii", errors="replace"),
            name=name,
            step_begin=values[3],
            step_count=values[4],
        )
        if record.step_begin > header.step_count or record.step_count > (
            header.step_count - record.step_begin
        ):
            raise RuntimeError("The selected path steps are outside the .pdx step table")
        matches.append(record)

    if not matches:
        raise RuntimeError(f"Path was not found in .pdx: {requested_name}")
    if len(matches) > 1:
        ids = ", ".join(str(record.path_id) for record in matches[:20])
        raise RuntimeError(
            f"Path name is duplicated in .pdx at path ids {ids}; "
            "use a unique canonical P/W path name"
        )
    return matches[0]


def iter_path_steps(
    handle: BinaryIO,
    header: Header,
    path: PathRecord,
    chunk_steps: int = 1 << 20,
) -> Iterator[tuple[int, int, bool]]:
    """Stream packed steps as (zero-based step, node rank, is_reverse)."""
    remaining = path.step_count
    step_rank = 0
    byte_offset = header.step_table_offset + path.step_begin * STEP_STRUCT.size

    while remaining:
        take = min(remaining, chunk_steps)
        raw = read_exact_at(handle, byte_offset, take * STEP_STRUCT.size)
        for (packed,) in STEP_STRUCT.iter_unpack(raw):
            node_rank = packed & NODE_MASK
            if node_rank >= header.node_count:
                raise RuntimeError(
                    f"Step {step_rank} has node rank {node_rank} outside the node table"
                )
            yield step_rank, node_rank, bool(packed & REVERSE_BIT)
            step_rank += 1
        remaining -= take
        byte_offset += take * STEP_STRUCT.size


def read_node_name(handle: BinaryIO, header: Header, node_rank: int) -> str:
    """Resolve one rank through the PDX node and string tables."""
    raw = read_exact_at(
        handle,
        header.node_table_offset + node_rank * NODE_RECORD_STRUCT.size,
        NODE_RECORD_STRUCT.size,
    )
    name_offset, name_length, _, _ = NODE_RECORD_STRUCT.unpack(raw)
    return read_index_string(handle, header, name_offset, name_length)


def check_path(
    handle: BinaryIO,
    header: Header,
    path: PathRecord,
    max_report: int,
    max_positions: int,
) -> tuple[int, int]:
    """Scan twice: detect repeats compactly, then locate reported occurrences."""
    # Two bits per graph node keep memory bounded even for very large indexes.
    seen = bytearray((header.node_count + 7) // 8)
    repeated = bytearray((header.node_count + 7) // 8)
    repeated_node_count = 0
    extra_occurrence_count = 0
    report_ranks: list[int] = []

    for _, node_rank, _ in iter_path_steps(handle, header, path):
        byte_index = node_rank >> 3
        mask = 1 << (node_rank & 7)
        if seen[byte_index] & mask:
            extra_occurrence_count += 1
            if not repeated[byte_index] & mask:
                repeated[byte_index] |= mask
                repeated_node_count += 1
                if len(report_ranks) < max_report:
                    report_ranks.append(node_rank)
        else:
            seen[byte_index] |= mask

    print(f"PDX: {handle.name}")
    print(f"Path: {path.name}")
    print(f"Path id/type: {path.path_id}/{path.record_type}")
    print(f"Steps: {path.step_count:,}")
    print(f"Unique node ranks: {path.step_count - extra_occurrence_count:,}")
    print(f"Repeated node ranks: {repeated_node_count:,}")
    print(f"Extra visits after first occurrence: {extra_occurrence_count:,}")

    if repeated_node_count == 0:
        print("Result: no repeated node visits; the path is node-simple")
        return repeated_node_count, extra_occurrence_count

    print("Result: repeated node visits detected; the path is not node-simple")
    if not report_ranks:
        return repeated_node_count, extra_occurrence_count

    positions: dict[int, list[tuple[int, bool]]] = {
        rank: [] for rank in report_ranks
    }
    occurrence_counts = {rank: 0 for rank in report_ranks}
    for step_rank, node_rank, is_reverse in iter_path_steps(handle, header, path):
        if node_rank not in positions:
            continue
        occurrence_counts[node_rank] += 1
        if len(positions[node_rank]) < max_positions:
            positions[node_rank].append((step_rank, is_reverse))

    print()
    print(
        f"First {len(report_ranks):,} repeated node ranks "
        "(step positions are zero-based):"
    )
    for node_rank in report_ranks:
        node_name = read_node_name(handle, header, node_rank)
        shown = ", ".join(
            f"{step}{'-' if reverse else '+'}"
            for step, reverse in positions[node_rank]
        )
        omitted = occurrence_counts[node_rank] - len(positions[node_rank])
        if omitted:
            shown += f", ... ({omitted:,} more)"
        print(
            f"  rank={node_rank} node={node_name!r} "
            f"occurrences={occurrence_counts[node_rank]:,} steps=[{shown}]"
        )
    return repeated_node_count, extra_occurrence_count


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Check whether one path in a version-4 .pdx revisits nodes"
    )
    parser.add_argument("pdx", help="input path index")
    parser.add_argument(
        "path",
        help=(
            "exact P path name, or canonical W key "
            "sample|haplotype|sequence|start|end"
        ),
    )
    parser.add_argument(
        "--max-report",
        type=int,
        default=20,
        help="maximum repeated node ranks to describe [default: 20]",
    )
    parser.add_argument(
        "--max-positions",
        type=int,
        default=20,
        help="maximum step positions shown per repeated node [default: 20]",
    )
    parser.add_argument(
        "--fail-on-repeat",
        action="store_true",
        help="exit with status 1 when the path revisits at least one node",
    )
    args = parser.parse_args()

    if args.max_report < 0:
        parser.error("--max-report must be non-negative")
    if args.max_positions < 1:
        parser.error("--max-positions must be at least 1")

    try:
        with open(args.pdx, "rb") as handle:
            header = read_header(handle)
            path = find_path(handle, header, args.path)
            repeated, _ = check_path(
                handle,
                header,
                path,
                args.max_report,
                args.max_positions,
            )
        return 1 if args.fail_on_repeat and repeated else 0
    except (OSError, RuntimeError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

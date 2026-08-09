#!/usr/bin/env python3
"""Resolve one absolute W-coordinate locus into tool-specific coordinates.

The benchmark manifest deliberately contains no node IDs or synthesized path
names. This helper joins one locus to the W-to-P mapping emitted by w_to_p.py,
checks the paths that VG and ODGI actually indexed, and writes one auditable
JSON record consumed by the untimed node-mapping and timed query rules.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path


class ResolutionError(RuntimeError):
    """Report an invalid or ambiguous benchmark locus."""


def parse_args() -> argparse.Namespace:
    """Parse one locus row supplied by the Snakefile."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mapping", required=True, help="W-to-P mapping TSV")
    parser.add_argument("--vg-paths", required=True, help="VG path-name list")
    parser.add_argument("--odgi-paths", required=True, help="ODGI path-name list")
    parser.add_argument("--graph", required=True)
    parser.add_argument("--query-id", required=True)
    parser.add_argument("--sample", required=True)
    parser.add_argument("--haplotype", required=True)
    parser.add_argument("--seq-id", required=True)
    parser.add_argument("--node-position", type=int)
    parser.add_argument("--region-start", type=int)
    parser.add_argument("--region-end", type=int)
    parser.add_argument("--notes", default="")
    parser.add_argument("--out", required=True)
    return parser.parse_args()


def read_mapping(path: Path) -> list[dict[str, str]]:
    """Read the converter mapping as structured TSV data."""
    with path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle, delimiter="\t"))
    required = {
        "sample",
        "haplotype",
        "seq_id",
        "seq_start",
        "seq_end",
        "walk_name",
        "p_path_name",
    }
    if rows and not required.issubset(rows[0]):
        missing = ", ".join(sorted(required - set(rows[0])))
        raise ResolutionError(f"W-to-P mapping is missing columns: {missing}")
    return rows


def read_path_names(path: Path) -> set[str]:
    """Read the first field from a VG or ODGI one-path-per-line listing."""
    names: set[str] = set()
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.rstrip("\r\n")
            if line:
                names.add(line.split("\t", 1)[0])
    return names


def requested_span(args: argparse.Namespace) -> tuple[int, int]:
    """Return the smallest half-open interval containing all requested work."""
    starts: list[int] = []
    ends: list[int] = []
    if args.node_position is not None:
        if args.node_position < 0:
            raise ResolutionError("node_position must be non-negative")
        starts.append(args.node_position)
        ends.append(args.node_position + 1)

    have_region_start = args.region_start is not None
    have_region_end = args.region_end is not None
    if have_region_start != have_region_end:
        raise ResolutionError("region_start and region_end must be supplied together")
    if have_region_start:
        if args.region_start < 0 or args.region_end <= args.region_start:
            raise ResolutionError("region coordinates must be increasing and non-negative")
        starts.append(args.region_start)
        ends.append(args.region_end)

    if not starts:
        raise ResolutionError("locus defines neither a node position nor a region")
    return min(starts), max(ends)


def select_walk(args: argparse.Namespace, rows: list[dict[str, str]]) -> dict[str, str]:
    """Select the unique W fragment containing every requested coordinate."""
    wanted_start, wanted_end = requested_span(args)
    matching_identity = [
        row
        for row in rows
        if row["sample"] == args.sample
        and row["haplotype"] == args.haplotype
        and row["seq_id"] == args.seq_id
    ]
    if not matching_identity:
        raise ResolutionError(
            "no converted W record matches "
            f"{args.sample}#{args.haplotype}#{args.seq_id}"
        )

    concrete: list[tuple[dict[str, str], int, int]] = []
    for row in matching_identity:
        if row["seq_start"] == "*" or row["seq_end"] == "*":
            continue
        try:
            start = int(row["seq_start"])
            end = int(row["seq_end"])
        except ValueError as error:
            raise ResolutionError("W-to-P mapping contains invalid coordinates") from error
        if start <= wanted_start and wanted_end <= end:
            concrete.append((row, start, end))

    if len(concrete) != 1:
        available = ", ".join(
            f"{row['seq_start']}-{row['seq_end']}" for row in matching_identity
        )
        if not concrete:
            raise ResolutionError(
                f"requested interval {wanted_start}-{wanted_end} is not contained in "
                f"one concrete W record; available fragments: {available}"
            )
        raise ResolutionError(
            f"requested interval {wanted_start}-{wanted_end} matches multiple W records; "
            f"available fragments: {available}"
        )

    row, start, end = concrete[0]
    selected = dict(row)
    selected["seq_start"] = str(start)
    selected["seq_end"] = str(end)
    return selected


def select_vg_path(walk: dict[str, str], available: set[str]) -> str:
    """Match VG's actual imported W name, including nonzero subranges."""
    base = walk["walk_name"]
    start = int(walk["seq_start"])
    end = int(walk["seq_end"])
    # VG keeps a zero-start W name plain and normally gives a subrange W record
    # a bracket suffix. Checking the generated list prevents a silent guess.
    candidates = [base, f"{base}[{start}-{end}]"] if start == 0 else [
        f"{base}[{start}-{end}]",
        base,
    ]
    matches = [name for name in candidates if name in available]
    if len(matches) != 1:
        raise ResolutionError(
            "could not uniquely match the W record to a VG path; tried: "
            + ", ".join(candidates)
        )
    return matches[0]


def resolve(args: argparse.Namespace) -> dict[str, object]:
    """Build the complete coordinate record used by all query rules."""
    mapping = read_mapping(Path(args.mapping))
    walk = select_walk(args, mapping)
    vg_paths = read_path_names(Path(args.vg_paths))
    odgi_paths = read_path_names(Path(args.odgi_paths))

    vg_path = select_vg_path(walk, vg_paths)
    odgi_path = walk["p_path_name"]
    if odgi_path not in odgi_paths:
        raise ResolutionError(
            f"converted P path {odgi_path!r} is absent from the ODGI graph"
        )

    seq_start = int(walk["seq_start"])
    result: dict[str, object] = {
        "graph": args.graph,
        "query_id": args.query_id,
        "sample": args.sample,
        "haplotype": args.haplotype,
        "seq_id": args.seq_id,
        "seq_start": seq_start,
        "seq_end": int(walk["seq_end"]),
        "walk_name": walk["walk_name"],
        "vg_path_name": vg_path,
        "odgi_path_name": odgi_path,
        "node_position": args.node_position,
        "node_local_position": None,
        "vg_node_region": None,
        "odgi_node_position": None,
        "region_start": args.region_start,
        "region_end": args.region_end,
        "gfaidx_region": None,
        "vg_region": None,
        "odgi_region": None,
        "gbz_sample": args.sample,
        "gbz_contig": args.seq_id,
        "gbz_interval": None,
        "notes": args.notes,
    }

    if args.node_position is not None:
        local = args.node_position - seq_start
        result["node_local_position"] = local
        # VG accepts a single path position without an end coordinate, which
        # identifies the containing node without touching a following boundary.
        result["vg_node_region"] = f"{vg_path}:{local}"
        result["odgi_node_position"] = f"{odgi_path},{local}"

    if args.region_start is not None:
        local_start = args.region_start - seq_start
        local_end = args.region_end - seq_start
        result["gfaidx_region"] = (
            f"{args.seq_id}:{args.region_start}-{args.region_end}"
        )
        result["vg_region"] = f"{vg_path}:{local_start}-{local_end}"
        result["odgi_region"] = f"{odgi_path}:{local_start}-{local_end}"
        result["gbz_interval"] = f"{args.region_start}..{args.region_end}"

    return result


def main() -> int:
    """Resolve the locus and write stable, human-readable JSON."""
    args = parse_args()
    result = resolve(args)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", encoding="utf-8") as handle:
        json.dump(result, handle, indent=2, sort_keys=True)
        handle.write("\n")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ResolutionError) as error:
        print(f"resolve_locus.py: {error}", file=sys.stderr)
        raise SystemExit(1)

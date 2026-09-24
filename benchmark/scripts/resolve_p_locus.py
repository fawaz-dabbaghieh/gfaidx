#!/usr/bin/env python3
"""Resolve a locus on an unsuffixed PanSN P path for every benchmark tool."""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path


class ResolutionError(RuntimeError):
    """Report malformed coordinates or inconsistent path metadata."""


def parse_args() -> argparse.Namespace:
    """Parse one P-path locus supplied by the Snakefile."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--vg-paths", required=True)
    parser.add_argument("--odgi-paths", required=True)
    parser.add_argument("--gbz-metadata", required=True)
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


def read_path_names(path: Path) -> set[str]:
    """Read one path name per line from VG or ODGI output."""
    names: set[str] = set()
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            value = line.rstrip("\r\n").split("\t", 1)[0]
            if value:
                names.add(value)
    return names


def validate_coordinates(args: argparse.Namespace) -> None:
    """Validate zero-based P coordinates without scanning the full path."""
    if args.node_position is not None and args.node_position < 0:
        raise ResolutionError("node_position must be non-negative")
    have_start = args.region_start is not None
    have_end = args.region_end is not None
    if have_start != have_end:
        raise ResolutionError("region_start and region_end must be supplied together")
    if have_start and (args.region_start < 0 or args.region_end <= args.region_start):
        raise ResolutionError("region coordinates must be increasing and non-negative")
    if args.node_position is None and not have_start:
        raise ResolutionError("locus defines neither a node position nor a region")


def validate_gbz_reference(path: Path, sample: str, haplotype: str, contig: str) -> None:
    """Require a matching REFERENCE path in VG's GBZ metadata listing."""
    with path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle, delimiter="\t"))

    # `vg paths -M` prefixes its first header field with '#'. Normalize that
    # spelling while leaving current and future non-name fields untouched.
    normalized = [{key.lstrip("#"): value for key, value in row.items()} for row in rows]
    matches = [
        row
        for row in normalized
        if row.get("SAMPLE") == sample
        and row.get("HAPLOTYPE") == haplotype
        and row.get("LOCUS") == contig
        and row.get("SENSE") == "REFERENCE"
    ]
    if len(matches) != 1:
        raise ResolutionError(
            "GBZ metadata does not contain exactly one REFERENCE path for "
            f"{sample}#{haplotype}#{contig}; check --path-regex, "
            "--path-fields, and --set-reference"
        )


def resolve(args: argparse.Namespace) -> dict[str, object]:
    """Build all tool-specific names and coordinate ranges for one P path."""
    validate_coordinates(args)
    path_name = f"{args.sample}#{args.haplotype}#{args.seq_id}"
    if ":" in path_name:
        raise ResolutionError("coordinate-suffixed or colon-containing P names are unsupported")

    vg_paths = read_path_names(Path(args.vg_paths))
    odgi_paths = read_path_names(Path(args.odgi_paths))
    if path_name not in vg_paths:
        raise ResolutionError(f"P path {path_name!r} is absent from the VG XG")
    if path_name not in odgi_paths:
        raise ResolutionError(f"P path {path_name!r} is absent from the ODGI graph")
    # Node-only GBZ-base queries use an already resolved numeric node ID and do
    # not require their source P path to be a coordinate-indexed reference.
    if args.region_start is not None:
        validate_gbz_reference(
            Path(args.gbz_metadata), args.sample, args.haplotype, args.seq_id
        )

    result: dict[str, object] = {
        "graph": args.graph,
        "query_id": args.query_id,
        "sample": args.sample,
        "haplotype": args.haplotype,
        "seq_id": args.seq_id,
        "seq_start": 0,
        "seq_end": None,
        "walk_name": path_name,
        "vg_path_name": path_name,
        "odgi_path_name": path_name,
        "node_position": args.node_position,
        "node_local_position": args.node_position,
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
        result["vg_node_region"] = f"{path_name}:{args.node_position}"
        result["odgi_node_position"] = f"{path_name},{args.node_position}"

    if args.region_start is not None:
        # gfaidx, ODGI, and GBZ-base consume half-open end coordinates here.
        # `vg chunk -p` documents an inclusive end, hence the explicit -1.
        result["gfaidx_region"] = f"{path_name}:{args.region_start}-{args.region_end}"
        result["vg_region"] = f"{path_name}:{args.region_start}-{args.region_end - 1}"
        result["odgi_region"] = f"{path_name}:{args.region_start}-{args.region_end}"
        result["gbz_interval"] = f"{args.region_start}..{args.region_end}"

    return result


def main() -> int:
    """Resolve one locus and write stable JSON consumed by query rules."""
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
        print(f"resolve_p_locus.py: {error}", file=sys.stderr)
        raise SystemExit(1)

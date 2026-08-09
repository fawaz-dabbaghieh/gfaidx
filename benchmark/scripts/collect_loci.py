#!/usr/bin/env python3
"""Join resolved locus JSON records and node-ID sidecars into one TSV."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path


FIELDS = [
    "graph",
    "query_id",
    "sample",
    "haplotype",
    "seq_id",
    "seq_start",
    "seq_end",
    "node_position",
    "node_local_position",
    "original_node_id",
    "odgi_node_id",
    "vg_path_name",
    "odgi_path_name",
    "region_start",
    "region_end",
    "gfaidx_region",
    "vg_region",
    "odgi_region",
    "gbz_interval",
    "notes",
]


def parse_args() -> argparse.Namespace:
    """Parse the results tree and graph filter."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results-dir", required=True)
    parser.add_argument("--graphs", nargs="+", required=True)
    parser.add_argument("--out", required=True)
    return parser.parse_args()


def optional_text(path: Path) -> str:
    """Return a stripped sidecar value when the mapping exists."""
    return path.read_text(encoding="utf-8").strip() if path.exists() else ""


def main() -> int:
    """Write only the current run's graphs, excluding stale result folders."""
    args = parse_args()
    results = Path(args.results_dir)
    rows: list[dict[str, object]] = []
    for graph in args.graphs:
        for path in sorted((results / "maps" / "loci" / graph).glob("*.json")):
            with path.open("r", encoding="utf-8") as handle:
                row = json.load(handle)
            query = str(row["query_id"])
            row["original_node_id"] = optional_text(
                results / "maps" / "nodes" / "original" / graph / f"{query}.node_id"
            )
            row["odgi_node_id"] = optional_text(
                results / "maps" / "odgi" / graph / f"{query}.node_id"
            )
            rows.append(row)

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS, delimiter="\t")
        writer.writeheader()
        for row in rows:
            writer.writerow({field: "" if row.get(field) is None else row.get(field, "") for field in FIELDS})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

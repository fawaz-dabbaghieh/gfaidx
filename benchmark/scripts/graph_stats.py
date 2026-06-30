#!/usr/bin/env python3
"""Count basic GFA records in an extracted graph.

All query outputs are converted to GFA before this script is called. Keeping the
statistics format shared across tools makes the result tables easy to compare.
"""

from __future__ import annotations

import argparse
import gzip
import json
from pathlib import Path


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gfa", required=True, help="Input GFA or GFA.gz")
    parser.add_argument("--out", required=True, help="Output JSON stats")
    return parser.parse_args()


def open_text(path: Path):
    """Open plain or gzip-compressed GFA as text."""
    if path.suffix == ".gz":
        return gzip.open(path, "rt", encoding="utf-8")
    return path.open("r", encoding="utf-8")


def main() -> int:
    """Count S/L/P/W records and write a JSON summary."""
    args = parse_args()
    gfa_path = Path(args.gfa)
    stats = {
        "nodes": 0,
        "edges": 0,
        "paths": 0,
        "walks": 0,
        "headers": 0,
        "other_records": 0,
        "bytes": gfa_path.stat().st_size if gfa_path.exists() else 0,
    }

    with open_text(gfa_path) as handle:
        for line in handle:
            if not line:
                continue
            tag = line[0]
            if tag == "S":
                stats["nodes"] += 1
            elif tag == "L":
                stats["edges"] += 1
            elif tag == "P":
                stats["paths"] += 1
            elif tag == "W":
                stats["walks"] += 1
            elif tag == "H":
                stats["headers"] += 1
            else:
                stats["other_records"] += 1

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as handle:
        json.dump(stats, handle, indent=2, sort_keys=True)
        handle.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

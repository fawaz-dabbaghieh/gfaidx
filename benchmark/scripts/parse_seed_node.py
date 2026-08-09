#!/usr/bin/env python3
"""Extract the unique S-line node ID from a single-position VG subgraph."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


def parse_args() -> argparse.Namespace:
    """Parse input and output paths."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gfa", required=True)
    parser.add_argument("--out", required=True)
    return parser.parse_args()


def main() -> int:
    """Require exactly one unique segment and write its original node ID."""
    args = parse_args()
    node_ids: list[str] = []
    with Path(args.gfa).open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.startswith("S\t"):
                continue
            fields = line.rstrip("\r\n").split("\t", 2)
            if len(fields) >= 2 and fields[1] not in node_ids:
                node_ids.append(fields[1])

    if len(node_ids) != 1:
        shown = ", ".join(node_ids[:10]) or "none"
        raise SystemExit(
            "single-position VG lookup must contain exactly one node; "
            f"found {len(node_ids)} ({shown})"
        )

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(node_ids[0] + "\n", encoding="utf-8")
    print(f"resolved original node id: {node_ids[0]}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

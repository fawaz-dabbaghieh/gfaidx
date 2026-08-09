#!/usr/bin/env python3
"""Extract the node ID from `odgi position -v` output.

`odgi position -i graph.og -p PATH,OFFSET -v` prints a TSV whose first data row
carries the graph position as `node,offset,strand` in a #-prefixed-header table.
This turns that into the bare node ID, which is how a query node in the original
GFA ID space is translated into an optimized odgi graph's compacted ID space.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, help="odgi position -v output")
    parser.add_argument("--out", required=True, help="file to write the node ID to")
    return parser.parse_args()


def node_id_from(text: str) -> str:
    """Return the first node ID found in odgi position output."""
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        # With path-to-graph translation the target ODGI graph position is the
        # final TSV field. Reading right-to-left avoids mistaking a numeric path
        # name for the compacted node ID.
        for field in reversed(line.split("\t")):
            field = field.strip()
            # The graph position field looks like "1234,17,+".
            head = field.split(",")[0]
            if head.isdigit():
                return head
    raise SystemExit(f"no graph position found in odgi position output:\n{text}")


def main() -> int:
    """Write the translated node ID."""
    args = parse_args()
    text = Path(args.input).read_text(encoding="utf-8")
    node_id = node_id_from(text)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(node_id + "\n", encoding="utf-8")
    print(f"translated node id: {node_id}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

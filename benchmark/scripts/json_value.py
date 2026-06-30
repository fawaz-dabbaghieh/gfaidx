#!/usr/bin/env python3
"""Print one value from a JSON file.

Snakemake shell blocks use this small helper to pass a source-tool output node
count into the matched gfaidx query as --max_nodes.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, help="Input JSON")
    parser.add_argument("--key", required=True, help="Top-level key to print")
    parser.add_argument(
        "--minimum",
        type=int,
        default=None,
        help="Optional integer minimum enforced on the printed value",
    )
    return parser.parse_args()


def main() -> int:
    """Read the value, optionally clamp it, and print it."""
    args = parse_args()
    with Path(args.input).open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    value = data[args.key]
    if args.minimum is not None:
        value = max(args.minimum, int(value))
    print(value)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

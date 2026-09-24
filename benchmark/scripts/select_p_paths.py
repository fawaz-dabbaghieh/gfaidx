#!/usr/bin/env python3
"""Select exact P path rows from `gfaidx get_path --print_path_names` output."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


class SelectionError(RuntimeError):
    """Report missing, duplicated, or malformed requested P paths."""


def parse_args() -> argparse.Namespace:
    """Parse requested PanSN names and the output selection file."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", required=True)
    parser.add_argument("--requested", nargs="+", required=True)
    return parser.parse_args()


def main() -> int:
    """Write one `P<TAB>name` row for every requested path in stable order."""
    args = parse_args()
    requested = set(args.requested)
    if len(requested) != len(args.requested):
        raise SelectionError("the requested P path list contains duplicates")

    found: set[str] = set()
    for line in sys.stdin:
        fields = line.rstrip("\r\n").split("\t")
        if len(fields) < 2 or fields[0] != "P" or fields[1] not in requested:
            continue
        if fields[1] in found:
            raise SelectionError(f"P path {fields[1]!r} appears more than once")
        found.add(fields[1])

    missing = sorted(requested - found)
    if missing:
        raise SelectionError("requested P paths are absent from .pdx: " + ", ".join(missing))

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", encoding="utf-8") as handle:
        for name in sorted(found):
            handle.write(f"P\t{name}\n")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, SelectionError) as error:
        print(f"select_p_paths.py: {error}", file=sys.stderr)
        raise SystemExit(1)

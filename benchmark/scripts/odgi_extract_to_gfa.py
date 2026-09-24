#!/usr/bin/env python3
"""Run ODGI extraction and GFA conversion as one measured operation."""

from __future__ import annotations

import argparse
import shlex
import subprocess
import sys
import tempfile
from pathlib import Path


def parse_args() -> argparse.Namespace:
    """Parse ODGI settings and extraction arguments following `--`."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--odgi", required=True)
    parser.add_argument("--input", required=True)
    parser.add_argument("--threads", type=int, required=True)
    parser.add_argument("--temp-dir", default="")
    parser.add_argument("extract_args", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if args.extract_args and args.extract_args[0] == "--":
        args.extract_args = args.extract_args[1:]
    return args


def main() -> int:
    """Write the converted GFA to stdout and remove the temporary OG graph."""
    args = parse_args()
    temp_parent = Path(args.temp_dir) if args.temp_dir else None
    if temp_parent is not None:
        temp_parent.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory(prefix="odgi_extract.", dir=temp_parent) as temp_dir:
        graph = Path(temp_dir) / "subgraph.og"
        extract = [
            args.odgi,
            "extract",
            "-i",
            args.input,
            "-o",
            str(graph),
            *args.extract_args,
            # Append the benchmark-controlled thread setting last so a stale
            # extract_extra value cannot override the requested sweep point.
            "-t",
            str(args.threads),
        ]
        # Record both commands because the outer metrics file names this
        # wrapper rather than its two ODGI child processes.
        print(f"+ {shlex.join(extract)}", file=sys.stderr, flush=True)
        subprocess.run(extract, check=True)
        # GFA serialization is part of the measured operation, so give odgi
        # view the same thread budget as extraction instead of leaving the
        # second half of a nominally threaded query at its one-thread default.
        view = [
            args.odgi,
            "view",
            "-i",
            str(graph),
            "-g",
            "-t",
            str(args.threads),
        ]
        print(f"+ {shlex.join(view)}", file=sys.stderr, flush=True)
        subprocess.run(
            view,
            check=True,
            stdout=sys.stdout.buffer,
        )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except subprocess.CalledProcessError as error:
        print(f"odgi_extract_to_gfa.py: command failed with exit code {error.returncode}", file=sys.stderr)
        raise SystemExit(error.returncode)

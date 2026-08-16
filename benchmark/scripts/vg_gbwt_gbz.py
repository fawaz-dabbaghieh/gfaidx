#!/usr/bin/env python3
"""Build the benchmark GBZ, including P-path reference promotion."""

from __future__ import annotations

import argparse
import shlex
import subprocess
import sys
import tempfile
from pathlib import Path


def parse_args() -> argparse.Namespace:
    """Parse stable wrapper settings and optional VG arguments after `--`."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--vg", required=True)
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--path-format", choices=("W", "P"), required=True)
    parser.add_argument("--reference-sample", default="")
    parser.add_argument("vg_args", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if args.vg_args and args.vg_args[0] == "--":
        args.vg_args = args.vg_args[1:]
    if args.path_format == "P" and not args.reference_sample:
        parser.error("--reference-sample is required for a P-line graph")
    return args


def run(command: list[str]) -> None:
    """Run one VG stage while preserving its output in the benchmark log."""
    # The outer timer records this wrapper as its command, so copy each actual
    # VG invocation into the log for complete provenance.
    print(f"+ {shlex.join(command)}", file=sys.stderr, flush=True)
    subprocess.run(command, check=True)


def main() -> int:
    """Build a direct W GBZ or build and reference-promote a P GBZ."""
    args = parse_args()
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)

    if args.path_format == "W":
        run([
            args.vg,
            "gbwt",
            "-g",
            str(output),
            "-G",
            args.input,
            *args.vg_args,
        ])
        return 0

    # P lines do not carry structured sample/haplotype metadata. Parse the
    # unsuffixed PanSN name sample#haplotype#contig while constructing GBZ.
    with tempfile.TemporaryDirectory(prefix="vg_gbwt.", dir=output.parent) as temp_dir:
        initial_gbz = Path(temp_dir) / "unmarked.gbz"
        run([
            args.vg,
            "gbwt",
            "-g",
            str(initial_gbz),
            "-G",
            args.input,
            "--path-regex",
            r"([^#]+)#([0-9]+)#(.+)",
            "--path-fields",
            "_SHC",
            *args.vg_args,
        ])

        # VG reference promotion is a separate GBWT operation. Rewriting the
        # temporary GBZ here keeps both stages in one measured indexing job.
        run([
            args.vg,
            "gbwt",
            "-Z",
            str(initial_gbz),
            "--set-reference",
            args.reference_sample,
            "--gbz-format",
            "-g",
            str(output),
        ])
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except subprocess.CalledProcessError as error:
        print(
            f"vg_gbwt_gbz.py: VG command failed with exit code {error.returncode}",
            file=sys.stderr,
        )
        raise SystemExit(error.returncode)

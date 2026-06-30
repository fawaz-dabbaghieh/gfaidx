#!/usr/bin/env python3
"""Record versions of the tools used by the benchmark."""

from __future__ import annotations

import argparse
import subprocess
from pathlib import Path


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", required=True, help="Output TSV")
    parser.add_argument("--gfaidx", required=True)
    parser.add_argument("--vg", required=True)
    parser.add_argument("--odgi", required=True)
    return parser.parse_args()


def run_version(command: list[str]) -> tuple[int, str]:
    """Run a version command and return exit code plus compact output."""
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=30)
    except (OSError, subprocess.SubprocessError) as exc:
        return 127, str(exc)
    text = " ".join((result.stdout + " " + result.stderr).split())
    return result.returncode, text


def main() -> int:
    """Write one TSV row per tool."""
    args = parse_args()
    tools = {
        "gfaidx": [args.gfaidx, "--version"],
        "vg": [args.vg, "version"],
        "odgi": [args.odgi, "version"],
    }

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as out:
        out.write("tool\tcommand\texit_code\tversion_text\n")
        for name, command in tools.items():
            exit_code, text = run_version(command)
            out.write(f"{name}\t{' '.join(command)}\t{exit_code}\t{text}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

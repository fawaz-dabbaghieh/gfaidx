#!/usr/bin/env python3
"""Run a VG node search and GFA serialization as one measured operation.

``vg find`` writes VG protobuf rather than GFA. This wrapper streams that graph
straight into ``vg convert -f`` so the benchmark includes both extraction and
serialization without writing an unmeasured intermediate graph.
"""

from __future__ import annotations

import argparse
import shlex
import subprocess
import sys


def parse_args() -> argparse.Namespace:
    """Parse controlled query settings and optional arguments after ``--``."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--vg", required=True, help="VG executable")
    parser.add_argument("--input", required=True, help="input XG index")
    parser.add_argument("--node", required=True, help="seed node ID")
    parser.add_argument(
        "--context",
        required=True,
        type=int,
        help="step count, or base-pair distance with --use-length",
    )
    parser.add_argument(
        "--use-length",
        action="store_true",
        help="interpret --context as base pairs by passing vg find -L",
    )
    parser.add_argument("find_args", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if args.context < 0:
        parser.error("--context must be non-negative")
    # argparse preserves the separator for REMAINDER, but VG must not see it.
    if args.find_args and args.find_args[0] == "--":
        args.find_args = args.find_args[1:]
    return args


def main() -> int:
    """Stream ``vg find`` output through ``vg convert`` and return its status."""
    args = parse_args()

    # User extras precede benchmark-controlled flags so a stale manifest value
    # cannot silently replace the selected index, node, or context.
    find_command = [
        args.vg,
        "find",
        *args.find_args,
        "-x",
        args.input,
        "-n",
        args.node,
        "-c",
        str(args.context),
    ]
    if args.use_length:
        # With -L, vg find interprets -c as a minimum base-pair distance rather
        # than a number of graph steps.
        find_command.append("-L")

    # VG find emits protobuf. Convert from stdin to GFA on stdout inside this
    # process tree so measure.py accounts for the complete query-ready output.
    convert_command = [args.vg, "convert", "-f", "-t", "1", "-"]
    print(
        f"+ {shlex.join(find_command)} | {shlex.join(convert_command)}",
        file=sys.stderr,
        flush=True,
    )

    find_process = subprocess.Popen(find_command, stdout=subprocess.PIPE)
    assert find_process.stdout is not None
    try:
        convert_process = subprocess.Popen(
            convert_command,
            stdin=find_process.stdout,
            stdout=sys.stdout.buffer,
        )
    except OSError:
        # If VG conversion could not start, stop the producer before reporting
        # the original launch error to avoid leaving an orphaned find process.
        find_process.stdout.close()
        find_process.terminate()
        find_process.wait()
        raise

    # Close the parent's duplicate pipe descriptor. This lets find observe a
    # broken pipe promptly if conversion exits before consuming all protobuf.
    find_process.stdout.close()
    convert_status = convert_process.wait()
    find_status = find_process.wait()

    if find_status != 0:
        print(
            f"vg_find_to_gfa.py: vg find failed with exit code {find_status}",
            file=sys.stderr,
        )
        return find_status
    if convert_status != 0:
        print(
            f"vg_find_to_gfa.py: vg convert failed with exit code {convert_status}",
            file=sys.stderr,
        )
        return convert_status
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except OSError as error:
        print(f"vg_find_to_gfa.py: could not run VG: {error}", file=sys.stderr)
        raise SystemExit(127)

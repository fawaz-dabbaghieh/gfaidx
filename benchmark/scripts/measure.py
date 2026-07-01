#!/usr/bin/env python3
"""Run one benchmark command and record wall time plus process-tree RSS.

The workflow uses this helper instead of relying only on Snakemake benchmarks
because some commands spawn helper processes. Sampling the full process tree
keeps sort/conversion helpers visible in peak memory measurements.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import shlex
import subprocess
import sys
import time
from pathlib import Path
from typing import Iterable


def parse_args() -> argparse.Namespace:
    """Parse wrapper options and the command after `--`."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--metrics", required=True, help="JSON metrics output")
    parser.add_argument("--log", required=True, help="stderr/stdout log output")
    parser.add_argument(
        "--stdout",
        default="",
        help="Optional file for command stdout; stderr still goes to --log",
    )
    parser.add_argument(
        "--sample-interval",
        type=float,
        default=0.10,
        help="RSS sampling interval in seconds",
    )
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if not args.command or args.command[0] != "--" or len(args.command) == 1:
        parser.error("command must be supplied after --")
    args.command = args.command[1:]
    return args


def ps_snapshot() -> list[tuple[int, int, int]]:
    """Return visible processes as `(pid, ppid, rss_kb)` rows.

    The `ps -axo pid=,ppid=,rss=` form works on macOS and Linux and reports RSS
    in KiB on both platforms.
    """
    result = subprocess.run(
        ["ps", "-axo", "pid=,ppid=,rss="],
        check=True,
        capture_output=True,
        text=True,
    )
    rows: list[tuple[int, int, int]] = []
    for line in result.stdout.splitlines():
        parts = line.split()
        if len(parts) != 3:
            continue
        try:
            rows.append((int(parts[0]), int(parts[1]), int(parts[2])))
        except ValueError:
            continue
    return rows


def descendants(root_pid: int, rows: Iterable[tuple[int, int, int]]) -> set[int]:
    """Collect the root process and all descendants in one process snapshot."""
    children_by_parent: dict[int, list[int]] = {}
    for pid, ppid, _rss_kb in rows:
        children_by_parent.setdefault(ppid, []).append(pid)

    seen: set[int] = set()
    stack = [root_pid]
    while stack:
        pid = stack.pop()
        if pid in seen:
            continue
        seen.add(pid)
        stack.extend(children_by_parent.get(pid, []))
    return seen


def tree_rss_kb(root_pid: int) -> int:
    """Sum RSS for the root command and every visible descendant."""
    try:
        rows = ps_snapshot()
    except (OSError, subprocess.SubprocessError):
        return 0
    wanted = descendants(root_pid, rows)
    rss_by_pid = {pid: rss_kb for pid, _ppid, rss_kb in rows}
    return sum(rss_by_pid.get(pid, 0) for pid in wanted)


def maxrss_to_kb(value: int) -> int:
    """Convert a platform ru_maxrss value to KiB.

    Linux reports ru_maxrss in KiB, while macOS reports it in bytes.
    """
    if platform.system() == "Darwin":
        return int(value) // 1024
    return int(value)


def wait4_nohang(pid: int) -> tuple[int | None, int]:
    """Poll one child and return `(exit_code, maxrss_kb)`.

    `subprocess.Popen.poll()` uses waitpid, which discards resource usage. Using
    wait4 gives us OS accounting for the wrapped command itself, so very short
    commands still report memory even when process-tree sampling misses them.
    """
    waited_pid, status, usage = os.wait4(pid, os.WNOHANG)
    if waited_pid == 0:
        return None, 0
    return os.waitstatus_to_exitcode(status), maxrss_to_kb(int(usage.ru_maxrss))


def write_metrics(path: Path, metrics: dict[str, object]) -> None:
    """Write benchmark metrics as stable, machine-readable JSON."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(metrics, handle, indent=2, sort_keys=True)
        handle.write("\n")


def main() -> int:
    """Run the wrapped command and report its exit code."""
    args = parse_args()
    metrics_path = Path(args.metrics)
    log_path = Path(args.log)
    stdout_path = Path(args.stdout) if args.stdout else None

    log_path.parent.mkdir(parents=True, exist_ok=True)
    if stdout_path is not None:
        stdout_path.parent.mkdir(parents=True, exist_ok=True)

    start = time.monotonic()
    peak_rss_kb = 0
    wait4_peak_rss_kb = 0
    sample_count = 0
    # Poll process exit frequently so fast commands do not inherit the full RSS
    # sample interval as artificial wall-clock overhead.
    sample_interval = max(float(args.sample_interval), 0.001)
    exit_poll_interval = min(sample_interval, 0.01)

    with log_path.open("w", encoding="utf-8") as log_handle:
        if stdout_path is None:
            stdout_handle = log_handle
            close_stdout = False
        else:
            # Binary mode avoids corrupting native graph formats emitted to stdout.
            stdout_handle = stdout_path.open("wb")
            close_stdout = True

        try:
            proc = subprocess.Popen(
                args.command,
                stdout=stdout_handle,
                stderr=log_handle,
            )
            next_sample = time.monotonic()
            while True:
                now = time.monotonic()
                if now >= next_sample:
                    sample_count += 1
                    peak_rss_kb = max(peak_rss_kb, tree_rss_kb(proc.pid))
                    next_sample = now + sample_interval

                return_code, child_peak_rss_kb = wait4_nohang(proc.pid)
                wait4_peak_rss_kb = max(wait4_peak_rss_kb, child_peak_rss_kb)
                if return_code is not None:
                    proc.returncode = return_code
                    break
                time.sleep(min(exit_poll_interval, max(0.0, next_sample - time.monotonic())))
        finally:
            if close_stdout:
                stdout_handle.close()

    wall_seconds = time.monotonic() - start
    combined_peak_rss_kb = max(peak_rss_kb, wait4_peak_rss_kb)
    metrics = {
        "command": " ".join(shlex.quote(part) for part in args.command),
        "exit_code": int(return_code),
        "measurement_mode": "max_ps_tree_wait4",
        "peak_rss_kb": int(combined_peak_rss_kb),
        "peak_rss_kb_ps_tree": int(peak_rss_kb),
        "peak_rss_kb_wait4": int(wait4_peak_rss_kb),
        "sample_count": int(sample_count),
        "sample_interval_seconds": float(args.sample_interval),
        "wall_seconds": round(wall_seconds, 6),
    }
    write_metrics(metrics_path, metrics)

    if return_code != 0:
        print(f"command failed; see {log_path}", file=sys.stderr)
    return int(return_code)


if __name__ == "__main__":
    raise SystemExit(main())

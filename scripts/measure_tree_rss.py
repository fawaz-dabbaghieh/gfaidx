#!/usr/bin/env python3
"""Run a command and measure peak RSS across the full process tree.

This helper exists because plain timing tools often report only the parent
process, while `gfaidx index_gfa` can spawn child processes such as `sort`.
The script launches the command, redirects stdout/stderr to a log file, samples
RSS for the child and all descendants, and writes a small key=value metrics file
that the shell runner can parse without extra dependencies.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, Iterable, List, Set, Tuple


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments.

    The command to run is everything after `--`. Keeping the benchmarked command
    separate from the sampler options avoids ambiguous parsing and makes the
    wrapper predictable in shell scripts.
    """
    parser = argparse.ArgumentParser(
        description="Run a command and measure peak RSS across the process tree."
    )
    parser.add_argument(
        "--log",
        required=True,
        help="Path to the combined stdout/stderr log written for the benchmarked command.",
    )
    parser.add_argument(
        "--metrics",
        required=True,
        help="Path to the key=value metrics file produced by this script.",
    )
    parser.add_argument(
        "--sample-interval",
        type=float,
        default=0.10,
        help="Sampling interval in seconds for RSS polling (default: 0.10).",
    )
    parser.add_argument(
        "command",
        nargs=argparse.REMAINDER,
        help="Command to execute, prefixed with `--`.",
    )
    args = parser.parse_args()
    if not args.command or args.command[0] != "--" or len(args.command) == 1:
        parser.error("command must be provided after --")
    args.command = args.command[1:]
    return args


def run_ps_snapshot() -> List[Tuple[int, int, int]]:
    """Return a snapshot of pid, ppid, rss_kb for all visible processes.

    `ps -axo pid=,ppid=,rss=` is available on both macOS and Linux and returns
    RSS in kilobytes, which matches the unit used by `/usr/bin/time -v`.
    """
    result = subprocess.run(
        ["ps", "-axo", "pid=,ppid=,rss="],
        check=True,
        capture_output=True,
        text=True,
    )

    rows: List[Tuple[int, int, int]] = []
    for line in result.stdout.splitlines():
        parts = line.split()
        if len(parts) != 3:
            continue
        try:
            pid = int(parts[0])
            ppid = int(parts[1])
            rss_kb = int(parts[2])
        except ValueError:
            continue
        rows.append((pid, ppid, rss_kb))
    return rows


def collect_descendants(root_pid: int, rows: Iterable[Tuple[int, int, int]]) -> Set[int]:
    """Collect the root process and every descendant process.

    We rebuild the parent/child graph from each `ps` snapshot and then walk it
    starting at the benchmarked process. This captures short-lived helpers as
    long as they appear in at least one sample.
    """
    by_parent: Dict[int, List[int]] = {}
    for pid, ppid, _rss_kb in rows:
        by_parent.setdefault(ppid, []).append(pid)

    descendants: Set[int] = set()
    stack = [root_pid]
    while stack:
        pid = stack.pop()
        if pid in descendants:
            continue
        descendants.add(pid)
        stack.extend(by_parent.get(pid, []))
    return descendants


def sum_tree_rss_kb(root_pid: int) -> int:
    """Sum RSS for the root process and all descendants.

    If `ps` fails momentarily we return 0 for that sample rather than aborting
    the whole benchmark. The next sample usually recovers, and the peak is what
    matters for this workflow.
    """
    try:
        rows = run_ps_snapshot()
    except subprocess.SubprocessError:
        return 0

    descendants = collect_descendants(root_pid, rows)
    rss_by_pid = {pid: rss_kb for pid, _ppid, rss_kb in rows}
    return sum(rss_by_pid.get(pid, 0) for pid in descendants)


def write_metrics(path: Path, metrics: Dict[str, object]) -> None:
    """Write the metrics file as simple key=value pairs.

    The shell runner sources this file line by line, so the format is kept
    intentionally minimal and robust.
    """
    with path.open("w", encoding="utf-8") as handle:
        for key, value in metrics.items():
            handle.write(f"{key}={value}\n")


def run_with_ps_sampling(args: argparse.Namespace, log_path: Path) -> Tuple[int, Dict[str, object]]:
    """Run the command with process-tree RSS sampling via `ps`.

    This is the preferred mode because it captures the root process and all
    descendants over time. It is used when the environment allows invoking
    `ps`, which is typical on an unrestricted local shell.
    """
    start = time.monotonic()
    peak_rss_kb = 0
    sample_count = 0

    with log_path.open("w", encoding="utf-8") as log_handle:
        proc = subprocess.Popen(
            args.command,
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            text=True,
        )

        while True:
            sample_count += 1
            peak_rss_kb = max(peak_rss_kb, sum_tree_rss_kb(proc.pid))
            return_code = proc.poll()
            if return_code is not None:
                break
            time.sleep(args.sample_interval)

        sample_count += 1
        peak_rss_kb = max(peak_rss_kb, sum_tree_rss_kb(proc.pid))

    wall_seconds = time.monotonic() - start
    metrics = {
        "wall_seconds": f"{wall_seconds:.6f}",
        "peak_rss_kb": peak_rss_kb,
        "exit_code": return_code,
        "sample_interval_seconds": args.sample_interval,
        "sample_count": sample_count,
        "pid": proc.pid,
        "measurement_mode": "ps_tree",
    }
    return int(return_code), metrics


def parse_log_peak_rss_kb(log_path: Path) -> int:
    """Parse the highest RSS snapshot emitted by gfaidx's own logging.

    This is the last-resort fallback for environments where external process
    inspection is blocked. It only sees the parent process snapshots that gfaidx
    logs itself, so it is less complete than process-tree sampling, but still
    useful for local regression tracking.
    """
    unit_scale = {
        "B": 1 / 1024.0,
        "KB": 1.0,
        "MB": 1024.0,
        "GB": 1024.0 * 1024.0,
        "TB": 1024.0 * 1024.0 * 1024.0,
    }
    peak_rss_kb = 0
    pattern = re.compile(r"RSS\s+([0-9]+(?:\.[0-9]+)?)\s+([KMGTP]?B)\)")

    with log_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            match = pattern.search(line)
            if not match:
                continue
            value = float(match.group(1))
            unit = match.group(2)
            rss_kb = int(value * unit_scale[unit])
            peak_rss_kb = max(peak_rss_kb, rss_kb)
    return peak_rss_kb


def run_with_log_rss_fallback(args: argparse.Namespace, log_path: Path) -> Tuple[int, Dict[str, object]]:
    """Run the command normally and derive peak RSS from the command log.

    This path is used only when both process-tree sampling and `/usr/bin/time`
    style measurement are unavailable in the current environment.
    """
    start = time.monotonic()
    with log_path.open("w", encoding="utf-8") as log_handle:
        proc = subprocess.Popen(
            args.command,
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            text=True,
        )
        return_code = proc.wait()

    wall_seconds = time.monotonic() - start
    metrics = {
        "wall_seconds": f"{wall_seconds:.6f}",
        "peak_rss_kb": parse_log_peak_rss_kb(log_path),
        "exit_code": return_code,
        "sample_interval_seconds": args.sample_interval,
        "sample_count": 0,
        "pid": proc.pid,
        "measurement_mode": "log_rss",
    }
    return int(return_code), metrics


def main() -> int:
    args = parse_args()

    log_path = Path(args.log)
    metrics_path = Path(args.metrics)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    metrics_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        run_ps_snapshot()
        return_code, metrics = run_with_ps_sampling(args, log_path)
    except (PermissionError, FileNotFoundError, subprocess.SubprocessError):
        return_code, metrics = run_with_log_rss_fallback(args, log_path)

    write_metrics(metrics_path, metrics)

    if return_code != 0:
        print(
            f"benchmark command failed with exit code {return_code}; see {log_path}",
            file=sys.stderr,
        )
    return int(return_code)


if __name__ == "__main__":
    raise SystemExit(main())

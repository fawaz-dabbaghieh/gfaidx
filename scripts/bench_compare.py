#!/usr/bin/env python3
"""Compare the latest local benchmark results with saved baselines.

The benchmark runner appends every run to benchmarks/results.tsv. This script
loads the result history, keeps the latest run per dataset, loads the saved
server baselines, and prints a compact terminal table so regressions are easy to
spot without opening a spreadsheet.
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Dict, List


REPO_ROOT = Path(__file__).resolve().parent.parent
BASELINES_TSV = REPO_ROOT / "benchmarks" / "baselines.tsv"
RESULTS_TSV = REPO_ROOT / "benchmarks" / "results.tsv"


def read_tsv(path: Path) -> List[Dict[str, str]]:
    """Read a TSV file while skipping comment lines.

    The benchmark manifests use `#` for human-readable notes above the header,
    so we strip those lines before passing the content to DictReader.
    """
    if not path.exists():
        return []

    rows: List[Dict[str, str]] = []
    with path.open("r", encoding="utf-8", newline="") as handle:
        filtered = [line for line in handle if line.strip() and not line.startswith("#")]
    if not filtered:
        return rows

    reader = csv.DictReader(filtered, delimiter="\t")
    for row in reader:
        rows.append(row)
    return rows


def latest_results_by_dataset(rows: List[Dict[str, str]]) -> Dict[str, Dict[str, str]]:
    """Keep the latest result row for each dataset.

    Timestamps are written in sortable ISO-like form, so lexicographic ordering
    is sufficient here.
    """
    latest: Dict[str, Dict[str, str]] = {}
    for row in rows:
        dataset = row["dataset"]
        current = latest.get(dataset)
        if current is None or row["timestamp"] > current["timestamp"]:
            latest[dataset] = row
    return latest


def format_delta(current: float, baseline: float) -> str:
    """Format absolute and percent deltas for terminal output."""
    delta = current - baseline
    if baseline == 0:
        return f"{delta:.2f}"
    pct = (delta / baseline) * 100.0
    return f"{delta:+.2f} ({pct:+.1f}%)"


def print_table(latest: Dict[str, Dict[str, str]], baselines: Dict[str, Dict[str, str]]) -> None:
    """Print a simple fixed-width comparison table.

    The output is intentionally plain text so it can be pasted into commit
    messages, issues, or terminal notes without further processing.
    """
    header = (
        f"{'dataset':<8} {'latest_wall_s':>14} {'baseline_wall_s':>16} {'delta_wall':>18} "
        f"{'latest_peak_kb':>16} {'baseline_peak_kb':>18} {'delta_peak':>18} {'mode':>10} {'timestamp':>24}"
    )
    print(header)
    print("-" * len(header))

    dataset_names = sorted(set(latest) | set(baselines))
    for dataset in dataset_names:
        latest_row = latest.get(dataset)
        baseline_row = baselines.get(dataset)

        latest_wall = latest_peak = latest_ts = latest_mode = "-"
        baseline_wall = baseline_peak = "-"
        delta_wall = delta_peak = "-"

        if latest_row is not None:
            latest_wall = f"{float(latest_row['wall_seconds']):.2f}"
            latest_peak = f"{int(latest_row['peak_rss_kb'])}"
            latest_ts = latest_row["timestamp"]
            latest_mode = latest_row.get("measurement_mode", "-")
        if baseline_row is not None:
            baseline_wall = f"{float(baseline_row['wall_seconds']):.2f}"
            baseline_peak = f"{int(baseline_row['peak_rss_kb'])}"
        if latest_row is not None and baseline_row is not None:
            delta_wall = format_delta(float(latest_row["wall_seconds"]), float(baseline_row["wall_seconds"]))
            delta_peak = format_delta(float(latest_row["peak_rss_kb"]), float(baseline_row["peak_rss_kb"]))

        print(
            f"{dataset:<8} {latest_wall:>14} {baseline_wall:>16} {delta_wall:>18} "
            f"{latest_peak:>16} {baseline_peak:>18} {delta_peak:>18} {latest_mode:>10} {latest_ts:>24}"
        )


def main() -> int:
    baseline_rows = read_tsv(BASELINES_TSV)
    result_rows = read_tsv(RESULTS_TSV)

    baselines = {row["dataset"]: row for row in baseline_rows}
    latest = latest_results_by_dataset(result_rows)

    if not baselines and not latest:
        print("No baselines or benchmark results found.")
        return 0

    print_table(latest, baselines)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

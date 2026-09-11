#!/usr/bin/env python3
"""Summarize and plot repeated gfaidx versus gbz-base interval benchmarks.

Technical repetitions and genomic locations are handled separately. For each
query and tool, run 1 is the first execution and the median of runs 2..N is the
warm execution estimate. Means and bootstrap 95% confidence intervals are then
calculated across query locations of the same interval length.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import math
import os
import random
import re
import statistics
import sys
import tempfile
from collections import defaultdict
from pathlib import Path
from typing import Iterable


# Thread-specific gfaidx runs use related blue/green colors while gbz-base keeps
# a distinct magenta. The cycles also support thread sweeps beyond the default
# 1- and 8-thread comparison without hard-coding those two configurations.
GFAIDX_TOOL_PATTERN = re.compile(r"^gfaidx-([1-9][0-9]*)t$")
GFAIDX_COLORS = ("#0072B2", "#56B4E9", "#009E73", "#E69F00", "#D55E00")
GFAIDX_MARKERS = ("o", "^", "D", "v", "P")
GBZ_BASE_STYLE = ("#CC79A7", "s")

RAW_REQUIRED_COLUMNS = {
    "query_id",
    "locus_id",
    "reference",
    "contig",
    "start",
    "end",
    "interval_bp",
    "tool",
    "run_index",
    "elapsed_seconds",
    "max_rss_kb",
    "output_nodes",
    "output_edges",
    "output_paths",
    "output_bytes",
    "exit_status",
}

LOCATION_FIELDS = [
    "query_id",
    "locus_id",
    "reference",
    "contig",
    "start",
    "end",
    "interval_bp",
    "tool",
    "repetitions",
    "first_elapsed_seconds",
    "warm_median_elapsed_seconds",
    "all_mean_elapsed_seconds",
    "first_max_rss_kb",
    "warm_median_max_rss_kb",
    "all_mean_max_rss_kb",
    "output_nodes",
    "output_edges",
    "output_paths",
    "output_bytes",
]

SIZE_FIELDS = [
    "interval_bp",
    "tool",
    "locations",
    "runs",
    "first_elapsed_mean",
    "first_elapsed_ci_low",
    "first_elapsed_ci_high",
    "warm_elapsed_mean",
    "warm_elapsed_ci_low",
    "warm_elapsed_ci_high",
    "first_rss_kb_mean",
    "first_rss_kb_ci_low",
    "first_rss_kb_ci_high",
    "warm_rss_kb_mean",
    "warm_rss_kb_ci_low",
    "warm_rss_kb_ci_high",
    "output_nodes_mean",
    "output_nodes_ci_low",
    "output_nodes_ci_high",
]

REPETITION_FIELDS = [
    "interval_bp",
    "tool",
    "run_index",
    "locations",
    "elapsed_mean",
    "elapsed_ci_low",
    "elapsed_ci_high",
    "rss_kb_mean",
    "rss_kb_ci_low",
    "rss_kb_ci_high",
    "elapsed_relative_to_run1_mean",
    "elapsed_relative_to_run1_ci_low",
    "elapsed_relative_to_run1_ci_high",
]

CACHE_FIELDS = [
    "tool",
    "run_index",
    "queries",
    "elapsed_relative_to_run1_mean",
    "elapsed_relative_to_run1_ci_low",
    "elapsed_relative_to_run1_ci_high",
]


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw", required=True, help="raw_runs.tsv from the runner")
    parser.add_argument(
        "--output-dir",
        default="",
        help="output root; defaults to the directory containing --raw",
    )
    parser.add_argument(
        "--bootstrap-samples",
        type=int,
        default=10000,
        help="bootstrap resamples per confidence interval (default: 10000)",
    )
    parser.add_argument("--seed", type=int, default=17, help="bootstrap seed")
    parser.add_argument(
        "--formats",
        default="png,svg",
        help="comma-separated Matplotlib output formats (default: png,svg)",
    )
    args = parser.parse_args()
    if args.bootstrap_samples <= 0:
        parser.error("--bootstrap-samples must be positive")
    return args


def parse_number(value: str, field: str, integer: bool = False) -> float | int:
    """Parse a required finite number and name malformed fields clearly."""
    try:
        result = int(value) if integer else float(value)
    except ValueError as exc:
        raise ValueError(f"invalid {field} value: {value!r}") from exc
    if not integer and not math.isfinite(float(result)):
        raise ValueError(f"non-finite {field} value: {value!r}")
    return result


def read_raw(path: Path) -> list[dict[str, object]]:
    """Read successful raw executions and convert measurement columns."""
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        missing = RAW_REQUIRED_COLUMNS.difference(reader.fieldnames or [])
        if missing:
            raise ValueError(f"{path} is missing columns: {', '.join(sorted(missing))}")

        rows: list[dict[str, object]] = []
        failed = 0
        for raw in reader:
            exit_status = int(parse_number(raw["exit_status"], "exit_status", True))
            if exit_status != 0:
                failed += 1
                continue
            row: dict[str, object] = dict(raw)
            for field in (
                "start",
                "end",
                "interval_bp",
                "run_index",
                "max_rss_kb",
                "output_nodes",
                "output_edges",
                "output_paths",
                "output_bytes",
            ):
                row[field] = int(parse_number(raw[field], field, True))
            row["elapsed_seconds"] = float(
                parse_number(raw["elapsed_seconds"], "elapsed_seconds")
            )
            rows.append(row)

    if failed:
        print(f"warning: ignored {failed} failed raw execution(s)", file=sys.stderr)
    if not rows:
        raise ValueError(f"{path} contains no successful executions")
    return rows


def stable_seed(base_seed: int, key: str) -> int:
    """Derive a reproducible per-group seed without Python's randomized hash."""
    digest = hashlib.sha256(f"{base_seed}\0{key}".encode()).digest()
    return int.from_bytes(digest[:8], "little")


def percentile(sorted_values: list[float], fraction: float) -> float:
    """Linearly interpolate a percentile from an already sorted vector."""
    if len(sorted_values) == 1:
        return sorted_values[0]
    position = fraction * (len(sorted_values) - 1)
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return sorted_values[lower]
    weight = position - lower
    return sorted_values[lower] * (1.0 - weight) + sorted_values[upper] * weight


def mean_and_ci(
    values: Iterable[float], samples: int, base_seed: int, key: str
) -> tuple[float | None, float | None, float | None]:
    """Return a mean and deterministic percentile-bootstrap 95% interval."""
    data = [float(value) for value in values]
    if not data:
        return None, None, None
    center = statistics.fmean(data)
    if len(data) == 1:
        return center, center, center

    rng = random.Random(stable_seed(base_seed, key))
    count = len(data)
    bootstrapped = [
        sum(data[rng.randrange(count)] for _ in range(count)) / count
        for _ in range(samples)
    ]
    bootstrapped.sort()
    return center, percentile(bootstrapped, 0.025), percentile(bootstrapped, 0.975)


def consistent_value(rows: list[dict[str, object]], field: str) -> object:
    """Return a field that must be identical across repetitions."""
    values = {row[field] for row in rows}
    if len(values) != 1:
        query = rows[0]["query_id"]
        tool = rows[0]["tool"]
        raise ValueError(f"{field} changed across repetitions for {query}/{tool}: {values}")
    return next(iter(values))


def build_location_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    """Collapse technical repetitions into one first and one warm value."""
    grouped: dict[tuple[str, str], list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        grouped[(str(row["query_id"]), str(row["tool"]))].append(row)

    output: list[dict[str, object]] = []
    for (query_id, tool), group in sorted(grouped.items()):
        group.sort(key=lambda row: int(row["run_index"]))
        run_indexes = [int(row["run_index"]) for row in group]
        if len(set(run_indexes)) != len(run_indexes):
            raise ValueError(f"duplicate run_index for {query_id}/{tool}")
        if run_indexes[0] != 1:
            raise ValueError(f"run 1 is missing for {query_id}/{tool}")

        first = group[0]
        warm = [row for row in group if int(row["run_index"]) >= 2]
        elapsed = [float(row["elapsed_seconds"]) for row in group]
        rss = [float(row["max_rss_kb"]) for row in group]
        output.append(
            {
                "query_id": query_id,
                "locus_id": consistent_value(group, "locus_id"),
                "reference": consistent_value(group, "reference"),
                "contig": consistent_value(group, "contig"),
                "start": consistent_value(group, "start"),
                "end": consistent_value(group, "end"),
                "interval_bp": consistent_value(group, "interval_bp"),
                "tool": tool,
                "repetitions": len(group),
                "first_elapsed_seconds": float(first["elapsed_seconds"]),
                "warm_median_elapsed_seconds": (
                    statistics.median(float(row["elapsed_seconds"]) for row in warm)
                    if warm
                    else None
                ),
                "all_mean_elapsed_seconds": statistics.fmean(elapsed),
                "first_max_rss_kb": float(first["max_rss_kb"]),
                "warm_median_max_rss_kb": (
                    statistics.median(float(row["max_rss_kb"]) for row in warm)
                    if warm
                    else None
                ),
                "all_mean_max_rss_kb": statistics.fmean(rss),
                "output_nodes": consistent_value(group, "output_nodes"),
                "output_edges": consistent_value(group, "output_edges"),
                "output_paths": consistent_value(group, "output_paths"),
                "output_bytes": consistent_value(group, "output_bytes"),
            }
        )
    return output


def add_stats(
    row: dict[str, object],
    prefix: str,
    values: Iterable[float],
    samples: int,
    seed: int,
    key: str,
) -> None:
    """Add `<prefix>_mean/ci_low/ci_high` fields to a summary row."""
    center, low, high = mean_and_ci(values, samples, seed, key)
    row[f"{prefix}_mean"] = center
    row[f"{prefix}_ci_low"] = low
    row[f"{prefix}_ci_high"] = high


def build_size_rows(
    locations: list[dict[str, object]], samples: int, seed: int
) -> list[dict[str, object]]:
    """Aggregate first and warm per-location values by interval length."""
    grouped: dict[tuple[int, str], list[dict[str, object]]] = defaultdict(list)
    for location in locations:
        grouped[(int(location["interval_bp"]), str(location["tool"]))].append(location)

    output: list[dict[str, object]] = []
    for (interval_bp, tool), group in sorted(grouped.items()):
        row: dict[str, object] = {
            "interval_bp": interval_bp,
            "tool": tool,
            "locations": len(group),
            "runs": ",".join(str(value) for value in sorted({int(x["repetitions"]) for x in group})),
        }
        key = f"size:{interval_bp}:{tool}"
        add_stats(
            row,
            "first_elapsed",
            (float(x["first_elapsed_seconds"]) for x in group),
            samples,
            seed,
            key + ":first_elapsed",
        )
        add_stats(
            row,
            "warm_elapsed",
            (
                float(x["warm_median_elapsed_seconds"])
                for x in group
                if x["warm_median_elapsed_seconds"] is not None
            ),
            samples,
            seed,
            key + ":warm_elapsed",
        )
        add_stats(
            row,
            "first_rss_kb",
            (float(x["first_max_rss_kb"]) for x in group),
            samples,
            seed,
            key + ":first_rss",
        )
        add_stats(
            row,
            "warm_rss_kb",
            (
                float(x["warm_median_max_rss_kb"])
                for x in group
                if x["warm_median_max_rss_kb"] is not None
            ),
            samples,
            seed,
            key + ":warm_rss",
        )
        add_stats(
            row,
            "output_nodes",
            (float(x["output_nodes"]) for x in group),
            samples,
            seed,
            key + ":nodes",
        )
        output.append(row)
    return output


def build_repetition_rows(
    rows: list[dict[str, object]], samples: int, seed: int
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    """Aggregate actual and run-1-normalized measurements by repetition."""
    first_elapsed: dict[tuple[str, str], float] = {}
    for row in rows:
        if int(row["run_index"]) == 1:
            first_elapsed[(str(row["query_id"]), str(row["tool"]))] = float(
                row["elapsed_seconds"]
            )

    grouped: dict[tuple[int, str, int], list[dict[str, object]]] = defaultdict(list)
    overall: dict[tuple[str, int], list[float]] = defaultdict(list)
    zero_baselines: set[tuple[str, str]] = set()
    for row in rows:
        key = (str(row["query_id"]), str(row["tool"]))
        baseline = first_elapsed.get(key)
        if baseline is None:
            raise ValueError(f"missing run-1 elapsed time for {key[0]}/{key[1]}")
        # GNU time reports only hundredths of a second. Keep absolute metrics
        # for very fast queries, but do not divide by a rounded zero baseline.
        ratio = float(row["elapsed_seconds"]) / baseline if baseline > 0 else None
        if ratio is None:
            zero_baselines.add(key)
        row_with_ratio = dict(row)
        row_with_ratio["elapsed_relative_to_run1"] = ratio
        group_key = (int(row["interval_bp"]), str(row["tool"]), int(row["run_index"]))
        grouped[group_key].append(row_with_ratio)
        if ratio is not None:
            overall[(str(row["tool"]), int(row["run_index"]))].append(ratio)

    if zero_baselines:
        print(
            "warning: omitted normalized cache ratios for "
            f"{len(zero_baselines)} query/tool pair(s) whose run 1 rounded to zero",
            file=sys.stderr,
        )

    repetition_rows: list[dict[str, object]] = []
    for (interval_bp, tool, run_index), group in sorted(grouped.items()):
        row = {
            "interval_bp": interval_bp,
            "tool": tool,
            "run_index": run_index,
            "locations": len(group),
        }
        key = f"repeat:{interval_bp}:{tool}:{run_index}"
        add_stats(
            row,
            "elapsed",
            (float(x["elapsed_seconds"]) for x in group),
            samples,
            seed,
            key + ":elapsed",
        )
        add_stats(
            row,
            "rss_kb",
            (float(x["max_rss_kb"]) for x in group),
            samples,
            seed,
            key + ":rss",
        )
        add_stats(
            row,
            "elapsed_relative_to_run1",
            (
                float(x["elapsed_relative_to_run1"])
                for x in group
                if x["elapsed_relative_to_run1"] is not None
            ),
            samples,
            seed,
            key + ":relative",
        )
        repetition_rows.append(row)

    cache_rows: list[dict[str, object]] = []
    for (tool, run_index), ratios in sorted(overall.items()):
        row = {"tool": tool, "run_index": run_index, "queries": len(ratios)}
        add_stats(
            row,
            "elapsed_relative_to_run1",
            ratios,
            samples,
            seed,
            f"cache:{tool}:{run_index}",
        )
        cache_rows.append(row)
    return repetition_rows, cache_rows


def format_cell(value: object) -> str:
    """Format optional floating-point values consistently in TSV output."""
    if value is None:
        return "NA"
    if isinstance(value, float):
        return f"{value:.6f}"
    return str(value)


def write_table(path: Path, fields: list[str], rows: list[dict[str, object]]) -> None:
    """Write one stable, tab-separated summary table."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, delimiter="\t", lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow({field: format_cell(row.get(field)) for field in fields})


def configure_matplotlib() -> None:
    """Select a headless backend and writable cache before importing pyplot."""
    os.environ.setdefault(
        "MPLCONFIGDIR", str(Path(tempfile.gettempdir()) / "gfaidx-gbz-matplotlib")
    )
    os.environ.setdefault(
        "XDG_CACHE_HOME", str(Path(tempfile.gettempdir()) / "gfaidx-gbz-xdg-cache")
    )
    Path(os.environ["MPLCONFIGDIR"]).mkdir(parents=True, exist_ok=True)
    Path(os.environ["XDG_CACHE_HOME"]).mkdir(parents=True, exist_ok=True)


def tool_sort_key(tool: str) -> tuple[int, int, str]:
    """Order gfaidx variants by thread count and place gbz-base after them."""
    match = GFAIDX_TOOL_PATTERN.fullmatch(tool)
    if match:
        return 0, int(match.group(1)), tool
    # Accept raw tables made by the earlier two-tool runner as a 1-thread run.
    if tool == "gfaidx":
        return 0, 1, tool
    if tool == "gbz-base":
        return 1, 0, tool
    return 2, 0, tool


def ordered_tools(rows: Iterable[dict[str, object]]) -> list[str]:
    """Discover tool variants from the raw data in a stable display order."""
    return sorted({str(row["tool"]) for row in rows}, key=tool_sort_key)


def build_tool_styles(tools: list[str]) -> dict[str, tuple[str, str]]:
    """Assign colors and markers consistently across every generated plot."""
    styles: dict[str, tuple[str, str]] = {}
    gfaidx_index = 0
    for tool in tools:
        if tool == "gbz-base":
            styles[tool] = GBZ_BASE_STYLE
        elif tool == "gfaidx" or GFAIDX_TOOL_PATTERN.fullmatch(tool):
            styles[tool] = (
                GFAIDX_COLORS[gfaidx_index % len(GFAIDX_COLORS)],
                GFAIDX_MARKERS[gfaidx_index % len(GFAIDX_MARKERS)],
            )
            gfaidx_index += 1
        else:
            styles[tool] = ("#666666", "x")
    return styles


def tool_label(tool: str) -> str:
    """Turn the raw tool identifier into a readable plot label."""
    match = GFAIDX_TOOL_PATTERN.fullmatch(tool)
    if not match:
        return tool
    threads = int(match.group(1))
    suffix = "thread" if threads == 1 else "threads"
    return f"gfaidx ({threads} {suffix})"


def error_values(row: dict[str, object], prefix: str, scale: float = 1.0) -> tuple[float, list[float]]:
    """Convert a center and confidence limits into Matplotlib y-error lengths."""
    center = float(row[f"{prefix}_mean"]) / scale
    low = float(row[f"{prefix}_ci_low"]) / scale
    high = float(row[f"{prefix}_ci_high"]) / scale
    return center, [max(0.0, center - low), max(0.0, high - center)]


def save_figure(figure: object, output: Path, formats: list[str]) -> None:
    """Save one figure in every requested format."""
    for extension in formats:
        figure.savefig(output.with_suffix(f".{extension}"), dpi=200, bbox_inches="tight")


def plot_scaling(
    size_rows: list[dict[str, object]],
    output_dir: Path,
    formats: list[str],
    metric: str,
    tools: list[str],
    styles: dict[str, tuple[str, str]],
) -> None:
    """Plot first and warm interval scaling for elapsed time or peak RSS."""
    from matplotlib import pyplot as plt
    from matplotlib.ticker import FuncFormatter

    if metric == "elapsed":
        prefixes = ("first_elapsed", "warm_elapsed")
        ylabel = "Wall time (seconds)"
        scale = 1.0
        basename = "interval_time_first_and_warm"
    else:
        prefixes = ("first_rss_kb", "warm_rss_kb")
        ylabel = "Peak RSS (GiB)"
        scale = 1024.0 * 1024.0
        basename = "interval_memory_first_and_warm"

    figure, axes = plt.subplots(1, 2, figsize=(11.5, 4.4), sharey=True)
    for axis, prefix, title in zip(axes, prefixes, ("First execution", "Warm repetitions (median 2..N)")):
        plotted_y: list[float] = []
        for tool in tools:
            selected = [
                row
                for row in size_rows
                if row["tool"] == tool and row.get(f"{prefix}_mean") is not None
            ]
            selected.sort(key=lambda row: int(row["interval_bp"]))
            if not selected:
                continue
            centers_and_errors = [error_values(row, prefix, scale) for row in selected]
            plotted_y.extend(item[0] for item in centers_and_errors)
            axis.errorbar(
                [int(row["interval_bp"]) for row in selected],
                [item[0] for item in centers_and_errors],
                yerr=[
                    [item[1][0] for item in centers_and_errors],
                    [item[1][1] for item in centers_and_errors],
                ],
                color=styles[tool][0],
                marker=styles[tool][1],
                linewidth=1.8,
                capsize=3,
                label=tool_label(tool),
            )
        axis.set_xscale("log")
        # GNU time rounds very short test commands to zero. Real graph queries
        # normally use the log scale; retain a readable linear axis for zeros.
        if plotted_y and min(plotted_y) > 0:
            axis.set_yscale("log")
        axis.xaxis.set_major_formatter(FuncFormatter(lambda value, _pos: format_bp(value)))
        axis.set_title(title)
        axis.set_xlabel("Requested interval length")
        axis.grid(alpha=0.25)
    axes[0].set_ylabel(ylabel)
    axes[0].legend(frameon=False)
    figure.suptitle("Interval extraction comparison")
    save_figure(figure, output_dir / basename, formats)
    plt.close(figure)


def format_bp(value: float) -> str:
    """Format a base-pair count compactly for axes and filenames."""
    if value >= 1_000_000_000:
        return f"{value / 1_000_000_000:g} Gb"
    if value >= 1_000_000:
        return f"{value / 1_000_000:g} Mb"
    if value >= 1_000:
        return f"{value / 1_000:g} kb"
    return f"{value:g} bp"


def plot_location_variation(
    locations: list[dict[str, object]],
    size_rows: list[dict[str, object]],
    output_dir: Path,
    formats: list[str],
    tools: list[str],
    styles: dict[str, tuple[str, str]],
) -> None:
    """Show warm measurements from every location behind the aggregate error bars."""
    from matplotlib import pyplot as plt
    from matplotlib.ticker import FuncFormatter

    # One panel per tool keeps multiple gfaidx thread variants readable instead
    # of overlaying all genomic locations in a single dense scatter plot.
    figure, axes_grid = plt.subplots(
        1, len(tools), figsize=(5.2 * len(tools), 4.4), sharey=True, squeeze=False
    )
    axes = list(axes_grid[0])
    for axis, tool in zip(axes, tools):
        individual = [
            row
            for row in locations
            if row["tool"] == tool and row["warm_median_elapsed_seconds"] is not None
        ]
        axis.scatter(
            [int(row["interval_bp"]) for row in individual],
            [float(row["warm_median_elapsed_seconds"]) for row in individual],
            color=styles[tool][0],
            alpha=0.38,
            s=24,
            label="individual locations",
        )
        aggregates = [
            row
            for row in size_rows
            if row["tool"] == tool and row["warm_elapsed_mean"] is not None
        ]
        aggregates.sort(key=lambda row: int(row["interval_bp"]))
        values = [error_values(row, "warm_elapsed") for row in aggregates]
        axis.errorbar(
            [int(row["interval_bp"]) for row in aggregates],
            [item[0] for item in values],
            yerr=[
                [item[1][0] for item in values],
                [item[1][1] for item in values],
            ],
            color=styles[tool][0],
            marker=styles[tool][1],
            linewidth=1.8,
            capsize=3,
            label="mean and 95% CI",
        )
        axis.set_xscale("log")
        plotted_y = [float(row["warm_median_elapsed_seconds"]) for row in individual]
        plotted_y.extend(item[0] for item in values)
        if plotted_y and min(plotted_y) > 0:
            axis.set_yscale("log")
        axis.xaxis.set_major_formatter(FuncFormatter(lambda value, _pos: format_bp(value)))
        axis.set_title(tool_label(tool))
        axis.set_xlabel("Requested interval length")
        axis.grid(alpha=0.25)
        axis.legend(frameon=False)
    axes[0].set_ylabel("Warm wall time (seconds)")
    figure.suptitle("Variation among genomic locations")
    save_figure(figure, output_dir / "interval_time_location_variation", formats)
    plt.close(figure)


def plot_cache_effect(
    cache_rows: list[dict[str, object]],
    output_dir: Path,
    formats: list[str],
    tools: list[str],
    styles: dict[str, tuple[str, str]],
) -> None:
    """Plot elapsed time relative to run 1 across all interval queries."""
    from matplotlib import pyplot as plt

    figure, axis = plt.subplots(figsize=(6.8, 4.6))
    for tool in tools:
        selected = [
            row
            for row in cache_rows
            if row["tool"] == tool
            and row["elapsed_relative_to_run1_mean"] is not None
        ]
        selected.sort(key=lambda row: int(row["run_index"]))
        if not selected:
            continue
        values = [error_values(row, "elapsed_relative_to_run1") for row in selected]
        axis.errorbar(
            [int(row["run_index"]) for row in selected],
            [item[0] for item in values],
            yerr=[
                [item[1][0] for item in values],
                [item[1][1] for item in values],
            ],
            color=styles[tool][0],
            marker=styles[tool][1],
            linewidth=1.8,
            capsize=3,
            label=tool_label(tool),
        )
    axis.axhline(1.0, color="#666666", linestyle="--", linewidth=1)
    axis.set_xlabel("Repetition")
    axis.set_ylabel("Wall time relative to run 1")
    axis.set_title("Repeated-query page-cache effect")
    axis.grid(alpha=0.25)
    handles, labels = axis.get_legend_handles_labels()
    if handles:
        axis.legend(frameon=False)
    save_figure(figure, output_dir / "cache_effect_normalized", formats)
    plt.close(figure)


def plot_repetitions_by_size(
    repetition_rows: list[dict[str, object]],
    output_dir: Path,
    formats: list[str],
    tools: list[str],
    styles: dict[str, tuple[str, str]],
) -> None:
    """Plot actual run time by repetition in one panel per interval size."""
    from matplotlib import pyplot as plt

    sizes = sorted({int(row["interval_bp"]) for row in repetition_rows})
    columns = min(3, len(sizes))
    rows = math.ceil(len(sizes) / columns)
    figure, axes_grid = plt.subplots(rows, columns, figsize=(5.0 * columns, 3.8 * rows), squeeze=False)
    axes = [axis for grid_row in axes_grid for axis in grid_row]
    for axis, interval_bp in zip(axes, sizes):
        for tool in tools:
            selected = [
                row
                for row in repetition_rows
                if int(row["interval_bp"]) == interval_bp and row["tool"] == tool
            ]
            selected.sort(key=lambda row: int(row["run_index"]))
            values = [error_values(row, "elapsed") for row in selected]
            axis.errorbar(
                [int(row["run_index"]) for row in selected],
                [item[0] for item in values],
                yerr=[
                    [item[1][0] for item in values],
                    [item[1][1] for item in values],
                ],
                color=styles[tool][0],
                marker=styles[tool][1],
                linewidth=1.5,
                capsize=2,
                label=tool_label(tool),
            )
        axis.set_title(format_bp(interval_bp))
        axis.set_xlabel("Repetition")
        axis.set_ylabel("Wall time (seconds)")
        axis.grid(alpha=0.25)
    for axis in axes[len(sizes) :]:
        axis.set_visible(False)
    axes[0].legend(frameon=False)
    figure.suptitle("Repeated interval extraction by requested size")
    save_figure(figure, output_dir / "cache_effect_by_interval_size", formats)
    plt.close(figure)


def plot_output_nodes(
    size_rows: list[dict[str, object]],
    output_dir: Path,
    formats: list[str],
    tools: list[str],
    styles: dict[str, tuple[str, str]],
) -> None:
    """Plot returned node counts so workload differences remain visible."""
    from matplotlib import pyplot as plt
    from matplotlib.ticker import FuncFormatter

    figure, axis = plt.subplots(figsize=(6.8, 4.6))
    for tool in tools:
        selected = [row for row in size_rows if row["tool"] == tool]
        selected.sort(key=lambda row: int(row["interval_bp"]))
        values = [error_values(row, "output_nodes") for row in selected]
        axis.errorbar(
            [int(row["interval_bp"]) for row in selected],
            [item[0] for item in values],
            yerr=[
                [item[1][0] for item in values],
                [item[1][1] for item in values],
            ],
            color=styles[tool][0],
            marker=styles[tool][1],
            linewidth=1.8,
            capsize=3,
            label=tool_label(tool),
        )
    axis.set_xscale("log")
    axis.set_yscale("log")
    axis.xaxis.set_major_formatter(FuncFormatter(lambda value, _pos: format_bp(value)))
    axis.set_xlabel("Requested interval length")
    axis.set_ylabel("Output nodes")
    axis.set_title("Extracted graph size")
    axis.grid(alpha=0.25)
    axis.legend(frameon=False)
    save_figure(figure, output_dir / "interval_output_nodes", formats)
    plt.close(figure)


def main() -> int:
    """Build TSV summaries and publication-ready diagnostic plots."""
    args = parse_args()
    raw_path = Path(args.raw)
    output_root = Path(args.output_dir) if args.output_dir else raw_path.parent
    table_dir = output_root / "tables"
    plot_dir = output_root / "plots"
    formats = [item.strip().lstrip(".") for item in args.formats.split(",") if item.strip()]
    if not formats or any("/" in item or "\\" in item for item in formats):
        raise ValueError("--formats must contain simple comma-separated extensions")

    raw_rows = read_raw(raw_path)
    tools = ordered_tools(raw_rows)
    styles = build_tool_styles(tools)
    locations = build_location_rows(raw_rows)
    size_rows = build_size_rows(locations, args.bootstrap_samples, args.seed)
    repetition_rows, cache_rows = build_repetition_rows(
        raw_rows, args.bootstrap_samples, args.seed
    )

    write_table(table_dir / "location_summary.tsv", LOCATION_FIELDS, locations)
    write_table(table_dir / "size_summary.tsv", SIZE_FIELDS, size_rows)
    write_table(table_dir / "repetition_summary.tsv", REPETITION_FIELDS, repetition_rows)
    write_table(table_dir / "cache_summary.tsv", CACHE_FIELDS, cache_rows)

    configure_matplotlib()
    import matplotlib

    matplotlib.use("Agg")
    plot_dir.mkdir(parents=True, exist_ok=True)
    plot_scaling(size_rows, plot_dir, formats, "elapsed", tools, styles)
    plot_scaling(size_rows, plot_dir, formats, "rss", tools, styles)
    plot_location_variation(locations, size_rows, plot_dir, formats, tools, styles)
    plot_cache_effect(cache_rows, plot_dir, formats, tools, styles)
    plot_repetitions_by_size(repetition_rows, plot_dir, formats, tools, styles)
    plot_output_nodes(size_rows, plot_dir, formats, tools, styles)

    print(f"Wrote summary tables to {table_dir}")
    print(f"Wrote plots to {plot_dir}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError) as error:
        print(f"summarize_gfaidx_gbz_intervals.py: {error}", file=sys.stderr)
        raise SystemExit(1)

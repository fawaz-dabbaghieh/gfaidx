#!/usr/bin/env python3
"""Create benchmark plots from the final gfaidx benchmark TSV tables.

The script accepts both a complete Snakemake results directory, where tables
live under ``tables/``, and a directory containing copied TSV tables directly.
It uses only matplotlib plus the Python standard library so it can run without
pandas in a lightweight benchmark environment.
"""

from __future__ import annotations

import argparse
import csv
import math
import os
import re
import sys
import tempfile
from collections import defaultdict
from pathlib import Path

# Use a non-interactive backend and a writable cache. This keeps plotting
# reliable on headless compute nodes and in home directories mounted read-only.
os.environ.setdefault(
    "MPLCONFIGDIR", str(Path(tempfile.gettempdir()) / "gfaidx-matplotlib")
)
os.environ.setdefault(
    "XDG_CACHE_HOME", str(Path(tempfile.gettempdir()) / "gfaidx-xdg-cache")
)
Path(os.environ["MPLCONFIGDIR"]).mkdir(parents=True, exist_ok=True)
Path(os.environ["XDG_CACHE_HOME"]).mkdir(parents=True, exist_ok=True)

import matplotlib  # noqa: E402  (backend must be selected before pyplot)

matplotlib.use("Agg")
from matplotlib import pyplot as plt  # noqa: E402
from matplotlib.ticker import FuncFormatter  # noqa: E402


TOOL_ORDER = [
    "gfaidx_all_haplotypes",
    "gfaidx_matched_vg",
    "gfaidx_matched_odgi",
    "gfaidx_matched_gbz",
    "gfaidx",
    "vg",
    "odgi",
    "gbz",
]

TOOL_COLORS = {
    "gfaidx": "#0072B2",
    "vg": "#D55E00",
    "odgi": "#009E73",
    "gbz": "#CC79A7",
}

TOOL_MARKERS = {
    "gfaidx_all_haplotypes": "D",
    "gfaidx_matched_vg": "o",
    "gfaidx_matched_odgi": "s",
    "gfaidx_matched_gbz": "^",
    "vg": "o",
    "odgi": "s",
    "gbz": "^",
}

TOOL_LINESTYLES = {
    "gfaidx_matched_vg": "--",
    "gfaidx_matched_odgi": ":",
    "gfaidx_matched_gbz": "-.",
}

MATCHED_SOURCES = {
    "gfaidx_matched_vg": "vg",
    "gfaidx_matched_odgi": "odgi",
    "gfaidx_matched_gbz": "gbz",
}

PLOT_DESCRIPTIONS = {
    "indexing_totals": "Core indexing wall time and peak RSS as one consistently colored bar per tool.",
    "index_size_totals": "Core index footprint as one consistently colored bar per tool.",
    "indexing_summary_all_steps": "Supplementary index construction plot containing every recorded step.",
    "indexing_summary": "Primary index construction plot using only odgi build -O for ODGI.",
    "index_size_components": "All index artifacts recorded in index_sizes.tsv, split into components.",
    "interval_scaling": "Interval length versus mean extraction wall time and peak RSS.",
    "interval_output": "Interval length versus mean output nodes and serialized GFA bytes.",
    "interval_relative_to_gfaidx": "Mean source-tool cost ratio relative to exact all-haplotype gfaidx cost.",
    "node_steps_scaling": "Step context versus mean node-query wall time and peak RSS across seed nodes.",
    "node_bases_scaling": "Base-pair context versus mean node-query wall time and peak RSS across seed nodes.",
    "node_steps_speedup": "Mean per-seed source-tool cost ratio relative to node-count-matched gfaidx.",
    "node_bases_speedup": "Mean per-seed source-tool cost ratio relative to node-count-matched gfaidx.",
}


def parse_args() -> argparse.Namespace:
    """Parse command-line options."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--results-dir",
        required=True,
        help="directory containing TSV tables directly or in a tables/ subdirectory",
    )
    parser.add_argument(
        "--output-dir",
        default="",
        help="plot directory; defaults to <results-dir>/plots",
    )
    parser.add_argument(
        "--formats",
        default="png,pdf,svg",
        help="comma-separated output formats accepted by matplotlib (default: png,pdf,svg)",
    )
    parser.add_argument("--dpi", type=int, default=200, help="PNG resolution")
    return parser.parse_args()


def find_table(results: Path, name: str) -> Path:
    """Find one benchmark table in the normal or flat copied layout."""
    candidates = [results / "tables" / name, results / name]
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    raise FileNotFoundError(
        f"could not find {name} in {results} or {results / 'tables'}"
    )


def read_tsv(path: Path) -> list[dict[str, str]]:
    """Read a tab-separated benchmark table into string dictionaries."""
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle, delimiter="\t"))


def number(value: str | None) -> float | None:
    """Convert a populated numeric field, treating NA and blanks as missing."""
    text = str(value or "").strip()
    if not text or text.upper() == "NA":
        return None
    try:
        parsed = float(text)
    except ValueError:
        return None
    return parsed if math.isfinite(parsed) else None


def successful(row: dict[str, str]) -> bool:
    """Return whether a measured command completed successfully."""
    exit_code = number(row.get("exit_code"))
    return exit_code is None or exit_code == 0


def safe_name(value: str) -> str:
    """Make graph and query identifiers safe for generated filenames."""
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip("_") or "unnamed"


def base_tool(tool: str) -> str:
    """Return the color-family tool represented by a result variant."""
    if tool.startswith("gfaidx"):
        return "gfaidx"
    return tool


def tool_label(tool: str) -> str:
    """Return a concise, unambiguous legend label."""
    labels = {
        "gfaidx_all_haplotypes": "gfaidx (all haplotypes)",
        "gfaidx_matched_vg": "gfaidx (matched to VG)",
        "gfaidx_matched_odgi": "gfaidx (matched to ODGI)",
        "gfaidx_matched_gbz": "gfaidx (matched to gbz-base)",
        "vg": "VG",
        "odgi": "ODGI",
        "gbz": "gbz-base",
        "gfaidx": "gfaidx",
    }
    return labels.get(tool, tool)


def tool_sort_key(tool: str) -> tuple[int, str]:
    """Keep gfaidx variants and source tools in a stable order."""
    try:
        return TOOL_ORDER.index(tool), tool
    except ValueError:
        return len(TOOL_ORDER), tool


def line_style(tool: str) -> dict[str, object]:
    """Return consistent colors and markers for a tool series."""
    return {
        "color": TOOL_COLORS.get(base_tool(tool), "#666666"),
        "marker": TOOL_MARKERS.get(tool, "o"),
        "linestyle": TOOL_LINESTYLES.get(tool, "-"),
        "linewidth": 1.8,
        "markersize": 5,
    }


def human_number(value: float, _position: float | None = None) -> str:
    """Format coordinates and counts with SI suffixes."""
    absolute = abs(value)
    for suffix, scale in (("G", 1e9), ("M", 1e6), ("k", 1e3)):
        if absolute >= scale:
            return f"{value / scale:g}{suffix}"
    return f"{value:g}"


def configure_axis(
    axis,
    xlabel: str,
    ylabel: str,
    *,
    log_x: bool = False,
    log_y: bool = False,
) -> None:
    """Apply shared axis labels, scales, and grid styling."""
    axis.set_xlabel(xlabel)
    axis.set_ylabel(ylabel)
    if log_x:
        axis.set_xscale("log")
    if log_y:
        axis.set_yscale("log")
    axis.grid(True, which="both", color="#d9d9d9", linewidth=0.6, alpha=0.8)
    axis.set_axisbelow(True)


def label_tested_x_values(axis, values: list[float] | set[float]) -> None:
    """Label every tested x value, including non-power-of-ten log positions."""
    ticks = sorted({value for value in values if value > 0})
    if not ticks:
        return
    axis.set_xticks(ticks)
    axis.xaxis.set_major_formatter(FuncFormatter(human_number))
    if len(ticks) > 6:
        axis.tick_params(axis="x", labelrotation=30)
        for label in axis.get_xticklabels():
            label.set_horizontalalignment("right")


def save_figure(
    figure,
    output_dir: Path,
    stem: str,
    formats: list[str],
    dpi: int,
    generated: list[tuple[str, str, str]],
) -> None:
    """Write one figure in every requested format and record its manifest row."""
    figure.tight_layout()
    files: list[str] = []
    for extension in formats:
        path = output_dir / f"{stem}.{extension}"
        figure.savefig(path, dpi=dpi, bbox_inches="tight")
        files.append(path.name)
    plt.close(figure)
    description_key = next(
        (key for key in PLOT_DESCRIPTIONS if stem.startswith(key)), stem
    )
    generated.append(
        (stem, ",".join(files), PLOT_DESCRIPTIONS.get(description_key, ""))
    )


def remove_previous_outputs(output_dir: Path) -> None:
    """Remove only files listed by the previous plot manifest."""
    manifest = output_dir / "plots.tsv"
    if not manifest.is_file():
        return
    with manifest.open("r", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle, delimiter="\t"):
            for filename in row.get("files", "").split(","):
                filename = filename.strip()
                if filename and Path(filename).name == filename:
                    (output_dir / filename).unlink(missing_ok=True)


def parse_interval(context: str) -> tuple[str, int, int, int] | None:
    """Parse ``sequence:start-end`` into sequence, bounds, and length."""
    sequence, separator, coordinates = context.rpartition(":")
    start_text, dash, end_text = coordinates.partition("-")
    if not separator or not dash:
        return None
    try:
        start, end = int(start_text), int(end_text)
    except ValueError:
        return None
    if end <= start:
        return None
    return sequence, start, end, end - start


def index_metric_rows(
    rows: list[dict[str, str]], graph: str, *, all_odgi_steps: bool
) -> list[dict[str, str]]:
    """Select successful index metrics for a primary or all-steps view."""
    return [
        row
        for row in rows
        if row.get("graph") == graph
        and successful(row)
        and number(row.get("wall_seconds")) is not None
        and (
            all_odgi_steps
            or row.get("tool") != "odgi"
            or row.get("step") == "build_optimized"
        )
    ]


def plot_indexing_summary(
    rows: list[dict[str, str]],
    graph: str,
    output_dir: Path,
    formats: list[str],
    dpi: int,
    generated: list[tuple[str, str, str]],
    *,
    all_odgi_steps: bool,
) -> None:
    """Plot indexing time by step and maximum process RSS.

    The primary comparison treats ``odgi build -O`` as ODGI's functional graph
    construction operation. A separate supplementary invocation retains every
    measured ODGI preparation and side-index step.
    """
    selected = index_metric_rows(rows, graph, all_odgi_steps=all_odgi_steps)
    if not selected:
        return

    tools = sorted({row["tool"] for row in selected}, key=tool_sort_key)
    steps = list(dict.fromkeys(row["step"] for row in selected))
    step_colors = plt.get_cmap("tab20").colors
    figure, axes = plt.subplots(1, 2, figsize=(13, 5.4))

    bottoms = [0.0] * len(tools)
    for step_index, step in enumerate(steps):
        values = []
        for tool in tools:
            seconds = sum(
                number(row.get("wall_seconds")) or 0.0
                for row in selected
                if row["tool"] == tool and row["step"] == step
            )
            values.append(seconds / 3600.0)
        if any(values):
            axes[0].bar(
                range(len(tools)),
                values,
                bottom=bottoms,
                label=step,
                color=step_colors[step_index % len(step_colors)],
                edgecolor="white",
                linewidth=0.4,
            )
            bottoms = [bottom + value for bottom, value in zip(bottoms, values)]

    axes[0].set_xticks(range(len(tools)), [tool_label(tool) for tool in tools])
    axes[0].tick_params(axis="x", rotation=20)
    configure_axis(axes[0], "Tool", "Total wall time (hours)")
    axes[0].legend(title="Indexing step", fontsize=8, title_fontsize=8)
    for index, total in enumerate(bottoms):
        axes[0].annotate(
            f"{total:.2f} h",
            (index, total),
            xytext=(0, 4),
            textcoords="offset points",
            ha="center",
            fontsize=8,
        )

    rss_values = []
    for tool in tools:
        rss_values.append(
            max(
                (number(row.get("peak_rss_kb")) or 0.0) / (1024.0**2)
                for row in selected
                if row["tool"] == tool
            )
        )
    axes[1].bar(
        range(len(tools)),
        rss_values,
        color=[TOOL_COLORS.get(base_tool(tool), "#666666") for tool in tools],
    )
    axes[1].set_xticks(range(len(tools)), [tool_label(tool) for tool in tools])
    axes[1].tick_params(axis="x", rotation=20)
    configure_axis(axes[1], "Tool", "Maximum peak RSS (GiB)")
    for index, value in enumerate(rss_values):
        axes[1].annotate(
            f"{value:.1f}",
            (index, value),
            xytext=(0, 4),
            textcoords="offset points",
            ha="center",
            fontsize=8,
        )

    if all_odgi_steps:
        title = f"All recorded index construction steps: {graph}"
        stem = f"indexing_summary_all_steps__{safe_name(graph)}"
    else:
        title = (
            f"Core index construction: {graph}\n"
            "ODGI shows build -O only; this compacts and remaps node IDs"
        )
        stem = f"indexing_summary__{safe_name(graph)}"
    figure.suptitle(title, fontsize=14)
    save_figure(
        figure,
        output_dir,
        stem,
        formats,
        dpi,
        generated,
    )


def plot_indexing_totals(
    rows: list[dict[str, str]],
    graph: str,
    output_dir: Path,
    formats: list[str],
    dpi: int,
    generated: list[tuple[str, str, str]],
) -> None:
    """Plot one core indexing-time and memory bar per tool."""
    selected = index_metric_rows(rows, graph, all_odgi_steps=False)
    if not selected:
        return
    tools = sorted({row["tool"] for row in selected}, key=tool_sort_key)
    times = [
        sum(
            number(row.get("wall_seconds")) or 0.0
            for row in selected
            if row["tool"] == tool
        )
        / 3600.0
        for tool in tools
    ]
    memory = [
        max(
            (number(row.get("peak_rss_kb")) or 0.0) / (1024.0**2)
            for row in selected
            if row["tool"] == tool
        )
        for tool in tools
    ]
    colors = [TOOL_COLORS.get(base_tool(tool), "#666666") for tool in tools]
    labels = [tool_label(tool) for tool in tools]
    figure, axes = plt.subplots(1, 2, figsize=(12.5, 5.2))

    for axis, values, ylabel, suffix, precision in (
        (axes[0], times, "Total wall time (hours)", " h", 2),
        (axes[1], memory, "Maximum peak RSS (GiB)", " GiB", 1),
    ):
        axis.bar(range(len(tools)), values, color=colors)
        axis.set_xticks(range(len(tools)), labels)
        axis.tick_params(axis="x", rotation=20)
        configure_axis(axis, "Tool", ylabel)
        axis.set_ylim(0, max(values) * 1.16)
        for index, value in enumerate(values):
            axis.annotate(
                f"{value:.{precision}f}{suffix}",
                (index, value),
                xytext=(0, 4),
                textcoords="offset points",
                ha="center",
                fontsize=8,
            )

    figure.suptitle(
        f"Core indexing totals: {graph}\n"
        "gfaidx includes graph and coordinate indexing; ODGI includes build -O only",
        fontsize=14,
    )
    save_figure(
        figure,
        output_dir,
        f"indexing_totals__{safe_name(graph)}",
        formats,
        dpi,
        generated,
    )


def index_component(tool: str, filename: str) -> str:
    """Collapse filenames into meaningful index component labels."""
    if filename.endswith(".gfa.gz"):
        return "indexed GFA"
    if filename.endswith(".gbz.db"):
        return "GBZ database"
    suffix_labels = {
        ".cdx": ".cdx",
        ".idx": ".idx",
        ".lnx": ".lnx",
        ".ndx": ".ndx",
        ".pcx": ".pcx",
        ".pdx": ".pdx",
        ".xg": ".xg",
        ".stpidx": ".stpidx",
        ".xp": ".xp",
        ".og": ".og",
        ".gbz": ".gbz",
    }
    for suffix, label in suffix_labels.items():
        if filename.endswith(suffix):
            return label
    return f"{tool}: other"


def plot_index_sizes(
    rows: list[dict[str, str]],
    graph: str,
    output_dir: Path,
    formats: list[str],
    dpi: int,
    generated: list[tuple[str, str, str]],
) -> None:
    """Plot total index size as stacked, named components."""
    selected = [
        row
        for row in rows
        if row.get("graph") == graph
        and row.get("file") != "TOTAL"
        and number(row.get("bytes")) is not None
    ]
    if not selected:
        return

    tools = sorted({row["tool"] for row in selected}, key=tool_sort_key)
    components = list(
        dict.fromkeys(index_component(row["tool"], row["file"]) for row in selected)
    )
    colors = plt.get_cmap("tab20").colors
    figure, axis = plt.subplots(figsize=(10.5, 5.8))
    bottoms = [0.0] * len(tools)
    for component_index, component in enumerate(components):
        values = []
        for tool in tools:
            values.append(
                sum(
                    (number(row.get("bytes")) or 0.0) / (1024.0**3)
                    for row in selected
                    if row["tool"] == tool
                    and index_component(row["tool"], row["file"]) == component
                )
            )
        if any(values):
            axis.bar(
                range(len(tools)),
                values,
                bottom=bottoms,
                label=component,
                color=colors[component_index % len(colors)],
                edgecolor="white",
                linewidth=0.4,
            )
            bottoms = [bottom + value for bottom, value in zip(bottoms, values)]

    axis.set_xticks(range(len(tools)), [tool_label(tool) for tool in tools])
    configure_axis(axis, "Tool", "Total index size (GiB)")
    # Leave room for total-size annotations above the tallest stacked bar.
    axis.set_ylim(0, max(bottoms) * 1.15)
    axis.legend(title="Index component", fontsize=8, ncol=2)
    for index, total in enumerate(bottoms):
        axis.annotate(
            f"{total:.1f} GiB",
            (index, total),
            xytext=(0, 4),
            textcoords="offset points",
            ha="center",
            fontsize=8,
        )
    axis.set_title(f"Recorded index footprint: {graph}")
    save_figure(
        figure,
        output_dir,
        f"index_size_components__{safe_name(graph)}",
        formats,
        dpi,
        generated,
    )


def plot_index_size_totals(
    rows: list[dict[str, str]],
    graph: str,
    output_dir: Path,
    formats: list[str],
    dpi: int,
    generated: list[tuple[str, str, str]],
) -> None:
    """Plot one core index-size bar per tool.

    gfaidx, VG, and gbz-base use their recorded TOTAL rows. ODGI uses only the
    optimized ``.opt.og`` graph so the primary size figure matches the primary
    ``odgi build -O`` time and memory comparison.
    """
    selected = [row for row in rows if row.get("graph") == graph]
    if not selected:
        return
    tools = sorted({row["tool"] for row in selected}, key=tool_sort_key)
    sizes: list[float] = []
    plotted_tools: list[str] = []
    for tool in tools:
        tool_rows = [row for row in selected if row["tool"] == tool]
        if tool == "odgi":
            byte_count = sum(
                number(row.get("bytes")) or 0.0
                for row in tool_rows
                if row.get("file", "").endswith(".opt.og")
            )
        else:
            total = next(
                (
                    number(row.get("bytes"))
                    for row in tool_rows
                    if row.get("file") == "TOTAL"
                ),
                None,
            )
            byte_count = total if total is not None else sum(
                number(row.get("bytes")) or 0.0
                for row in tool_rows
                if row.get("file") != "TOTAL"
            )
        if byte_count > 0:
            plotted_tools.append(tool)
            sizes.append(byte_count / (1024.0**3))
    if not sizes:
        return

    colors = [
        TOOL_COLORS.get(base_tool(tool), "#666666") for tool in plotted_tools
    ]
    figure, axis = plt.subplots(figsize=(8.8, 5.5))
    axis.bar(range(len(plotted_tools)), sizes, color=colors)
    axis.set_xticks(
        range(len(plotted_tools)), [tool_label(tool) for tool in plotted_tools]
    )
    configure_axis(axis, "Tool", "Core index size (GiB)")
    axis.set_ylim(0, max(sizes) * 1.16)
    for index, value in enumerate(sizes):
        axis.annotate(
            f"{value:.1f} GiB",
            (index, value),
            xytext=(0, 4),
            textcoords="offset points",
            ha="center",
            fontsize=8,
        )
    axis.set_title(
        f"Core index footprint: {graph}\n"
        "gfaidx sums all sidecars; ODGI includes the optimized .og only"
    )
    save_figure(
        figure,
        output_dir,
        f"index_size_totals__{safe_name(graph)}",
        formats,
        dpi,
        generated,
    )


def interval_rows(
    rows: list[dict[str, str]], graph: str
) -> list[tuple[dict[str, str], str, int, int]]:
    """Return the primary region slice annotated with interval coordinates.

    The sweep data remain in the TSV. Existing overview plots deliberately use
    the smallest numeric thread count, gfaidx no-gap, ODGI default behavior,
    standard VG extraction, and the context-zero gbz-base query.
    """
    candidates = [
        row for row in rows
        if row.get("graph") == graph
        and row.get("track") == "region"
        and successful(row)
    ]
    if any(row.get("query_variant", "") not in {"", "legacy"} for row in candidates):
        candidates = [row for row in candidates if row.get("query_variant") != "legacy"]

    numeric_threads = [
        int(value) for row in candidates
        if (value := number(row.get("threads"))) is not None
    ]
    primary_thread = min(numeric_threads) if numeric_threads else None

    gfaidx_has_no_gap = any(
        row.get("tool") == "gfaidx_all_haplotypes"
        and row.get("query_variant") == "no_gap"
        for row in candidates
    )
    odgi_has_default = any(
        row.get("tool") == "odgi" and row.get("query_variant") == "default"
        for row in candidates
    )
    minimum_gap = {
        tool: min(
            int(value)
            for row in candidates
            if row.get("tool") == tool
            and (value := number(row.get("haplotype_gap_bp"))) is not None
        )
        for tool in ("gfaidx_all_haplotypes", "odgi")
        if any(
            row.get("tool") == tool
            and number(row.get("haplotype_gap_bp")) is not None
            for row in candidates
        )
    }
    minimum_odgi_iterations = min(
        (
            int(value)
            for row in candidates
            if row.get("tool") == "odgi"
            and (value := number(row.get("merging_iterations"))) is not None
        ),
        default=None,
    )

    parsed_rows = []
    for row in candidates:
        thread = number(row.get("threads"))
        if thread is not None and primary_thread is not None and int(thread) != primary_thread:
            continue
        tool = row.get("tool")
        variant = row.get("query_variant", "legacy")
        if tool == "gfaidx_all_haplotypes":
            if gfaidx_has_no_gap and variant != "no_gap":
                continue
            if not gfaidx_has_no_gap and number(row.get("haplotype_gap_bp")) != minimum_gap.get(tool):
                continue
        elif tool == "odgi":
            if odgi_has_default and variant != "default":
                continue
            if not odgi_has_default:
                if number(row.get("haplotype_gap_bp")) != minimum_gap.get(tool):
                    continue
                if number(row.get("merging_iterations")) != minimum_odgi_iterations:
                    continue
        if row.get("graph") != graph or row.get("track") != "region":
            continue
        parsed = parse_interval(row.get("context", ""))
        if parsed is None:
            continue
        sequence, start, _end, length = parsed
        parsed_rows.append((row, sequence, start, length))
    return parsed_rows


def primary_node_rows(
    rows: list[dict[str, str]], graph: str, track: str
) -> list[dict[str, str]]:
    """Select the smallest-thread node slice for legacy overview plots."""
    selected = [
        row for row in rows
        if row.get("graph") == graph
        and row.get("track") == track
        and successful(row)
        and number(row.get("context")) is not None
    ]
    if any(row.get("query_variant", "") not in {"", "legacy"} for row in selected):
        selected = [row for row in selected if row.get("query_variant") != "legacy"]
    numeric_threads = [
        int(value) for row in selected
        if (value := number(row.get("threads"))) is not None
    ]
    if not numeric_threads:
        return selected
    primary_thread = min(numeric_threads)
    return [
        row for row in selected
        if number(row.get("threads")) is None
        or int(number(row.get("threads")) or 0) == primary_thread
    ]


def mean_by_x_value(
    values: list[tuple[int, float]],
) -> list[tuple[int, float]]:
    """Average repeated measurements into one point per x-axis value.

    The result tables retain every individual locus. Aggregation happens only
    in the plotting layer so repeated regions and seed nodes remain available
    for later statistical analysis.
    """
    grouped: dict[int, list[float]] = defaultdict(list)
    for length, value in values:
        grouped[length].append(value)
    return [
        (length, sum(measurements) / len(measurements))
        for length, measurements in sorted(grouped.items())
    ]


def interval_plot_note(
    selected: list[tuple[dict[str, str], str, int, int]],
) -> str:
    """Describe cross-locus connections and repeated-length averaging."""
    starts = {(sequence, start) for _row, sequence, start, _length in selected}
    queries_by_length: dict[int, set[str]] = defaultdict(set)
    for row, _sequence, _start, length in selected:
        queries_by_length[length].add(row["query_id"])

    notes: list[str] = []
    if len(starts) > 1:
        notes.append("points span multiple genomic starts")
    if any(len(queries) > 1 for queries in queries_by_length.values()):
        notes.append("equal lengths show arithmetic means")
    return f"\n{'; '.join(notes)}" if notes else ""


def plot_interval_scaling(
    rows: list[dict[str, str]],
    graph: str,
    output_dir: Path,
    formats: list[str],
    dpi: int,
    generated: list[tuple[str, str, str]],
) -> None:
    """Plot extraction time and memory against interval length."""
    selected = interval_rows(rows, graph)
    if not selected:
        return
    lengths = {length for _row, _sequence, _start, length in selected}
    groups: dict[str, list[tuple[int, dict[str, str]]]] = defaultdict(list)
    for row, _sequence, _start, length in selected:
        groups[row["tool"]].append((length, row))

    figure, axes = plt.subplots(1, 2, figsize=(13, 5.3))
    metrics = [
        ("wall_seconds", "Wall time (seconds)", 1.0),
        ("peak_rss_kb", "Peak RSS (GiB)", 1.0 / (1024.0**2)),
    ]
    for axis, (field, ylabel, scale) in zip(axes, metrics):
        for tool, entries in sorted(groups.items(), key=lambda item: tool_sort_key(item[0])):
            # Collapse replicate loci of the same requested length before
            # drawing, leaving one mean point per tool and interval size.
            points = mean_by_x_value([
                (length, (number(row.get(field)) or 0.0) * scale)
                for length, row in entries
                if (number(row.get(field)) or 0.0) > 0
            ])
            if not points:
                continue
            axis.plot(
                [point[0] for point in points],
                [point[1] for point in points],
                label=tool_label(tool),
                **line_style(tool),
            )
        configure_axis(
            axis, "Requested interval length (bp)", ylabel, log_x=True, log_y=True
        )
        label_tested_x_values(axis, lengths)
    axes[1].legend(fontsize=7, ncol=2)
    figure.suptitle(
        f"Coordinate-interval extraction scaling: {graph}{interval_plot_note(selected)}",
        fontsize=14,
    )
    save_figure(
        figure,
        output_dir,
        f"interval_scaling__{safe_name(graph)}",
        formats,
        dpi,
        generated,
    )


def plot_interval_output(
    rows: list[dict[str, str]],
    graph: str,
    output_dir: Path,
    formats: list[str],
    dpi: int,
    generated: list[tuple[str, str, str]],
) -> None:
    """Plot how much graph each interval query materializes."""
    selected = interval_rows(rows, graph)
    if not selected:
        return
    lengths = {length for _row, _sequence, _start, length in selected}
    groups: dict[str, list[tuple[int, dict[str, str]]]] = defaultdict(list)
    for row, _sequence, _start, length in selected:
        groups[row["tool"]].append((length, row))

    figure, axes = plt.subplots(1, 2, figsize=(13, 5.3))
    for axis, field, ylabel, scale in (
        (axes[0], "out_nodes", "Output nodes", 1.0),
        (axes[1], "out_bytes", "Output GFA size (MiB)", 1.0 / (1024.0**2)),
    ):
        for tool, entries in sorted(groups.items(), key=lambda item: tool_sort_key(item[0])):
            # Output scale is averaged in the same way as time and memory so
            # each requested length has a single visual point per tool.
            points = mean_by_x_value([
                (length, (number(row.get(field)) or 0.0) * scale)
                for length, row in entries
                if (number(row.get(field)) or 0.0) > 0
            ])
            if points:
                axis.plot(
                    [point[0] for point in points],
                    [point[1] for point in points],
                    label=tool_label(tool),
                    **line_style(tool),
                )
        configure_axis(
            axis, "Requested interval length (bp)", ylabel, log_x=True, log_y=True
        )
        label_tested_x_values(axis, lengths)
    axes[1].legend(fontsize=7, ncol=2)
    figure.suptitle(
        f"Coordinate-interval output scale: {graph}{interval_plot_note(selected)}",
        fontsize=14,
    )
    save_figure(
        figure,
        output_dir,
        f"interval_output__{safe_name(graph)}",
        formats,
        dpi,
        generated,
    )


def plot_interval_relative(
    rows: list[dict[str, str]],
    graph: str,
    output_dir: Path,
    formats: list[str],
    dpi: int,
    generated: list[tuple[str, str, str]],
) -> None:
    """Plot time and memory ratios relative to exact gfaidx extraction."""
    selected = interval_rows(rows, graph)
    by_query_tool = {(row["query_id"], row["tool"]): row for row, *_rest in selected}
    interval_by_query = {
        row["query_id"]: (sequence, start, length)
        for row, sequence, start, length in selected
    }
    lengths = {length for _sequence, _start, length in interval_by_query.values()}
    groups: dict[str, list[tuple[int, float, float]]] = defaultdict(list)
    for query, (_sequence, _start, length) in interval_by_query.items():
        baseline = by_query_tool.get((query, "gfaidx_all_haplotypes"))
        if baseline is None:
            continue
        base_time = number(baseline.get("wall_seconds"))
        base_rss = number(baseline.get("peak_rss_kb"))
        if not base_time or not base_rss:
            continue
        for tool in ("vg", "odgi", "gbz"):
            source = by_query_tool.get((query, tool))
            if source is None:
                continue
            source_time = number(source.get("wall_seconds"))
            source_rss = number(source.get("peak_rss_kb"))
            if source_time and source_rss:
                groups[tool].append(
                    (length, source_time / base_time, source_rss / base_rss)
                )

    if not groups:
        return
    figure, axes = plt.subplots(1, 2, figsize=(13, 5.3))
    for axis, value_index, ylabel in (
        (axes[0], 1, "Time ratio (source / gfaidx)"),
        (axes[1], 2, "Peak RSS ratio (source / gfaidx)"),
    ):
        axis.axhline(1.0, color="#555555", linestyle="--", linewidth=1)
        for tool, points in sorted(groups.items(), key=lambda item: tool_sort_key(item[0])):
            # Preserve the previous per-query ratio definition, then average
            # those ratios for replicate intervals of the same length.
            averaged = mean_by_x_value([
                (point[0], point[value_index]) for point in points
            ])
            axis.plot(
                [point[0] for point in averaged],
                [point[1] for point in averaged],
                label=tool_label(tool),
                **line_style(tool),
            )
        configure_axis(
            axis, "Requested interval length (bp)", ylabel, log_x=True, log_y=True
        )
        label_tested_x_values(axis, lengths)
    axes[1].legend(fontsize=7, ncol=2)
    figure.suptitle(
        f"Coordinate extraction relative to gfaidx: {graph}{interval_plot_note(selected)}",
        fontsize=14,
    )
    save_figure(
        figure,
        output_dir,
        f"interval_relative_to_gfaidx__{safe_name(graph)}",
        formats,
        dpi,
        generated,
    )


def plot_node_scaling(
    rows: list[dict[str, str]],
    graph: str,
    track: str,
    output_dir: Path,
    formats: list[str],
    dpi: int,
    generated: list[tuple[str, str, str]],
) -> None:
    """Plot mean time and memory across seed nodes for one context series."""
    selected = primary_node_rows(rows, graph, track)
    if not selected:
        return
    groups: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in selected:
        groups[row["tool"]].append(row)

    figure, axes = plt.subplots(1, 2, figsize=(12.5, 5.2))
    metrics = [
        ("wall_seconds", "Wall time (seconds)", 1.0),
        ("peak_rss_kb", "Peak RSS (GiB)", 1.0 / (1024.0**2)),
    ]
    contexts = {
        number(row.get("context")) or 0.0
        for row in selected
        if (number(row.get("context")) or 0.0) > 0
    }
    for axis, (field, ylabel, scale) in zip(axes, metrics):
        for tool in sorted(groups, key=tool_sort_key):
            # Every query_id represents one seed node. Average all successful
            # seeds at the same configured context into one point per tool.
            points = mean_by_x_value([
                (
                    int(number(row.get("context")) or 0.0),
                    (number(row.get(field)) or 0.0) * scale,
                )
                for row in groups[tool]
                if (number(row.get("context")) or 0.0) > 0
                and (number(row.get(field)) or 0.0) > 0
            ])
            if points:
                axis.plot(
                    [point[0] for point in points],
                    [point[1] for point in points],
                    label=tool_label(tool),
                    **line_style(tool),
                )
        x_label = "Context steps" if track == "node_steps" else "Context bases"
        configure_axis(axis, x_label, ylabel, log_x=True, log_y=True)
        label_tested_x_values(axis, contexts)
    axes[1].legend(fontsize=7)
    track_label = "step" if track == "node_steps" else "base-pair"
    seed_count = len({row["query_id"] for row in selected})
    seed_note = (
        f"\nArithmetic mean across {seed_count} seed nodes"
        if seed_count > 1
        else ""
    )
    figure.suptitle(
        f"Node extraction by {track_label} context: {graph}{seed_note}", fontsize=14
    )
    save_figure(
        figure,
        output_dir,
        f"{track}_scaling__{safe_name(graph)}",
        formats,
        dpi,
        generated,
    )


def plot_node_speedup(
    rows: list[dict[str, str]],
    graph: str,
    track: str,
    output_dir: Path,
    formats: list[str],
    dpi: int,
    generated: list[tuple[str, str, str]],
) -> None:
    """Plot mean per-seed source/matched-gfaidx ratios by context."""
    selected = primary_node_rows(rows, graph, track)
    by_query_tool_context = {
        (row["query_id"], row["tool"], row["context"]): row for row in selected
    }
    ratios: dict[str, list[tuple[float, float, float]]] = defaultdict(list)
    for matched, source in MATCHED_SOURCES.items():
        # Compute the ratio within each seed first. Averaging these ratios by
        # context gives every seed equal weight and avoids a ratio-of-means bias.
        source_rows = [row for row in selected if row["tool"] == source]
        for source_row in source_rows:
            query = source_row["query_id"]
            context = source_row["context"]
            matched_row = by_query_tool_context.get((query, matched, context))
            if matched_row is None:
                continue
            source_time = number(source_row.get("wall_seconds"))
            matched_time = number(matched_row.get("wall_seconds"))
            source_rss = number(source_row.get("peak_rss_kb"))
            matched_rss = number(matched_row.get("peak_rss_kb"))
            if source_time and matched_time and source_rss and matched_rss:
                ratios[source].append(
                    (
                        float(context),
                        source_time / matched_time,
                        source_rss / matched_rss,
                    )
                )
    if not ratios:
        return

    figure, axes = plt.subplots(1, 2, figsize=(12.5, 5.2))
    contexts = {point[0] for points in ratios.values() for point in points}
    for axis, value_index, ylabel in (
        (axes[0], 1, "Time ratio (source / matched gfaidx)"),
        (axes[1], 2, "Peak RSS ratio (source / matched gfaidx)"),
    ):
        axis.axhline(1.0, color="#555555", linestyle="--", linewidth=1)
        for source in sorted(ratios, key=tool_sort_key):
            points = mean_by_x_value([
                (int(point[0]), point[value_index]) for point in ratios[source]
            ])
            axis.plot(
                [point[0] for point in points],
                [point[1] for point in points],
                label=f"{tool_label(source)} / gfaidx",
                **line_style(source),
            )
        x_label = "Context steps" if track == "node_steps" else "Context bases"
        configure_axis(axis, x_label, ylabel, log_x=True, log_y=True)
        label_tested_x_values(axis, contexts)
    axes[1].legend(fontsize=8)
    seed_count = len({row["query_id"] for row in selected})
    seed_note = (
        f"\nArithmetic mean across {seed_count} seed nodes"
        if seed_count > 1
        else ""
    )
    figure.suptitle(
        f"Matched node-query cost ratios: {graph}{seed_note}", fontsize=14
    )
    save_figure(
        figure,
        output_dir,
        f"{track}_speedup__{safe_name(graph)}",
        formats,
        dpi,
        generated,
    )


def write_manifest(
    output_dir: Path, generated: list[tuple[str, str, str]]
) -> None:
    """Write a short index describing every generated figure."""
    path = output_dir / "plots.tsv"
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter="\t", lineterminator="\n")
        writer.writerow(["plot", "files", "description"])
        writer.writerows(generated)


def main() -> int:
    """Load benchmark tables and generate every applicable figure."""
    args = parse_args()
    results = Path(args.results_dir).resolve()
    output_dir = (
        Path(args.output_dir).resolve()
        if args.output_dir
        else results / "plots"
    )
    formats = [item.strip().lstrip(".") for item in args.formats.split(",") if item.strip()]
    if not formats:
        raise ValueError("--formats must contain at least one output format")
    if args.dpi < 1:
        raise ValueError("--dpi must be positive")
    output_dir.mkdir(parents=True, exist_ok=True)
    remove_previous_outputs(output_dir)

    index_rows = read_tsv(find_table(results, "index_metrics.tsv"))
    size_rows = read_tsv(find_table(results, "index_sizes.tsv"))
    query_rows = read_tsv(find_table(results, "query_metrics.tsv"))
    graphs = sorted(
        {
            row.get("graph", "")
            for row in index_rows + size_rows + query_rows
            if row.get("graph", "")
        }
    )
    if not graphs:
        raise ValueError("the benchmark tables contain no graph rows")

    generated: list[tuple[str, str, str]] = []
    for graph in graphs:
        plot_indexing_totals(
            index_rows, graph, output_dir, formats, args.dpi, generated
        )
        plot_indexing_summary(
            index_rows,
            graph,
            output_dir,
            formats,
            args.dpi,
            generated,
            all_odgi_steps=False,
        )
        plot_indexing_summary(
            index_rows,
            graph,
            output_dir,
            formats,
            args.dpi,
            generated,
            all_odgi_steps=True,
        )
        plot_index_size_totals(
            size_rows, graph, output_dir, formats, args.dpi, generated
        )
        plot_index_sizes(size_rows, graph, output_dir, formats, args.dpi, generated)
        plot_interval_scaling(
            query_rows, graph, output_dir, formats, args.dpi, generated
        )
        plot_interval_output(
            query_rows, graph, output_dir, formats, args.dpi, generated
        )
        plot_interval_relative(
            query_rows, graph, output_dir, formats, args.dpi, generated
        )

        node_tracks = sorted(
            {
                row["track"]
                for row in query_rows
                if row.get("graph") == graph
                and row.get("track") in {"node_steps", "node_bases"}
            }
        )
        for track in node_tracks:
            plot_node_scaling(
                query_rows,
                graph,
                track,
                output_dir,
                formats,
                args.dpi,
                generated,
            )
            plot_node_speedup(
                query_rows,
                graph,
                track,
                output_dir,
                formats,
                args.dpi,
                generated,
            )
    write_manifest(output_dir, generated)
    print(f"Generated {len(generated)} plots in {output_dir}")
    for _stem, files, _description in generated:
        print(f"  {files}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError) as error:
        print(f"plot_results.py: {error}", file=sys.stderr)
        raise SystemExit(1)

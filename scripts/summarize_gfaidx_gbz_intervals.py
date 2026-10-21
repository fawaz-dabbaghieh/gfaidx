#!/usr/bin/env python3
"""Summarize and plot repeated gfaidx versus gbz-base interval benchmarks.

Technical repetitions and genomic locations are handled separately. For each
query, context, and variant, run 1 is the first execution and the median of
runs 2..N is the warm execution estimate. Means and bootstrap 95% confidence
intervals are then calculated across query locations of the same interval
length.

Every execution belongs to one comparison:

  reference_only   gbz-base --reference-only versus gfaidx --mode reference
                   --with_coords; both write the same subgraph and one
                   reference walk with coordinates
  all_haplotypes   gbz-base (anonymous `unknown` haplotypes) versus gfaidx
                   --mode bfs without --with_coords (matched work) and with
                   --with_coords (named, coordinate-bearing subwalks), plus
                   the optional gbz-base-query --resolve with a haplotype
                   index (named, coordinate-bearing subwalks)
  haplotype_span   optional gfaidx --mode all --with_coords; no gbz-base pair

Node-set and reference-walk hashes recorded by the runner are compared per
query, so the tables show whether paired tools extracted the same subgraph.

A location (query, context, variant) with any failed execution is reported as
NA: it is listed in failures_by_query.tsv and left out of every average. Size
summaries count those locations in `failed_locations`, and plots omit a
variant at any size where not every location succeeded, so a mean is never
drawn over the surviving, easier locations only.
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


# gfaidx variants are named `gfaidx-<kind>-<threads>t`. Raw tables from the
# earlier runner used `gfaidx-<threads>t` (all-haplotype mode) and `gbz-base`;
# both are still accepted as a `legacy` comparison.
GFAIDX_VARIANT_PATTERN = re.compile(
    r"^gfaidx-(?:(ref-coords|bfs|bfs-coords|span-coords)-)?([1-9][0-9]*)t$"
)
GFAIDX_KIND_LABELS = {
    "ref-coords": "reference + coords",
    "bfs": "bfs, no coords",
    "bfs-coords": "bfs + coords",
    "span-coords": "--mode all + coords",
    None: "--all_haplotypes",
}
GFAIDX_KIND_ORDER = ("ref-coords", "bfs", "bfs-coords", "span-coords", None)
# Variants without coordinate output are drawn dashed with open markers so the
# cost of --with_coords stays visible next to the matched-work gfaidx variant.
GFAIDX_KINDS_WITHOUT_COORDS = {"bfs"}
GBZ_BASE_LABELS = {
    "gbz-base-ref": "gbz-base --reference-only",
    "gbz-base-all": "gbz-base (unknown haplotypes)",
    "gbz-base-query-resolved": "gbz-base-query + haplotype index (resolved)",
    "gbz-base": "gbz-base",
}

COMPARISON_ORDER = ("reference_only", "all_haplotypes", "haplotype_span", "legacy")
COMPARISON_TITLES = {
    "reference_only": "Reference only",
    "all_haplotypes": "All haplotypes",
    "haplotype_span": "gfaidx haplotype-span selection",
    "legacy": "Interval extraction",
}
# Comparisons in which a gfaidx variant is checked against gbz-base output.
PAIRED_COMPARISONS = {"reference_only", "all_haplotypes"}

# Thread-specific gfaidx runs use related blue/green colors while gbz-base keeps
# a distinct magenta. The cycles also support thread sweeps beyond the default
# 1- and 8-thread comparison without hard-coding those two configurations.
GFAIDX_COLORS = ("#0072B2", "#56B4E9", "#009E73", "#E69F00", "#D55E00")
GFAIDX_MARKERS = ("o", "^", "D", "v", "P")
GBZ_BASE_STYLE = ("#CC79A7", "s", "-", True)
GBZ_QUERY_STYLE = ("#D55E00", "D", "-", True)

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

# Columns added with the context comparisons. Older raw tables lack them and
# receive these defaults.
RAW_OPTIONAL_COLUMNS = {
    "comparison": "legacy",
    "variant": "",
    "context_bp": "NA",
    "with_coords": "NA",
    "gfaidx_threads": "NA",
    "node_set_sha1": "NA",
    "reference_walks": "NA",
    "reference_walk_steps": "NA",
    "reference_walk_sha1": "NA",
}

LOCATION_FIELDS = [
    "query_id",
    "locus_id",
    "reference",
    "contig",
    "start",
    "end",
    "interval_bp",
    "comparison",
    "context_bp",
    "tool",
    "variant",
    "gfaidx_threads",
    "with_coords",
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
    "node_set_sha1",
    "reference_walks",
    "reference_walk_steps",
    "reference_walk_sha1",
]

PARITY_FIELDS = [
    "query_id",
    "interval_bp",
    "comparison",
    "context_bp",
    "variant",
    "gbz_variant",
    "gbz_nodes",
    "gfaidx_nodes",
    "gbz_edges",
    "gfaidx_edges",
    "gbz_paths",
    "gfaidx_paths",
    "node_set_match",
    "gbz_walk_steps",
    "gfaidx_walk_steps",
    "reference_walk_match",
]

SIZE_FIELDS = [
    "comparison",
    "context_bp",
    "interval_bp",
    "tool",
    "variant",
    "gfaidx_threads",
    "with_coords",
    "locations",
    "failed_locations",
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
    "warm_elapsed_vs_gbz_mean",
    "warm_elapsed_vs_gbz_ci_low",
    "warm_elapsed_vs_gbz_ci_high",
    "warm_elapsed_vs_gbz_geomean",
    "warm_elapsed_vs_gbz_geo_ci_low",
    "warm_elapsed_vs_gbz_geo_ci_high",
    "warm_rss_vs_gbz_mean",
    "warm_rss_vs_gbz_ci_low",
    "warm_rss_vs_gbz_ci_high",
    "warm_rss_vs_gbz_geomean",
    "warm_rss_vs_gbz_geo_ci_low",
    "warm_rss_vs_gbz_geo_ci_high",
    "output_nodes_mean",
    "output_nodes_ci_low",
    "output_nodes_ci_high",
    "output_edges_mean",
    "output_edges_ci_low",
    "output_edges_ci_high",
    "output_paths_mean",
    "output_paths_ci_low",
    "output_paths_ci_high",
    "output_bytes_mean",
    "output_bytes_ci_low",
    "output_bytes_ci_high",
    "node_set_matches",
    "reference_walk_matches",
]

REPETITION_FIELDS = [
    "comparison",
    "context_bp",
    "interval_bp",
    "tool",
    "variant",
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

FAILURE_FIELDS = [
    "query_id",
    "locus_id",
    "interval_bp",
    "comparison",
    "context_bp",
    "tool",
    "variant",
    "failed_runs",
    "runs",
    "exit_statuses",
    "reason",
    "stderr_log",
]

CACHE_FIELDS = [
    "comparison",
    "context_bp",
    "variant",
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


def read_raw(path: Path) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    """Read raw executions; return successful rows and failed rows separately."""
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        missing = RAW_REQUIRED_COLUMNS.difference(reader.fieldnames or [])
        if missing:
            raise ValueError(f"{path} is missing columns: {', '.join(sorted(missing))}")

        rows: list[dict[str, object]] = []
        failed: list[dict[str, object]] = []
        for raw in reader:
            exit_status = int(parse_number(raw["exit_status"], "exit_status", True))
            row: dict[str, object] = dict(raw)
            for field, default in RAW_OPTIONAL_COLUMNS.items():
                if not row.get(field):
                    row[field] = default
            # Legacy tables stored the variant label in `tool`.
            if not row["variant"]:
                row["variant"] = row["tool"]
            if exit_status != 0:
                # A failed execution has no output measurements; keep only what
                # is needed to report it as NA.
                row["exit_status"] = exit_status
                for field in ("interval_bp", "run_index"):
                    row[field] = int(parse_number(raw[field], field, True))
                failed.append(row)
                continue
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

    if not rows:
        raise ValueError(f"{path} contains no successful executions")
    return rows, failed


def failure_reason(row: dict[str, object]) -> str:
    """Classify a failed execution from its exit status and stderr log."""
    status = int(row["exit_status"])
    text = ""
    stderr_log = Path(str(row.get("stderr_log") or ""))
    if stderr_log.is_file():
        with stderr_log.open("r", encoding="utf-8", errors="replace") as handle:
            text = handle.read()[-65536:]
    if "heap out of memory" in text:
        return "JavaScript heap out of memory"
    if "Invalid string length" in text:
        return "JavaScript string length limit"
    if status > 128:
        return f"killed by signal {status - 128}"
    return f"exit status {status}"


def location_key(row: dict[str, object]) -> tuple[str, str, str, str]:
    """Identify one query location of one measured configuration."""
    return (str(row["query_id"]),) + series_key(row)


def build_failure_rows(
    rows: list[dict[str, object]], failed: list[dict[str, object]]
) -> list[dict[str, object]]:
    """Summarize every location that had at least one failed execution."""
    runs: dict[tuple[str, str, str, str], int] = defaultdict(int)
    for row in rows:
        runs[location_key(row)] += 1
    grouped: dict[tuple[str, str, str, str], list[dict[str, object]]] = defaultdict(list)
    for row in failed:
        grouped[location_key(row)].append(row)

    output: list[dict[str, object]] = []
    for key, group in sorted(
        grouped.items(),
        key=lambda item: (
            item[0][0],
            comparison_sort_key(item[0][1]),
            context_sort_key(item[0][2]),
            variant_sort_key(item[0][3]),
        ),
    ):
        group.sort(key=lambda row: int(row["run_index"]))
        output.append(
            {
                "query_id": key[0],
                "locus_id": group[0]["locus_id"],
                "interval_bp": group[0]["interval_bp"],
                "comparison": key[1],
                "context_bp": key[2],
                "tool": group[0]["tool"],
                "variant": key[3],
                "gfaidx_threads": group[0]["gfaidx_threads"],
                "with_coords": group[0]["with_coords"],
                "failed_runs": len(group),
                "runs": len(group) + runs[key],
                "exit_statuses": ",".join(
                    str(value) for value in sorted({int(row["exit_status"]) for row in group})
                ),
                "reason": "; ".join(sorted({failure_reason(row) for row in group})),
                "stderr_log": group[0].get("stderr_log", "NA"),
            }
        )
    return output


def failure_group(row: dict[str, object]) -> tuple[str, str, int, str]:
    """Identify the interval-size group a location or failure belongs to."""
    comparison, context, variant = series_key(row)
    return comparison, context, int(row["interval_bp"]), variant


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
        variant = rows[0]["variant"]
        context = rows[0]["context_bp"]
        raise ValueError(
            f"{field} changed across repetitions for {query}/{variant}"
            f" (context {context}): {values}"
        )
    return next(iter(values))


def context_sort_key(context: object) -> tuple[int, int]:
    """Order numeric contexts ascending and place context-free rows last."""
    text = str(context)
    return (1, 0) if text == "NA" else (0, int(text))


def comparison_sort_key(comparison: object) -> tuple[int, str]:
    """Keep the like-for-like comparison first in tables and plots."""
    text = str(comparison)
    if text in COMPARISON_ORDER:
        return COMPARISON_ORDER.index(text), text
    return len(COMPARISON_ORDER), text


def series_key(row: dict[str, object]) -> tuple[str, str, str]:
    """Identify one measured configuration independently of its location."""
    return str(row["comparison"]), str(row["context_bp"]), str(row["variant"])


def build_location_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    """Collapse technical repetitions into one first and one warm value."""
    grouped: dict[tuple[str, str, str, str], list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        grouped[(str(row["query_id"]),) + series_key(row)].append(row)

    output: list[dict[str, object]] = []
    for (query_id, comparison, context, variant), group in sorted(
        grouped.items(),
        key=lambda item: (
            item[0][0],
            comparison_sort_key(item[0][1]),
            context_sort_key(item[0][2]),
            variant_sort_key(item[0][3]),
        ),
    ):
        group.sort(key=lambda row: int(row["run_index"]))
        run_indexes = [int(row["run_index"]) for row in group]
        if len(set(run_indexes)) != len(run_indexes):
            raise ValueError(f"duplicate run_index for {query_id}/{variant} (context {context})")
        if run_indexes[0] != 1:
            raise ValueError(f"run 1 is missing for {query_id}/{variant} (context {context})")

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
                "comparison": comparison,
                "context_bp": context,
                "tool": consistent_value(group, "tool"),
                "variant": variant,
                "gfaidx_threads": consistent_value(group, "gfaidx_threads"),
                "with_coords": consistent_value(group, "with_coords"),
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
                "node_set_sha1": consistent_value(group, "node_set_sha1"),
                "reference_walks": consistent_value(group, "reference_walks"),
                "reference_walk_steps": consistent_value(group, "reference_walk_steps"),
                "reference_walk_sha1": consistent_value(group, "reference_walk_sha1"),
            }
        )
    return output


def hash_match(left: object, right: object) -> str:
    """Compare two recorded hashes; NA means at least one was not available."""
    if str(left) == "NA" or str(right) == "NA":
        return "NA"
    return "yes" if left == right else "no"


def is_gbz_base(row: dict[str, object]) -> bool:
    """Recognize the gbz-base baseline rows of current and legacy raw tables.

    Legacy tables stored the variant label `gbz-base` in `tool`. gbz-base-query
    is not a baseline: like a gfaidx variant it is compared with gbz-base-all.
    """
    return str(row["tool"]) == "gbz-base"


def build_gbz_index(
    locations: list[dict[str, object]],
) -> dict[tuple[str, str, str], dict[str, object]]:
    """Find the gbz-base location row paired with each query and context."""
    index: dict[tuple[str, str, str], dict[str, object]] = {}
    for location in locations:
        if is_gbz_base(location):
            key = (
                str(location["query_id"]),
                str(location["comparison"]),
                str(location["context_bp"]),
            )
            index[key] = location
    return index


def build_parity_rows(locations: list[dict[str, object]]) -> list[dict[str, object]]:
    """Compare every gfaidx output with the gbz-base output for the same unit."""
    gbz_index = build_gbz_index(locations)
    output: list[dict[str, object]] = []
    for location in locations:
        if is_gbz_base(location) or location["comparison"] not in PAIRED_COMPARISONS:
            continue
        partner = gbz_index.get(
            (str(location["query_id"]), str(location["comparison"]), str(location["context_bp"]))
        )
        if partner is None:
            continue
        output.append(
            {
                "query_id": location["query_id"],
                "interval_bp": location["interval_bp"],
                "comparison": location["comparison"],
                "context_bp": location["context_bp"],
                "variant": location["variant"],
                "gbz_variant": partner["variant"],
                "gbz_nodes": partner["output_nodes"],
                "gfaidx_nodes": location["output_nodes"],
                "gbz_edges": partner["output_edges"],
                "gfaidx_edges": location["output_edges"],
                "gbz_paths": partner["output_paths"],
                "gfaidx_paths": location["output_paths"],
                "node_set_match": hash_match(
                    partner["node_set_sha1"], location["node_set_sha1"]
                ),
                "gbz_walk_steps": partner["reference_walk_steps"],
                "gfaidx_walk_steps": location["reference_walk_steps"],
                "reference_walk_match": hash_match(
                    partner["reference_walk_sha1"], location["reference_walk_sha1"]
                ),
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


def add_geo_stats(
    row: dict[str, object],
    prefix: str,
    values: Iterable[float],
    samples: int,
    seed: int,
    key: str,
) -> None:
    """Add `<prefix>_geomean/geo_ci_low/geo_ci_high` fields to a summary row.

    The mean and bootstrap interval are taken over log ratios and transformed
    back, so a 10x faster and a 10x slower location cancel out instead of the
    slower one dominating the arithmetic mean.
    """
    center, low, high = mean_and_ci(
        (math.log(value) for value in values if value > 0), samples, seed, key
    )
    row[f"{prefix}_geomean"] = None if center is None else math.exp(center)
    row[f"{prefix}_geo_ci_low"] = None if low is None else math.exp(low)
    row[f"{prefix}_geo_ci_high"] = None if high is None else math.exp(high)


def match_fraction(parity: list[dict[str, object]], field: str) -> str:
    """Summarize yes/no parity values as `matches/compared`, or NA."""
    compared = [row[field] for row in parity if row[field] in ("yes", "no")]
    if not compared:
        return "NA"
    return f"{sum(1 for value in compared if value == 'yes')}/{len(compared)}"


def location_ratios(
    group: list[dict[str, object]],
    gbz_index: dict[tuple[str, str, str], dict[str, object]],
    field: str,
) -> list[float]:
    """Divide each gfaidx location value by its paired gbz-base value."""
    ratios: list[float] = []
    for location in group:
        partner = gbz_index.get(
            (str(location["query_id"]), str(location["comparison"]), str(location["context_bp"]))
        )
        if partner is None or location[field] is None or partner[field] is None:
            continue
        # GNU time reports only hundredths of a second; skip zero baselines.
        if float(partner[field]) > 0:
            ratios.append(float(location[field]) / float(partner[field]))
    return ratios


def build_size_rows(
    locations: list[dict[str, object]],
    parity_rows: list[dict[str, object]],
    failure_rows: list[dict[str, object]],
    samples: int,
    seed: int,
) -> list[dict[str, object]]:
    """Aggregate first and warm per-location values by interval length.

    Statistics cover the successful locations only; `failed_locations` says how
    many locations of the size were NA, and a size where every location failed
    is still listed, with NA statistics.
    """
    grouped: dict[tuple[str, str, int, str], list[dict[str, object]]] = defaultdict(list)
    for location in locations:
        grouped[failure_group(location)].append(location)
    failures: dict[tuple[str, str, int, str], list[dict[str, object]]] = defaultdict(list)
    for failure in failure_rows:
        failures[failure_group(failure)].append(failure)
    parity_groups: dict[tuple[str, str, int, str], list[dict[str, object]]] = defaultdict(list)
    for parity in parity_rows:
        parity_groups[
            (
                str(parity["comparison"]),
                str(parity["context_bp"]),
                int(parity["interval_bp"]),
                str(parity["variant"]),
            )
        ].append(parity)
    gbz_index = build_gbz_index(locations)

    output: list[dict[str, object]] = []
    for (comparison, context, interval_bp, variant), group in sorted(
        grouped.items(),
        key=lambda item: (
            comparison_sort_key(item[0][0]),
            context_sort_key(item[0][1]),
            item[0][2],
            variant_sort_key(item[0][3]),
        ),
    ):
        row: dict[str, object] = {
            "comparison": comparison,
            "context_bp": context,
            "interval_bp": interval_bp,
            "tool": group[0]["tool"],
            "variant": variant,
            "gfaidx_threads": group[0]["gfaidx_threads"],
            "with_coords": group[0]["with_coords"],
            "locations": len(group),
            "failed_locations": len(failures.get((comparison, context, interval_bp, variant), [])),
            "runs": ",".join(str(value) for value in sorted({int(x["repetitions"]) for x in group})),
        }
        key = f"size:{comparison}:{context}:{interval_bp}:{variant}"
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
        # Ratios are formed per location before averaging, so a slow region
        # does not dominate the relative cost estimate.
        paired = not is_gbz_base(group[0]) and comparison in PAIRED_COMPARISONS
        elapsed_ratios = (
            location_ratios(group, gbz_index, "warm_median_elapsed_seconds") if paired else []
        )
        rss_ratios = location_ratios(group, gbz_index, "warm_median_max_rss_kb") if paired else []
        add_stats(row, "warm_elapsed_vs_gbz", elapsed_ratios, samples, seed, key + ":elapsed_vs_gbz")
        add_geo_stats(
            row, "warm_elapsed_vs_gbz", elapsed_ratios, samples, seed, key + ":elapsed_vs_gbz_geo"
        )
        add_stats(row, "warm_rss_vs_gbz", rss_ratios, samples, seed, key + ":rss_vs_gbz")
        add_geo_stats(row, "warm_rss_vs_gbz", rss_ratios, samples, seed, key + ":rss_vs_gbz_geo")
        for field in ("output_nodes", "output_edges", "output_paths", "output_bytes"):
            add_stats(
                row,
                field,
                (float(x[field]) for x in group),
                samples,
                seed,
                f"{key}:{field}",
            )
        parity = parity_groups.get((comparison, context, interval_bp, variant), [])
        row["node_set_matches"] = match_fraction(parity, "node_set_match")
        row["reference_walk_matches"] = match_fraction(parity, "reference_walk_match")
        output.append(row)

    for (comparison, context, interval_bp, variant), group in failures.items():
        if (comparison, context, interval_bp, variant) in grouped:
            continue
        output.append(
            {
                "comparison": comparison,
                "context_bp": context,
                "interval_bp": interval_bp,
                "tool": group[0]["tool"],
                "variant": variant,
                "gfaidx_threads": group[0]["gfaidx_threads"],
                "with_coords": group[0]["with_coords"],
                "locations": 0,
                "failed_locations": len(group),
                "runs": "NA",
                "node_set_matches": "NA",
                "reference_walk_matches": "NA",
            }
        )
    output.sort(
        key=lambda row: (
            comparison_sort_key(row["comparison"]),
            context_sort_key(row["context_bp"]),
            int(row["interval_bp"]),
            variant_sort_key(str(row["variant"])),
        )
    )
    return output


def build_repetition_rows(
    rows: list[dict[str, object]], samples: int, seed: int
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    """Aggregate actual and run-1-normalized measurements by repetition."""
    first_elapsed: dict[tuple[str, str, str, str], float] = {}
    for row in rows:
        if int(row["run_index"]) == 1:
            first_elapsed[(str(row["query_id"]),) + series_key(row)] = float(
                row["elapsed_seconds"]
            )

    grouped: dict[tuple[str, str, int, str, int], list[dict[str, object]]] = defaultdict(list)
    overall: dict[tuple[str, str, str, int], list[float]] = defaultdict(list)
    zero_baselines: set[tuple[str, str, str, str]] = set()
    for row in rows:
        key = (str(row["query_id"]),) + series_key(row)
        baseline = first_elapsed.get(key)
        if baseline is None:
            raise ValueError(f"missing run-1 elapsed time for {key[0]}/{key[3]} (context {key[2]})")
        # GNU time reports only hundredths of a second. Keep absolute metrics
        # for very fast queries, but do not divide by a rounded zero baseline.
        ratio = float(row["elapsed_seconds"]) / baseline if baseline > 0 else None
        if ratio is None:
            zero_baselines.add(key)
        row_with_ratio = dict(row)
        row_with_ratio["elapsed_relative_to_run1"] = ratio
        comparison, context, variant = series_key(row)
        group_key = (comparison, context, int(row["interval_bp"]), variant, int(row["run_index"]))
        grouped[group_key].append(row_with_ratio)
        if ratio is not None:
            overall[(comparison, context, variant, int(row["run_index"]))].append(ratio)

    if zero_baselines:
        print(
            "warning: omitted normalized cache ratios for "
            f"{len(zero_baselines)} query/variant pair(s) whose run 1 rounded to zero",
            file=sys.stderr,
        )

    repetition_rows: list[dict[str, object]] = []
    for (comparison, context, interval_bp, variant, run_index), group in sorted(
        grouped.items(),
        key=lambda item: (
            comparison_sort_key(item[0][0]),
            context_sort_key(item[0][1]),
            item[0][2],
            variant_sort_key(item[0][3]),
            item[0][4],
        ),
    ):
        row = {
            "comparison": comparison,
            "context_bp": context,
            "interval_bp": interval_bp,
            "tool": group[0]["tool"],
            "variant": variant,
            "run_index": run_index,
            "locations": len(group),
        }
        key = f"repeat:{comparison}:{context}:{interval_bp}:{variant}:{run_index}"
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
    for (comparison, context, variant, run_index), ratios in sorted(
        overall.items(),
        key=lambda item: (
            comparison_sort_key(item[0][0]),
            context_sort_key(item[0][1]),
            variant_sort_key(item[0][2]),
            item[0][3],
        ),
    ):
        row = {
            "comparison": comparison,
            "context_bp": context,
            "variant": variant,
            "run_index": run_index,
            "queries": len(ratios),
        }
        add_stats(
            row,
            "elapsed_relative_to_run1",
            ratios,
            samples,
            seed,
            f"cache:{comparison}:{context}:{variant}:{run_index}",
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


def parse_gfaidx_variant(variant: str) -> tuple[str | None, int] | None:
    """Return (kind, threads) for gfaidx variants, or None for other tools."""
    match = GFAIDX_VARIANT_PATTERN.fullmatch(variant)
    if match:
        return match.group(1), int(match.group(2))
    # Accept raw tables made by the earlier two-tool runner as a 1-thread run.
    if variant == "gfaidx":
        return None, 1
    return None


def variant_sort_key(variant: str) -> tuple[int, int, int, str]:
    """Order gbz-base first, then gbz-base-query, then gfaidx by kind and threads."""
    if variant.startswith("gbz-base-query"):
        return 0, 1, 0, variant
    if variant.startswith("gbz-base"):
        return 0, 0, 0, variant
    parsed = parse_gfaidx_variant(variant)
    if parsed is not None:
        kind, threads = parsed
        return 1, GFAIDX_KIND_ORDER.index(kind), threads, variant
    return 2, 0, 0, variant


def ordered_variants(rows: Iterable[dict[str, object]]) -> list[str]:
    """Discover variants from the data in a stable display order."""
    return sorted({str(row["variant"]) for row in rows}, key=variant_sort_key)


def build_variant_styles(variants: list[str]) -> dict[str, tuple[str, str, str, bool]]:
    """Assign color, marker, line style, and marker fill consistently.

    Colors and markers follow the gfaidx thread count, so each gfaidx mode keeps
    the same color at a given thread count; variants without coordinates use a
    dashed line and open markers.
    """
    thread_counts = sorted(
        {parsed[1] for parsed in map(parse_gfaidx_variant, variants) if parsed is not None}
    )
    styles: dict[str, tuple[str, str, str, bool]] = {}
    for variant in variants:
        parsed = parse_gfaidx_variant(variant)
        if variant.startswith("gbz-base-query"):
            styles[variant] = GBZ_QUERY_STYLE
        elif variant.startswith("gbz-base"):
            styles[variant] = GBZ_BASE_STYLE
        elif parsed is not None:
            kind, threads = parsed
            index = thread_counts.index(threads)
            without_coords = kind in GFAIDX_KINDS_WITHOUT_COORDS
            styles[variant] = (
                GFAIDX_COLORS[index % len(GFAIDX_COLORS)],
                GFAIDX_MARKERS[index % len(GFAIDX_MARKERS)],
                "--" if without_coords else "-",
                not without_coords,
            )
        else:
            styles[variant] = ("#666666", "x", "-", True)
    return styles


def style_kwargs(style: tuple[str, str, str, bool]) -> dict[str, object]:
    """Translate one variant style into Matplotlib errorbar keyword arguments."""
    color, marker, linestyle, filled = style
    return {
        "color": color,
        "marker": marker,
        "linestyle": linestyle,
        "markerfacecolor": color if filled else "none",
    }


def variant_label(variant: str) -> str:
    """Turn the raw variant identifier into a readable plot label."""
    if variant in GBZ_BASE_LABELS:
        return GBZ_BASE_LABELS[variant]
    parsed = parse_gfaidx_variant(variant)
    if parsed is None:
        return variant
    kind, threads = parsed
    suffix = "thread" if threads == 1 else "threads"
    return f"gfaidx {GFAIDX_KIND_LABELS[kind]} ({threads} {suffix})"


def context_label(context: str) -> str:
    """Describe a context for plot titles."""
    return "no context" if context == "NA" else f"context {format_bp(float(context))}"


def context_slug(context: str) -> str:
    """Name a context in output filenames."""
    return "noctx" if context == "NA" else f"ctx{context}"


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


def metric_settings(metric: str, warm_only: bool = False) -> tuple[tuple[str, ...], str, float]:
    """Return summary prefixes, axis label, and scale for time or memory."""
    if metric == "elapsed":
        prefixes = ("warm_elapsed",) if warm_only else ("first_elapsed", "warm_elapsed")
        return prefixes, "Wall time (seconds)", 1.0
    prefixes = ("warm_rss_kb",) if warm_only else ("first_rss_kb", "warm_rss_kb")
    return prefixes, "Peak RSS (GiB)", 1024.0 * 1024.0


def plot_scaling(
    size_rows: list[dict[str, object]],
    output: Path,
    title: str,
    formats: list[str],
    metric: str,
    variants: list[str],
    styles: dict[str, tuple[str, str, str, bool]],
) -> None:
    """Plot first and warm interval scaling for elapsed time or peak RSS."""
    from matplotlib import pyplot as plt
    from matplotlib.ticker import FuncFormatter

    prefixes, ylabel, scale = metric_settings(metric)
    figure, axes = plt.subplots(1, 2, figsize=(11.5, 4.4), sharey=True)
    for axis, prefix, panel in zip(axes, prefixes, ("First execution", "Warm repetitions (median 2..N)")):
        plotted_y: list[float] = []
        for variant in variants:
            selected = [
                row
                for row in size_rows
                if row["variant"] == variant and row.get(f"{prefix}_mean") is not None
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
                linewidth=1.8,
                capsize=3,
                label=variant_label(variant),
                **style_kwargs(styles[variant]),
            )
        axis.set_xscale("log")
        # GNU time rounds very short test commands to zero. Real graph queries
        # normally use the log scale; retain a readable linear axis for zeros.
        if plotted_y and min(plotted_y) > 0:
            axis.set_yscale("log")
        axis.xaxis.set_major_formatter(FuncFormatter(lambda value, _pos: format_bp(value)))
        axis.set_title(panel)
        axis.set_xlabel("Requested interval length")
        axis.grid(alpha=0.25)
    axes[0].set_ylabel(ylabel)
    axes[0].legend(frameon=False)
    figure.suptitle(title)
    save_figure(figure, output, formats)
    plt.close(figure)


def plot_by_context(
    size_rows: list[dict[str, object]],
    output: Path,
    title: str,
    formats: list[str],
    metric: str,
    variants: list[str],
    styles: dict[str, tuple[str, str, str, bool]],
) -> None:
    """Plot warm time or memory against context, one panel per interval size.

    Contexts are placed at equal categorical positions because zero is a valid
    context and the sweep is usually roughly geometric.
    """
    from matplotlib import pyplot as plt

    (prefix,), ylabel, scale = metric_settings(metric, warm_only=True)
    contexts = sorted({str(row["context_bp"]) for row in size_rows}, key=context_sort_key)
    positions = {context: index for index, context in enumerate(contexts)}
    sizes = sorted({int(row["interval_bp"]) for row in size_rows})
    columns = min(3, len(sizes))
    rows = math.ceil(len(sizes) / columns)
    figure, axes_grid = plt.subplots(
        rows, columns, figsize=(5.0 * columns, 3.9 * rows), squeeze=False, sharey=True
    )
    axes = [axis for grid_row in axes_grid for axis in grid_row]
    plotted_y: list[float] = []
    for axis, interval_bp in zip(axes, sizes):
        for variant in variants:
            selected = [
                row
                for row in size_rows
                if row["variant"] == variant
                and int(row["interval_bp"]) == interval_bp
                and row.get(f"{prefix}_mean") is not None
            ]
            selected.sort(key=lambda row: context_sort_key(row["context_bp"]))
            if not selected:
                continue
            values = [error_values(row, prefix, scale) for row in selected]
            plotted_y.extend(item[0] for item in values)
            axis.errorbar(
                [positions[str(row["context_bp"])] for row in selected],
                [item[0] for item in values],
                yerr=[
                    [item[1][0] for item in values],
                    [item[1][1] for item in values],
                ],
                linewidth=1.5,
                capsize=2,
                label=variant_label(variant),
                **style_kwargs(styles[variant]),
            )
        axis.set_xticks(range(len(contexts)))
        axis.set_xticklabels([format_bp(float(context)) for context in contexts])
        axis.set_title(f"{format_bp(interval_bp)} interval")
        axis.set_xlabel("Context")
        axis.grid(alpha=0.25)
    if plotted_y and min(plotted_y) > 0:
        axes[0].set_yscale("log")
    for axis in axes[len(sizes) :]:
        axis.set_visible(False)
    for grid_row in axes_grid:
        grid_row[0].set_ylabel(f"Warm {ylabel[0].lower()}{ylabel[1:]}")
    axes[0].legend(frameon=False)
    figure.suptitle(title)
    save_figure(figure, output, formats)
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
    output: Path,
    title: str,
    formats: list[str],
    variants: list[str],
    styles: dict[str, tuple[str, str, str, bool]],
) -> None:
    """Show warm measurements from every location behind the aggregate error bars."""
    from matplotlib import pyplot as plt
    from matplotlib.ticker import FuncFormatter

    # One panel per variant keeps multiple gfaidx variants readable instead of
    # overlaying all genomic locations in a single dense scatter plot.
    figure, axes_grid = plt.subplots(
        1, len(variants), figsize=(5.2 * len(variants), 4.4), sharey=True, squeeze=False
    )
    axes = list(axes_grid[0])
    for axis, variant in zip(axes, variants):
        color = styles[variant][0]
        individual = [
            row
            for row in locations
            if row["variant"] == variant and row["warm_median_elapsed_seconds"] is not None
        ]
        axis.scatter(
            [int(row["interval_bp"]) for row in individual],
            [float(row["warm_median_elapsed_seconds"]) for row in individual],
            color=color,
            alpha=0.38,
            s=24,
            label="individual locations",
        )
        aggregates = [
            row
            for row in size_rows
            if row["variant"] == variant and row["warm_elapsed_mean"] is not None
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
            linewidth=1.8,
            capsize=3,
            label="mean and 95% CI",
            **style_kwargs(styles[variant]),
        )
        axis.set_xscale("log")
        plotted_y = [float(row["warm_median_elapsed_seconds"]) for row in individual]
        plotted_y.extend(item[0] for item in values)
        if plotted_y and min(plotted_y) > 0:
            axis.set_yscale("log")
        axis.xaxis.set_major_formatter(FuncFormatter(lambda value, _pos: format_bp(value)))
        axis.set_title(variant_label(variant))
        axis.set_xlabel("Requested interval length")
        axis.grid(alpha=0.25)
        axis.legend(frameon=False)
    axes[0].set_ylabel("Warm wall time (seconds)")
    figure.suptitle(title)
    save_figure(figure, output, formats)
    plt.close(figure)


def plot_cache_effect(
    cache_rows: list[dict[str, object]],
    output: Path,
    title: str,
    formats: list[str],
    variants: list[str],
    styles: dict[str, tuple[str, str, str, bool]],
) -> None:
    """Plot elapsed time relative to run 1, one panel per context."""
    from matplotlib import pyplot as plt
    from matplotlib.ticker import MaxNLocator

    contexts = sorted({str(row["context_bp"]) for row in cache_rows}, key=context_sort_key)
    figure, axes_grid = plt.subplots(
        1, len(contexts), figsize=(6.0 * len(contexts), 4.6), sharey=True, squeeze=False
    )
    axes = list(axes_grid[0])
    for axis, context in zip(axes, contexts):
        for variant in variants:
            selected = [
                row
                for row in cache_rows
                if row["variant"] == variant
                and str(row["context_bp"]) == context
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
                linewidth=1.8,
                capsize=3,
                label=variant_label(variant),
                **style_kwargs(styles[variant]),
            )
        axis.axhline(1.0, color="#666666", linestyle="--", linewidth=1)
        axis.xaxis.set_major_locator(MaxNLocator(integer=True))
        axis.set_xlabel("Repetition")
        axis.set_title(context_label(context))
        axis.grid(alpha=0.25)
    axes[0].set_ylabel("Wall time relative to run 1")
    handles, labels = axes[0].get_legend_handles_labels()
    if handles:
        axes[0].legend(frameon=False)
    figure.suptitle(title)
    save_figure(figure, output, formats)
    plt.close(figure)


def plot_repetitions_by_size(
    repetition_rows: list[dict[str, object]],
    output: Path,
    title: str,
    formats: list[str],
    variants: list[str],
    styles: dict[str, tuple[str, str, str, bool]],
) -> None:
    """Plot actual run time by repetition in one panel per interval size."""
    from matplotlib import pyplot as plt
    from matplotlib.ticker import MaxNLocator

    sizes = sorted({int(row["interval_bp"]) for row in repetition_rows})
    columns = min(3, len(sizes))
    rows = math.ceil(len(sizes) / columns)
    figure, axes_grid = plt.subplots(rows, columns, figsize=(5.0 * columns, 3.8 * rows), squeeze=False)
    axes = [axis for grid_row in axes_grid for axis in grid_row]
    for axis, interval_bp in zip(axes, sizes):
        for variant in variants:
            selected = [
                row
                for row in repetition_rows
                if int(row["interval_bp"]) == interval_bp and row["variant"] == variant
            ]
            selected.sort(key=lambda row: int(row["run_index"]))
            if not selected:
                continue
            values = [error_values(row, "elapsed") for row in selected]
            axis.errorbar(
                [int(row["run_index"]) for row in selected],
                [item[0] for item in values],
                yerr=[
                    [item[1][0] for item in values],
                    [item[1][1] for item in values],
                ],
                linewidth=1.5,
                capsize=2,
                label=variant_label(variant),
                **style_kwargs(styles[variant]),
            )
        axis.set_title(format_bp(interval_bp))
        axis.xaxis.set_major_locator(MaxNLocator(integer=True))
        axis.set_xlabel("Repetition")
        axis.set_ylabel("Wall time (seconds)")
        axis.grid(alpha=0.25)
    for axis in axes[len(sizes) :]:
        axis.set_visible(False)
    axes[0].legend(frameon=False)
    figure.suptitle(title)
    save_figure(figure, output, formats)
    plt.close(figure)


def plot_output_nodes(
    size_rows: list[dict[str, object]],
    output: Path,
    title: str,
    formats: list[str],
    variants: list[str],
    styles: dict[str, tuple[str, str, str, bool]],
) -> None:
    """Plot returned node counts so workload differences remain visible."""
    from matplotlib import pyplot as plt
    from matplotlib.ticker import FuncFormatter

    figure, axis = plt.subplots(figsize=(6.8, 4.6))
    for variant in variants:
        selected = [row for row in size_rows if row["variant"] == variant]
        selected.sort(key=lambda row: int(row["interval_bp"]))
        if not selected:
            continue
        values = [error_values(row, "output_nodes") for row in selected]
        axis.errorbar(
            [int(row["interval_bp"]) for row in selected],
            [item[0] for item in values],
            yerr=[
                [item[1][0] for item in values],
                [item[1][1] for item in values],
            ],
            linewidth=1.8,
            capsize=3,
            label=variant_label(variant),
            **style_kwargs(styles[variant]),
        )
    axis.set_xscale("log")
    axis.set_yscale("log")
    axis.xaxis.set_major_formatter(FuncFormatter(lambda value, _pos: format_bp(value)))
    axis.set_xlabel("Requested interval length")
    axis.set_ylabel("Output nodes")
    axis.set_title(title)
    axis.grid(alpha=0.25)
    axis.legend(frameon=False)
    save_figure(figure, output, formats)
    plt.close(figure)


def plot_comparison(
    comparison: str,
    locations: list[dict[str, object]],
    size_rows: list[dict[str, object]],
    repetition_rows: list[dict[str, object]],
    cache_rows: list[dict[str, object]],
    plot_dir: Path,
    formats: list[str],
    styles: dict[str, tuple[str, str, str, bool]],
) -> None:
    """Write every plot for one comparison, one set per context."""

    def subset(rows: list[dict[str, object]], context: str | None = None) -> list[dict[str, object]]:
        return [
            row
            for row in rows
            if row["comparison"] == comparison
            and (context is None or str(row["context_bp"]) == context)
        ]

    heading = COMPARISON_TITLES.get(comparison, comparison)
    comparison_sizes = subset(size_rows)
    variants = ordered_variants(comparison_sizes)
    contexts = sorted({str(row["context_bp"]) for row in comparison_sizes}, key=context_sort_key)
    for context in contexts:
        stem = f"{comparison}_{context_slug(context)}"
        title = f"{heading}, {context_label(context)}"
        context_sizes = subset(size_rows, context)
        context_variants = ordered_variants(context_sizes)
        plot_scaling(
            context_sizes, plot_dir / f"{stem}_time_first_and_warm", title,
            formats, "elapsed", context_variants, styles,
        )
        plot_scaling(
            context_sizes, plot_dir / f"{stem}_memory_first_and_warm", title,
            formats, "rss", context_variants, styles,
        )
        plot_location_variation(
            subset(locations, context), context_sizes,
            plot_dir / f"{stem}_time_location_variation",
            f"{title}: variation among genomic locations", formats, context_variants, styles,
        )
        plot_repetitions_by_size(
            subset(repetition_rows, context),
            plot_dir / f"{stem}_cache_effect_by_interval_size",
            f"{title}: repeated extraction by requested size", formats, context_variants, styles,
        )
        plot_output_nodes(
            context_sizes, plot_dir / f"{stem}_output_nodes",
            f"{title}: extracted graph size", formats, context_variants, styles,
        )
    # Cache ratios are omitted when every run 1 rounded to zero seconds.
    if subset(cache_rows):
        plot_cache_effect(
            subset(cache_rows), plot_dir / f"{comparison}_cache_effect_normalized",
            f"{heading}: repeated-query page-cache effect", formats, variants, styles,
        )
    if contexts != ["NA"]:
        plot_by_context(
            comparison_sizes, plot_dir / f"{comparison}_warm_time_by_context",
            f"{heading}: warm wall time by context", formats, "elapsed", variants, styles,
        )
        plot_by_context(
            comparison_sizes, plot_dir / f"{comparison}_warm_memory_by_context",
            f"{heading}: warm peak memory by context", formats, "rss", variants, styles,
        )


# Overview figure: every tool and mode in one figure per context and gfaidx
# thread count. Hue encodes what was extracted, so the two tools producing the
# same output share a color; line style and marker encode the tool. The four
# hues pass a colorblind and normal-vision separation check on every pair.
OVERVIEW_SERIES = (
    # (variant, or a gfaidx kind expanded with the thread count; output class; label)
    ("gbz-base-ref", "reference", "gbz-base --reference-only"),
    ("gbz-base-all", "anonymous", "gbz-base, all haplotypes (unnamed walks)"),
    ("gbz-base-query-resolved", "named", "gbz-base-query + haplotype index, all haplotypes (named)"),
    ("ref-coords", "reference", "gfaidx --mode reference --with_coords"),
    ("bfs", "anonymous", "gfaidx --mode bfs (no coordinates)"),
    ("bfs-coords", "named", "gfaidx --mode bfs --with_coords (named)"),
    ("span-coords", "span", "gfaidx --mode all --with_coords (haplotype spans, no context)"),
)
OVERVIEW_CLASS_COLORS = {
    "reference": "#2a78d6",
    "anonymous": "#eb6834",
    "named": "#1baf7a",
    "span": "#4a3aa7",
}
OVERVIEW_TOOL_STYLES = {
    # tool: (line style, marker)
    "gfaidx": ("-", "o"),
    "gbz-base": ("--", "s"),
    "gbz-base-query": ("--", "D"),
}
OVERVIEW_INK = {"primary": "#0b0b0b", "secondary": "#52514e", "muted": "#898781"}
OVERVIEW_GRID = "#e1e0d9"
OVERVIEW_AXIS = "#c3c2b7"


def overview_series(threads: int) -> list[tuple[str, str, str, str]]:
    """Return (variant, tool, output class, label) for one gfaidx thread count."""
    series = []
    for name, output_class, label in OVERVIEW_SERIES:
        if name.startswith("gbz-base-query"):
            series.append((name, "gbz-base-query", output_class, label))
        elif name.startswith("gbz-base"):
            series.append((name, "gbz-base", output_class, label))
        else:
            suffix = "thread" if threads == 1 else "threads"
            series.append(
                (f"gfaidx-{name}-{threads}t", "gfaidx", output_class, f"{label}, {threads} {suffix}")
            )
    return series


def plot_overview(size_rows: list[dict[str, object]], plot_dir: Path, formats: list[str]) -> None:
    """Draw time and memory of every variant together, first and warm runs.

    One figure per context and gfaidx thread count. A variant is drawn only at
    sizes where every location succeeded; a size with failed locations gets an
    x at the top of the panel with the number of failed locations.
    """
    from matplotlib import pyplot as plt
    from matplotlib import transforms
    from matplotlib.lines import Line2D
    from matplotlib.ticker import FixedLocator, FuncFormatter, NullFormatter

    contexts = sorted(
        {str(row["context_bp"]) for row in size_rows if str(row["context_bp"]) != "NA"},
        key=context_sort_key,
    )
    thread_counts = sorted(
        {
            parsed[1]
            for parsed in (parse_gfaidx_variant(str(row["variant"])) for row in size_rows)
            if parsed is not None
        }
    )
    by_variant: dict[tuple[str, str], list[dict[str, object]]] = defaultdict(list)
    for row in size_rows:
        by_variant[(str(row["variant"]), str(row["context_bp"]))].append(row)
    sizes = sorted({int(row["interval_bp"]) for row in size_rows})
    panels = (
        ("first_elapsed", "First execution: wall time", "Wall time (s)", 1.0),
        ("first_rss_kb", "First execution: peak memory", "Peak RSS (GiB)", 1024.0 * 1024.0),
        ("warm_elapsed", "Warm (median of runs 2..N): wall time", "Wall time (s)", 1.0),
        ("warm_rss_kb", "Warm (median of runs 2..N): peak memory", "Peak RSS (GiB)", 1024.0 * 1024.0),
    )

    for context in contexts:
        for threads in thread_counts or [1]:
            series = [
                item
                for item in overview_series(threads)
                if by_variant.get((item[0], "NA" if item[0].startswith("gfaidx-span") else context))
            ]
            if not series:
                continue
            figure, axes_grid = plt.subplots(2, 2, figsize=(11.0, 8.2), sharex=True)
            for panel_index, (axis, (prefix, title, ylabel, scale)) in enumerate(
                zip([axis for row in axes_grid for axis in row], panels)
            ):
                failures_at: dict[int, list[tuple[str, int, int]]] = defaultdict(list)
                for variant, tool, output_class, _label in series:
                    row_context = "NA" if variant.startswith("gfaidx-span") else context
                    rows = sorted(
                        by_variant[(variant, row_context)], key=lambda row: int(row["interval_bp"])
                    )
                    xs, ys, low, high = [], [], [], []
                    for row in rows:
                        failed = int(row.get("failed_locations") or 0)
                        if failed:
                            failures_at[int(row["interval_bp"])].append(
                                (output_class, failed, failed + int(row["locations"]))
                            )
                        xs.append(int(row["interval_bp"]))
                        # NaN breaks the line, so it never bridges a failed size.
                        if failed or row.get(f"{prefix}_mean") is None:
                            ys.append(math.nan)
                            low.append(0.0)
                            high.append(0.0)
                            continue
                        center, (below, above) = error_values(row, prefix, scale)
                        ys.append(center)
                        low.append(below)
                        high.append(above)
                    linestyle, marker = OVERVIEW_TOOL_STYLES[tool]
                    color = OVERVIEW_CLASS_COLORS[output_class]
                    axis.errorbar(
                        xs, ys, yerr=[low, high], color=color, linestyle=linestyle,
                        marker=marker, markersize=5.5, markeredgecolor="white",
                        markeredgewidth=0.9, linewidth=1.6, elinewidth=0.9, capsize=2,
                        zorder=3,
                    )
                axis.set_xscale("log")
                axis.set_yscale("log")
                if failures_at:
                    bottom, top = axis.get_ylim()
                    axis.set_ylim(bottom, top * 6.0)
                    marks = transforms.blended_transform_factory(axis.transData, axis.transAxes)
                    for interval_bp, failed in failures_at.items():
                        for offset, (output_class, count, total) in enumerate(failed):
                            y = 0.955 - 0.085 * offset
                            axis.plot(
                                [interval_bp], [y], transform=marks, marker="x",
                                markersize=7, markeredgewidth=1.8, linestyle="none",
                                color=OVERVIEW_CLASS_COLORS[output_class], clip_on=False,
                                zorder=4,
                            )
                            axis.text(
                                interval_bp, y - 0.045, f"{count}/{total}", transform=marks,
                                ha="center", va="top", fontsize=7,
                                color=OVERVIEW_INK["secondary"],
                            )
                axis.xaxis.set_major_locator(FixedLocator([size for size in sizes]))
                axis.xaxis.set_major_formatter(
                    FuncFormatter(lambda value, _pos: format_bp(value).replace(" ", "\u2009"))
                )
                axis.xaxis.set_minor_formatter(NullFormatter())
                axis.yaxis.set_major_formatter(FuncFormatter(lambda value, _pos: f"{value:g}"))
                axis.yaxis.set_minor_formatter(NullFormatter())
                axis.grid(True, which="major", color=OVERVIEW_GRID, linewidth=0.6)
                axis.set_axisbelow(True)
                for side in ("top", "right"):
                    axis.spines[side].set_visible(False)
                for side in ("left", "bottom"):
                    axis.spines[side].set_color(OVERVIEW_AXIS)
                axis.tick_params(colors=OVERVIEW_INK["muted"], labelcolor=OVERVIEW_INK["secondary"],
                                 labelsize=8)
                axis.set_title(title, loc="left", fontsize=10, color=OVERVIEW_INK["primary"])
                axis.text(-0.13, 1.04, "abcd"[panel_index], transform=axis.transAxes,
                          fontsize=12, fontweight="bold", color=OVERVIEW_INK["primary"])
                axis.set_ylabel(ylabel, color=OVERVIEW_INK["secondary"], fontsize=9)
                if panel_index >= 2:
                    axis.set_xlabel("Requested interval length", color=OVERVIEW_INK["secondary"],
                                    fontsize=9)

            # Legend columns: gbz-base tools on the left, gfaidx on the right, so
            # each row pairs the outputs that share a color.
            gbz_handles, gfaidx_handles = [], []
            for variant, tool, output_class, label in series:
                linestyle, marker = OVERVIEW_TOOL_STYLES[tool]
                (gfaidx_handles if tool == "gfaidx" else gbz_handles).append(
                    Line2D([], [], color=OVERVIEW_CLASS_COLORS[output_class],
                           linestyle=linestyle, marker=marker, markersize=5.5,
                           markeredgecolor="white", linewidth=1.6, label=label)
                )
            if any(int(row.get("failed_locations") or 0) for row in size_rows):
                gbz_handles.append(
                    Line2D([], [], color=OVERVIEW_INK["secondary"], marker="x", linestyle="none",
                           markersize=7, markeredgewidth=1.8,
                           label="NA: failed at k of n loci (not plotted at that size)"),
                )
            # Matplotlib fills legend columns top to bottom; pad the shorter one.
            rows_per_column = max(len(gbz_handles), len(gfaidx_handles))
            for column in (gbz_handles, gfaidx_handles):
                column.extend(
                    Line2D([], [], linestyle="none", label=" ")
                    for _ in range(rows_per_column - len(column))
                )
            figure.legend(
                handles=gbz_handles + gfaidx_handles, loc="lower center", ncol=2,
                frameon=False, fontsize=8.5,
                bbox_to_anchor=(0.5, -0.02 - 0.028 * rows_per_column),
                labelcolor=OVERVIEW_INK["primary"],
            )
            suffix = "thread" if threads == 1 else "threads"
            figure.suptitle(
                f"Interval extraction, context {format_bp(float(context))}, "
                f"gfaidx {threads} {suffix}; mean and 95% CI across loci",
                fontsize=11, color=OVERVIEW_INK["primary"],
            )
            figure.tight_layout()
            save_figure(
                figure, plot_dir / f"overview_time_memory_{context_slug(context)}_{threads}t", formats
            )
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

    raw_rows, failed_runs = read_raw(raw_path)
    failure_rows = build_failure_rows(raw_rows, failed_runs)
    # Every repetition of a location that failed at least once is NA, so no
    # partial location enters an average.
    failed_keys = {location_key(row) for row in failed_runs}
    measured_rows = [row for row in raw_rows if location_key(row) not in failed_keys]
    if not measured_rows:
        raise ValueError(f"{raw_path} contains no location without failures")
    if failure_rows:
        print(
            f"note: {len(failed_runs)} failed execution(s) at {len(failure_rows)} "
            f"location(s) are reported as NA; see {table_dir / 'failures_by_query.tsv'}",
            file=sys.stderr,
        )
    styles = build_variant_styles(ordered_variants(raw_rows + failed_runs))
    locations = build_location_rows(measured_rows)
    parity_rows = build_parity_rows(locations)
    size_rows = build_size_rows(
        locations, parity_rows, failure_rows, args.bootstrap_samples, args.seed
    )
    repetition_rows, cache_rows = build_repetition_rows(
        measured_rows, args.bootstrap_samples, args.seed
    )

    write_table(table_dir / "location_summary.tsv", LOCATION_FIELDS, locations)
    write_table(table_dir / "parity_by_query.tsv", PARITY_FIELDS, parity_rows)
    write_table(table_dir / "size_summary.tsv", SIZE_FIELDS, size_rows)
    write_table(table_dir / "repetition_summary.tsv", REPETITION_FIELDS, repetition_rows)
    write_table(table_dir / "cache_summary.tsv", CACHE_FIELDS, cache_rows)
    write_table(table_dir / "failures_by_query.tsv", FAILURE_FIELDS, failure_rows)

    # Plots leave a variant out at any size where some location failed, rather
    # than averaging over the locations that happened to succeed.
    failed_groups = {failure_group(row) for row in failure_rows}
    plot_sizes = [row for row in size_rows if failure_group(row) not in failed_groups]
    plot_locations = [row for row in locations if failure_group(row) not in failed_groups]
    plot_repetitions = [
        row for row in repetition_rows if failure_group(row) not in failed_groups
    ]

    mismatches = [
        row for row in parity_rows
        if row["node_set_match"] == "no"
        or (row["comparison"] == "reference_only" and row["reference_walk_match"] == "no")
    ]
    if mismatches:
        print(
            f"warning: {len(mismatches)} output(s) differ from gbz-base; "
            f"see {table_dir / 'parity_by_query.tsv'}",
            file=sys.stderr,
        )

    configure_matplotlib()
    import matplotlib

    matplotlib.use("Agg")
    plot_dir.mkdir(parents=True, exist_ok=True)
    for comparison in sorted({str(row["comparison"]) for row in plot_sizes}, key=comparison_sort_key):
        plot_comparison(
            comparison, plot_locations, plot_sizes, plot_repetitions, cache_rows,
            plot_dir, formats, styles,
        )
    plot_overview(size_rows, plot_dir, formats)

    print(f"Wrote summary tables to {table_dir}")
    print(f"Wrote plots to {plot_dir}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError) as error:
        print(f"summarize_gfaidx_gbz_intervals.py: {error}", file=sys.stderr)
        raise SystemExit(1)

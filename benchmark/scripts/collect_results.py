#!/usr/bin/env python3
"""Collect benchmark metrics into final TSV tables.

This walks the metrics/ tree written by measure.py and joins each record with
the GFA statistics of the output it produced (when there is one). The layout is
parsed from the file path, so adding a tool or a track needs no change here:

    metrics/index/<tool>/<graph>/<step>.json
    metrics/queries/node_steps/<tool>/<graph>/<query>/context_<K>/threads_<T>.json
    metrics/queries/node_bases/<tool>/<graph>/<query>/context_<BP>/threads_<T>.json
    metrics/queries/region/<tool>/<graph>/<query>/threads_<T>/gap_<G>/iterations_<I>.json

Four tables are produced:

    index_metrics.tsv   one row per indexing command
    index_sizes.tsv     one row per on-disk index file
    query_metrics.tsv   one row per query command
    query_comparison.tsv  one row per query, one column group per tool
"""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path

# Which on-disk files make up the query-ready index for each tool. Sizes are
# reported per file and summed per tool so the footprint comparison is explicit
# about what it counts.
INDEX_GLOBS = {
    # gfaidx needs the indexed BGZF graph plus all generated sidecars.
    "gfaidx": ["*.gfa.gz", "*.gfa.gz.*"],
    # VG queries the XG directly; source GFA and setup files are excluded.
    "vg": ["*.xg"],
    # Timed ODGI queries consume only the optimized graph. The unoptimized OG,
    # path index, and step index are supplementary construction artifacts.
    "odgi": ["*.opt.og"],
    # gbz-base queries its database rather than the intermediate GBZ.
    "gbz": ["*.gbz.db"],
}

NODE_TRACKS = ("node_steps", "node_bases")

# Which tools can appear in each track at all. Used to distinguish a cell that is
# structurally not applicable ("NA") from one that is genuinely absent because the
# job has not run or failed (left empty). gbz-base has no step-context query, and
# Exact all-haplotype extraction is a coordinate-only run, while the matched
# gfaidx variants are used only for node-context comparisons.
TRACK_TOOLS = {
    "node_steps": {"vg", "odgi", "gfaidx_matched_vg", "gfaidx_matched_odgi"},
    "node_bases": {"vg", "odgi", "gbz", "gfaidx_matched_vg", "gfaidx_matched_odgi",
                   "gfaidx_matched_gbz"},
    "region": {"vg", "odgi", "gbz", "gfaidx_all_haplotypes"},
}


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results-dir", required=True)
    parser.add_argument(
        "--region-queries",
        default="",
        help="loci.tsv (or legacy region TSV), used to label region rows",
    )
    parser.add_argument("--index-out", required=True)
    parser.add_argument(
        "--graphs",
        nargs="+",
        default=None,
        help="optional graph IDs to include; excludes stale results for other graphs",
    )
    parser.add_argument("--index-sizes-out", required=True)
    parser.add_argument("--query-out", required=True)
    parser.add_argument("--comparison-out", required=True)
    return parser.parse_args()


def load_json(path: Path) -> dict:
    """Read a JSON file, returning {} when it is missing."""
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def write_tsv(path: Path, header: list[str], rows: list[list[object]]) -> None:
    """Write one TSV table with a fixed header."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as out:
        out.write("\t".join(header) + "\n")
        for row in rows:
            out.write("\t".join("" if v is None else str(v) for v in row) + "\n")


def seconds(metrics: dict) -> object:
    """Return wall seconds rounded for reporting."""
    value = metrics.get("wall_seconds")
    return round(float(value), 4) if value is not None else None


def collect_index(results: Path, graphs: set[str] | None = None) -> tuple[list[list[object]], list[list[object]]]:
    """Build the index metrics and index size tables."""
    metric_rows: list[list[object]] = []
    size_rows: list[list[object]] = []

    metrics_root = results / "metrics" / "index"
    for path in sorted(metrics_root.glob("*/*/*.json")):
        tool, graph, step = path.parts[-3], path.parts[-2], path.stem
        m = load_json(path)
        if graphs is not None and graph not in graphs:
            continue
        metric_rows.append([
            graph, tool, step,
            seconds(m), m.get("peak_rss_kb"), m.get("exit_code"),
            m.get("command"),
        ])

    for tool, patterns in INDEX_GLOBS.items():
        tool_dir = results / "indexes" / tool
        if not tool_dir.is_dir():
            continue
        for graph_dir in sorted(p for p in tool_dir.iterdir() if p.is_dir()):
            seen: set[Path] = set()
            if graphs is not None and graph_dir.name not in graphs:
                continue
            for pattern in patterns:
                for f in sorted(graph_dir.glob(pattern)):
                    if f.is_file():
                        seen.add(f)
            for f in sorted(seen):
                size_rows.append([graph_dir.name, tool, f.name, f.stat().st_size])
            total = sum(f.stat().st_size for f in seen)
            size_rows.append([graph_dir.name, tool, "TOTAL", total])

    return metric_rows, size_rows


def query_stats_path(results: Path, track: str, tool: str, graph: str,
                     query: str, param: str, thread_token: str,
                     gap_token: str, iteration_token: str,
                     legacy: bool = False) -> Path:
    """Return the stats JSON that belongs to one query metrics record."""
    base = results / "queries" / track / tool / graph / query
    if track in NODE_TRACKS:
        if legacy:
            return base / f"context_{param}" / "subgraph.stats.json"
        return base / f"context_{param}" / f"threads_{thread_token}" / "subgraph.stats.json"
    if legacy:
        return base / "subgraph.stats.json"
    return (base / f"threads_{thread_token}" / f"gap_{gap_token}" /
            f"iterations_{iteration_token}" / "subgraph.stats.json")


def numeric_token(token: str) -> str:
    """Return a numeric path token or NA for a named/unsupported setting."""
    return token if token.isdigit() else "NA"


def query_variant(track: str, tool: str, gap_token: str,
                  legacy: bool = False) -> str:
    """Name the semantics represented by one query metrics path."""
    if legacy:
        return "legacy"
    if track in NODE_TRACKS:
        return "standard"
    if tool == "gfaidx_all_haplotypes":
        return "no_gap" if gap_token == "none" else "gap"
    if tool == "odgi":
        return "default" if gap_token == "default" else "gap"
    if tool == "gbz":
        return "context0"
    return "standard"


def region_labels(path_value: str) -> dict[tuple[str, str], str]:
    """Map (graph, query_id) to a human-readable interval for region rows."""
    labels: dict[tuple[str, str], str] = {}
    if not path_value:
        return labels
    path = Path(path_value)
    if not path.exists():
        return labels
    with path.open(newline="", encoding="utf-8") as handle:
        lines = [l for l in handle if l.strip() and not l.startswith("#")]
    import csv as _csv
    for row in _csv.DictReader(lines, delimiter="\t"):
        label = (row.get("gbz_interval") or "").strip() or (row.get("gfaidx_region") or "").strip()
        if not label:
            seq_id = (row.get("seq_id") or "").strip()
            start = (row.get("region_start") or "").strip()
            end = (row.get("region_end") or "").strip()
            if seq_id and start and end:
                label = f"{seq_id}:{start}-{end}"
        if label:
            labels[(row["graph"], row["query_id"])] = label
    return labels


def collect_queries(results: Path, labels: dict | None = None,
                    graphs: set[str] | None = None) -> tuple[list[list[object]], list[list[object]]]:
    """Build the per-query table and the per-query tool comparison table."""
    labels = labels or {}
    rows: list[list[object]] = []
    # Parameter columns are part of the comparison key so independent sweep
    # points never overwrite one another in the wide audit table.
    # (graph, track, query, context, threads, gap, iterations, variant)
    pivot: dict[tuple[str, ...], dict[str, tuple]] = defaultdict(dict)

    metrics_root = results / "metrics" / "queries"
    for path in sorted(metrics_root.rglob("*.json")):
        rel = path.relative_to(metrics_root).parts
        track, tool, graph = rel[0], rel[1], rel[2]
        if graphs is not None and graph not in graphs:
            continue
        # Ignore metrics left by an older workflow layout in a reused results
        # directory; only the query variants defined for this track are valid.
        if tool not in TRACK_TOOLS.get(track, set()):
            continue
        if track in NODE_TRACKS:
            query = rel[3]
            if len(rel) >= 6 and rel[4].startswith("context_"):
                param = rel[4].removeprefix("context_")
                thread_token = path.stem.removeprefix("threads_")
                gap_token = "na"
                iteration_token = "na"
                legacy = False
            else:
                # Accept pre-sweep result trees so existing completed runs can
                # still be recollected after this workflow update.
                param = path.stem.removeprefix("context_")
                thread_token = "na"
                gap_token = "na"
                iteration_token = "na"
                legacy = True
        else:
            if len(rel) >= 7 and rel[4].startswith("threads_"):
                query = rel[3]
                thread_token = rel[4].removeprefix("threads_")
                gap_token = rel[5].removeprefix("gap_")
                iteration_token = path.stem.removeprefix("iterations_")
                legacy = False
            else:
                query = path.stem
                thread_token = "na"
                gap_token = "na"
                iteration_token = "na"
                legacy = True
            # Region queries have no context parameter; label them with the
            # interval instead of leaving the column blank.
            param = labels.get((graph, query), "")

        m = load_json(path)
        threads = numeric_token(thread_token)
        gap = numeric_token(gap_token)
        iterations = numeric_token(iteration_token)
        variant = query_variant(track, tool, gap_token, legacy)
        stats = load_json(query_stats_path(
            results, track, tool, graph, query, param,
            thread_token, gap_token, iteration_token, legacy,
        ))
        rows.append([
            graph, track, query, param, tool, threads, gap, iterations, variant,
            seconds(m), m.get("peak_rss_kb"), m.get("exit_code"),
            stats.get("nodes"), stats.get("edges"),
            stats.get("paths"), stats.get("walks"), stats.get("bytes"),
            m.get("command"),
        ])
        pivot[(graph, track, query, param, threads, gap, iterations, variant)][tool] = (
            seconds(m), m.get("peak_rss_kb"), stats.get("nodes"),
        )

    tools = sorted({tool for per_tool in pivot.values() for tool in per_tool})
    header = [
        "graph", "track", "query_id", "context", "threads",
        "haplotype_gap_bp", "merging_iterations", "query_variant",
    ]
    for tool in tools:
        header += [f"{tool}_seconds", f"{tool}_peak_rss_kb", f"{tool}_nodes"]

    comparison: list[list[object]] = []
    for key in sorted(pivot):
        track = key[1]
        row: list[object] = list(key)
        for tool in tools:
            if tool in pivot[key]:
                row += list(pivot[key][tool])
            elif tool not in TRACK_TOOLS.get(track, set()):
                # This tool cannot express this kind of query at all.
                row += ["NA", "NA", "NA"]
            else:
                # Applicable but absent: job pending, skipped, or failed.
                row += [None, None, None]
        comparison.append(row)

    return rows, (header, comparison)


def main() -> int:
    """Write all four result tables."""
    args = parse_args()
    graphs = set(args.graphs) if args.graphs else None
    results = Path(args.results_dir)

    index_rows, size_rows = collect_index(results, graphs)
    write_tsv(
        Path(args.index_out),
        ["graph", "tool", "step", "wall_seconds", "peak_rss_kb", "exit_code", "command"],
        index_rows,
    )
    write_tsv(
        Path(args.index_sizes_out),
        ["graph", "tool", "file", "bytes"],
        size_rows,
    )

    query_rows, (comp_header, comp_rows) = collect_queries(
        results, region_labels(args.region_queries), graphs
    )
    write_tsv(
        Path(args.query_out),
        ["graph", "track", "query_id", "context", "tool", "threads",
         "haplotype_gap_bp", "merging_iterations", "query_variant",
         "wall_seconds", "peak_rss_kb", "exit_code", "out_nodes",
         "out_edges", "out_paths", "out_walks", "out_bytes", "command"],
        query_rows,
    )
    write_tsv(Path(args.comparison_out), comp_header, comp_rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

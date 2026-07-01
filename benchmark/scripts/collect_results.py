#!/usr/bin/env python3
"""Collect benchmark metrics, output sizes, and graph stats into TSV tables."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Iterable


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results-dir", required=True)
    parser.add_argument("--graphs", required=True)
    parser.add_argument("--node-queries", required=True)
    parser.add_argument("--region-queries", required=True)
    parser.add_argument("--contexts", required=True, help="Comma-separated context values")
    parser.add_argument("--index-out", required=True)
    parser.add_argument("--query-out", required=True)
    return parser.parse_args()


def read_tsv(path: Path) -> list[dict[str, str]]:
    """Read a TSV manifest while ignoring blank/comment lines."""
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        lines = [line for line in handle if line.strip() and not line.startswith("#")]
    if not lines:
        return []
    return list(csv.DictReader(lines, delimiter="\t"))


def read_json(path: Path) -> dict[str, object]:
    """Read JSON or return an empty dictionary when the file is absent."""
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def clean(value: str) -> str:
    """Normalize optional TSV cells."""
    return str(value or "").strip()


def is_available(value: str) -> bool:
    """Return whether an optional manifest field describes runnable work."""
    # Empty/NA region cells mean the source tool cannot express this coordinate
    # query for the graph, but the final table should still report the gap.
    return clean(value).lower() not in {"", "na", "n/a", "."}


def parse_bool(value: str, default: bool = False) -> bool:
    """Parse optional yes/no manifest fields with an explicit default."""
    text = clean(value).lower()
    if not text:
        return default
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    raise ValueError(f"invalid boolean value in benchmark TSV: {value!r}")


def file_size(path: Path) -> int:
    """Return file size in bytes, or zero when the file is absent."""
    return path.stat().st_size if path.exists() else 0


def sum_sizes(paths: Iterable[Path]) -> int:
    """Sum sizes of a list of benchmark output files."""
    return sum(file_size(path) for path in paths)


def metric_value(metrics: dict[str, object], key: str) -> str:
    """Return a metric value as a string for TSV output."""
    value = metrics.get(key, "")
    return str(value)


def write_index_table(results: Path, graphs: list[dict[str, str]], out_path: Path) -> None:
    """Write per-phase and combined indexing metrics."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8", newline="") as handle:
        fields = [
            "graph",
            "tool",
            "phase",
            "wall_seconds",
            "peak_rss_kb",
            "exit_code",
            "measurement_mode",
            "output_bytes",
            "output_files",
            "log_path",
            "command",
        ]
        writer = csv.DictWriter(handle, fieldnames=fields, delimiter="\t")
        writer.writeheader()

        for row in graphs:
            graph = row["graph"]
            odgi_phases = ["build"]
            odgi_files = [results / "indexes" / "odgi" / graph / f"{graph}.og"]
            if parse_bool(row.get("odgi_path_indexes", ""), default=True):
                # ODGI path-side indexes are useful and included by default, but
                # pathless graphs can make odgi pathindex abort in some builds.
                odgi_phases.extend(["pathindex", "stepindex"])
                odgi_files.extend([
                    results / "indexes" / "odgi" / graph / f"{graph}.xp",
                    results / "indexes" / "odgi" / graph / f"{graph}.stpidx",
                ])
            specs = {
                "gfaidx": {
                    "phases": ["index_gfa", "index_coordinates"],
                    "files": [
                        results / "indexes" / "gfaidx" / graph / f"{graph}.indexed.gfa.gz",
                        results / "indexes" / "gfaidx" / graph / f"{graph}.indexed.gfa.gz.idx",
                        results / "indexes" / "gfaidx" / graph / f"{graph}.indexed.gfa.gz.ndx",
                        results / "indexes" / "gfaidx" / graph / f"{graph}.indexed.gfa.gz.pdx",
                        results / "indexes" / "gfaidx" / graph / f"{graph}.indexed.gfa.gz.cdx",
                    ],
                },
                "vg": {
                    "phases": ["convert", "xg"],
                    "files": [
                        results / "indexes" / "vg" / graph / f"{graph}.vg",
                        results / "indexes" / "vg" / graph / f"{graph}.xg",
                    ],
                },
                "odgi": {
                    "phases": odgi_phases,
                    "files": odgi_files,
                },
            }

            for tool, spec in specs.items():
                phase_metrics: list[dict[str, object]] = []
                for phase in spec["phases"]:
                    metric_path = results / "metrics" / "index" / tool / graph / f"{phase}.json"
                    log_path = results / "logs" / "index" / tool / graph / f"{phase}.log"
                    metrics = read_json(metric_path)
                    phase_metrics.append(metrics)
                    writer.writerow(
                        {
                            "graph": graph,
                            "tool": tool,
                            "phase": phase,
                            "wall_seconds": metric_value(metrics, "wall_seconds"),
                            "peak_rss_kb": metric_value(metrics, "peak_rss_kb"),
                            "exit_code": metric_value(metrics, "exit_code"),
                            "measurement_mode": metric_value(metrics, "measurement_mode"),
                            "output_bytes": "",
                            "output_files": "",
                            "log_path": str(log_path),
                            "command": metric_value(metrics, "command"),
                        }
                    )

                wall_sum = sum(float(m.get("wall_seconds", 0) or 0) for m in phase_metrics)
                peak_max = max((int(m.get("peak_rss_kb", 0) or 0) for m in phase_metrics), default=0)
                exit_codes = [int(m.get("exit_code", 0) or 0) for m in phase_metrics]
                writer.writerow(
                    {
                        "graph": graph,
                        "tool": tool,
                        "phase": "query_ready_total",
                        "wall_seconds": f"{wall_sum:.6f}",
                        "peak_rss_kb": str(peak_max),
                        "exit_code": str(max(exit_codes) if exit_codes else ""),
                        "measurement_mode": "sum_wall_max_rss",
                        "output_bytes": str(sum_sizes(spec["files"])),
                        "output_files": ",".join(str(path) for path in spec["files"]),
                        "log_path": "",
                        "command": "",
                    }
                )


def write_query_row(
    writer: csv.DictWriter,
    results: Path,
    graph: str,
    query_class: str,
    query_id: str,
    source_tool: str,
    context_or_region: str,
    measured_tool: str,
    output_gfa: Path,
    metrics_path: Path,
    stats_path: Path,
    source_stats_path: Path | None,
    log_path: Path,
) -> None:
    """Write one node or region query result row."""
    metrics = read_json(metrics_path)
    stats = read_json(stats_path)
    source_stats = read_json(source_stats_path) if source_stats_path else {}
    writer.writerow(
        {
            "graph": graph,
            "query_class": query_class,
            "query_id": query_id,
            "source_tool": source_tool,
            "context_or_region": context_or_region,
            "measured_tool": measured_tool,
            "wall_seconds": metric_value(metrics, "wall_seconds"),
            "peak_rss_kb": metric_value(metrics, "peak_rss_kb"),
            "exit_code": metric_value(metrics, "exit_code"),
            "nodes": stats.get("nodes", ""),
            "edges": stats.get("edges", ""),
            "paths": stats.get("paths", ""),
            "walks": stats.get("walks", ""),
            "source_nodes_for_gfaidx_cap": source_stats.get("nodes", ""),
            "output_bytes": str(file_size(output_gfa)),
            "log_path": str(log_path),
            "command": metric_value(metrics, "command"),
        }
    )


def write_na_query_row(
    writer: csv.DictWriter,
    graph: str,
    query_class: str,
    query_id: str,
    source_tool: str,
    context_or_region: str,
    measured_tool: str,
    reason: str,
) -> None:
    """Write a not-applicable query row with NA metric fields."""
    writer.writerow(
        {
            "graph": graph,
            "query_class": query_class,
            "query_id": query_id,
            "source_tool": source_tool,
            "context_or_region": context_or_region or "NA",
            "measured_tool": measured_tool,
            "wall_seconds": "NA",
            "peak_rss_kb": "NA",
            "exit_code": "NA",
            "nodes": "NA",
            "edges": "NA",
            "paths": "NA",
            "walks": "NA",
            "source_nodes_for_gfaidx_cap": "NA",
            "output_bytes": "NA",
            "log_path": "NA",
            "command": f"not_applicable: {reason}",
        }
    )


def write_query_table(
    results: Path,
    node_queries: list[dict[str, str]],
    region_queries: list[dict[str, str]],
    contexts: list[str],
    out_path: Path,
) -> None:
    """Write all query benchmark metrics."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8", newline="") as handle:
        fields = [
            "graph",
            "query_class",
            "query_id",
            "source_tool",
            "context_or_region",
            "measured_tool",
            "wall_seconds",
            "peak_rss_kb",
            "exit_code",
            "nodes",
            "edges",
            "paths",
            "walks",
            "source_nodes_for_gfaidx_cap",
            "output_bytes",
            "log_path",
            "command",
        ]
        writer = csv.DictWriter(handle, fieldnames=fields, delimiter="\t")
        writer.writeheader()

        for query in node_queries:
            graph = query["graph"]
            query_id = query["query_id"]
            for context in contexts:
                for source in ("vg", "odgi"):
                    source_gfa = results / "queries" / "node" / source / graph / query_id / f"context_{context}" / "subgraph.gfa"
                    source_stats = source_gfa.with_suffix(".stats.json")
                    source_metrics = results / "metrics" / "queries" / "node" / source / graph / query_id / f"context_{context}.json"
                    source_log = results / "logs" / "queries" / "node" / source / graph / query_id / f"context_{context}.log"
                    write_query_row(writer, results, graph, "node", query_id, source, context, source,
                                    source_gfa, source_metrics, source_stats, None, source_log)

                    gfaidx_gfa = results / "queries" / "node" / f"gfaidx_matched_{source}" / graph / query_id / f"context_{context}" / "subgraph.gfa"
                    gfaidx_stats = gfaidx_gfa.with_suffix(".stats.json")
                    gfaidx_metrics = results / "metrics" / "queries" / "node" / f"gfaidx_matched_{source}" / graph / query_id / f"context_{context}.json"
                    gfaidx_log = results / "logs" / "queries" / "node" / f"gfaidx_matched_{source}" / graph / query_id / f"context_{context}.log"
                    write_query_row(writer, results, graph, "node", query_id, source, context, "gfaidx",
                                    gfaidx_gfa, gfaidx_metrics, gfaidx_stats, source_stats, gfaidx_log)

        for query in region_queries:
            graph = query["graph"]
            query_id = query["query_id"]
            direct_gfa = results / "queries" / "region" / "gfaidx_direct" / graph / query_id / "subgraph.gfa"
            direct_stats = direct_gfa.with_suffix(".stats.json")
            direct_metrics = results / "metrics" / "queries" / "region" / "gfaidx_direct" / graph / f"{query_id}.json"
            direct_log = results / "logs" / "queries" / "region" / "gfaidx_direct" / graph / f"{query_id}.log"
            write_query_row(writer, results, graph, "region", query_id, "gfaidx_direct",
                            query["gfaidx_region"], "gfaidx", direct_gfa, direct_metrics,
                            direct_stats, None, direct_log)

            for source in ("vg", "odgi"):
                region_label = clean(query.get(f"{source}_region", ""))
                if not is_available(region_label):
                    write_na_query_row(
                        writer,
                        graph,
                        "region",
                        query_id,
                        source,
                        "NA",
                        source,
                        f"{source}_region is empty or NA for this graph",
                    )
                    write_na_query_row(
                        writer,
                        graph,
                        "region",
                        query_id,
                        source,
                        query["gfaidx_region"],
                        "gfaidx",
                        f"no {source} region output is available to define a matched --max_nodes cap",
                    )
                    continue

                source_gfa = results / "queries" / "region" / source / graph / query_id / "subgraph.gfa"
                source_stats = source_gfa.with_suffix(".stats.json")
                source_metrics = results / "metrics" / "queries" / "region" / source / graph / f"{query_id}.json"
                source_log = results / "logs" / "queries" / "region" / source / graph / f"{query_id}.log"
                write_query_row(writer, results, graph, "region", query_id, source, region_label, source,
                                source_gfa, source_metrics, source_stats, None, source_log)

                gfaidx_gfa = results / "queries" / "region" / f"gfaidx_matched_{source}" / graph / query_id / "subgraph.gfa"
                gfaidx_stats = gfaidx_gfa.with_suffix(".stats.json")
                gfaidx_metrics = results / "metrics" / "queries" / "region" / f"gfaidx_matched_{source}" / graph / f"{query_id}.json"
                gfaidx_log = results / "logs" / "queries" / "region" / f"gfaidx_matched_{source}" / graph / f"{query_id}.log"
                write_query_row(writer, results, graph, "region", query_id, source, query["gfaidx_region"], "gfaidx",
                                gfaidx_gfa, gfaidx_metrics, gfaidx_stats, source_stats, gfaidx_log)


def main() -> int:
    """Collect index and query result tables."""
    args = parse_args()
    results = Path(args.results_dir)
    graphs = read_tsv(Path(args.graphs))
    node_queries = read_tsv(Path(args.node_queries))
    region_queries = read_tsv(Path(args.region_queries))
    contexts = [value for value in args.contexts.split(",") if value != ""]

    write_index_table(results, graphs, Path(args.index_out))
    write_query_table(results, node_queries, region_queries, contexts, Path(args.query_out))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

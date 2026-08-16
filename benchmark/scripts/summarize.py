#!/usr/bin/env python3
"""Render the benchmark tables as a readable Markdown summary.

Reads results/tables/*.tsv and prints indexing cost, index footprint, and
per-track query timings. Intended for eyeballing a run and for pasting into
notes; the TSVs remain the machine-readable source.
"""

from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from pathlib import Path


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tables", required=True, help="tables directory")
    return parser.parse_args()


def read(path: Path) -> list[dict]:
    """Read one TSV table."""
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle, delimiter="\t"))


def num(value, default=0.0) -> float:
    """Parse a possibly-empty numeric cell."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def gib(kb) -> str:
    """Format a KiB value as GiB."""
    return f"{num(kb) / 1048576:.2f}"


def mb(byts) -> str:
    """Format a byte value as MB."""
    return f"{num(byts) / 1e6:,.1f}"


def table(header: list[str], rows: list[list[str]]) -> str:
    """Render a Markdown table."""
    out = ["| " + " | ".join(header) + " |",
           "| " + " | ".join("---" for _ in header) + " |"]
    for row in rows:
        out.append("| " + " | ".join(str(c) for c in row) + " |")
    return "\n".join(out)


def main() -> int:
    """Print the summary."""
    args = parse_args()
    tables = Path(args.tables)

    print("## Indexing cost\n")
    rows = []
    totals = defaultdict(lambda: [0.0, 0.0])
    for r in read(tables / "index_metrics.tsv"):
        rows.append([r["tool"], r["step"], f"{num(r['wall_seconds']):.1f}",
                     gib(r["peak_rss_kb"]), r["exit_code"]])
        totals[r["tool"]][0] += num(r["wall_seconds"])
        totals[r["tool"]][1] = max(totals[r["tool"]][1], num(r["peak_rss_kb"]))
    print(table(["tool", "step", "seconds", "peak RSS GiB", "exit"], rows))

    print("\n### Total per tool\n")
    sizes = defaultdict(float)
    for r in read(tables / "index_sizes.tsv"):
        if r["file"] == "TOTAL":
            sizes[r["tool"]] = num(r["bytes"])
    rows = [[tool, f"{t[0]:.1f}", f"{t[1] / 1048576:.2f}", mb(sizes.get(tool, 0))]
            for tool, t in sorted(totals.items())]
    print(table(["tool", "total build seconds", "max peak RSS GiB", "index MB"], rows))

    print("\n## Index files\n")
    rows = [[r["tool"], r["file"], mb(r["bytes"])]
            for r in read(tables / "index_sizes.tsv") if r["file"] != "TOTAL"]
    print(table(["tool", "file", "MB"], rows))

    queries = read(tables / "query_metrics.tsv")
    for track in ("node_steps", "node_bases", "region"):
        subset = [r for r in queries if r["track"] == track]
        if not subset:
            continue
        print(f"\n## {track}\n")
        rows = []
        for r in sorted(subset, key=lambda r: (
            r["query_id"], num(r["context"]), r["tool"],
            num(r.get("threads")), num(r.get("haplotype_gap_bp")),
            num(r.get("merging_iterations")), r.get("query_variant", ""),
        )):
            rows.append([r["query_id"], r["context"] or "-", r["tool"],
                         r.get("threads", "NA"),
                         r.get("haplotype_gap_bp", "NA"),
                         r.get("merging_iterations", "NA"),
                         r.get("query_variant", "legacy"),
                         f"{num(r['wall_seconds']):.3f}", gib(r["peak_rss_kb"]),
                         r["out_nodes"] or "-", r["exit_code"]])
        print(table(["query", "context", "tool", "threads", "gap bp",
                     "ODGI iterations", "variant", "seconds", "peak RSS GiB",
                     "out nodes", "exit"], rows))

    print("\n## Mean query cost per tool and track\n")
    agg = defaultdict(lambda: [0.0, 0.0, 0, 0.0])
    for r in queries:
        # Keep every experimental setting separate; averaging across thread or
        # haplotype-gap values would make the summary scientifically ambiguous.
        key = (
            r["track"], r["tool"], r.get("threads", "NA"),
            r.get("haplotype_gap_bp", "NA"),
            r.get("merging_iterations", "NA"),
            r.get("query_variant", "legacy"),
        )
        agg[key][0] += num(r["wall_seconds"])
        agg[key][1] = max(agg[key][1], num(r["peak_rss_kb"]))
        agg[key][2] += 1
        agg[key][3] += num(r["out_nodes"])
    rows = []
    for (track, tool, threads, gap, iterations, variant), (secs, rss, n, nodes) in sorted(agg.items()):
        rows.append([track, tool, threads, gap, iterations, variant, n,
                     f"{secs / n:.3f}", f"{rss / 1048576:.2f}",
                     f"{nodes / n:,.0f}"])
    print(table(["track", "tool", "threads", "gap bp", "ODGI iterations",
                 "variant", "queries", "mean seconds", "max peak RSS GiB",
                 "mean out nodes"], rows))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

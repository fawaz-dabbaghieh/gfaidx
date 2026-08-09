#!/usr/bin/env python3
"""Render a self-contained HTML report from the benchmark tables.

Reads results/tables/*.tsv (plus optional results/dataset_facts.json) and writes
one standalone HTML file: no external CSS, JS, fonts or images, so it can be
opened from disk or emailed. Re-run it after any benchmark run to refresh.

Usage:
    python3 scripts/make_report.py --results results --out report.html
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import html
import json
import platform
import socket
from collections import defaultdict
from pathlib import Path

# Per-tool accent colours, used for both bars and table row keys. Chosen to stay
# distinguishable in light and dark themes and for common colour-vision
# deficiencies (blue / orange / green / purple).
TOOL_COLORS = {
    "gfaidx": "#3b73d9",
    "vg": "#d97706",
    "odgi": "#0f9b6c",
    "gbz": "#8b5cf6",
    "gbz-base": "#8b5cf6",
}
TRACK_TITLES = {
    "node_steps": "Node queries — context in expansion steps",
    "node_bases": "Node queries — context in base pairs",
    "region": "Coordinate-interval queries",
}


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results", required=True, help="results directory")
    parser.add_argument("--out", required=True, help="output HTML file")
    parser.add_argument("--title", default="Genome graph benchmark: gfaidx vs vg, odgi, gbz-base")
    parser.add_argument(
        "--partial",
        action="store_true",
        help="mark the report as generated from an incomplete run",
    )
    return parser.parse_args()


def read_tsv(path: Path) -> list[dict]:
    """Read one TSV table, returning [] when absent."""
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


def esc(value) -> str:
    """HTML-escape a cell value."""
    return html.escape("" if value is None else str(value))


def fmt_secs(value) -> str:
    """Format seconds with a resolution that suits the magnitude."""
    v = num(value)
    if v >= 100:
        return f"{v:,.0f}"
    if v >= 1:
        return f"{v:,.2f}"
    return f"{v:.3f}"


def fmt_bytes(value) -> str:
    """Format a byte count in the largest sensible unit."""
    v = num(value)
    for unit, scale in (("GB", 1e9), ("MB", 1e6), ("kB", 1e3)):
        if v >= scale:
            return f"{v / scale:,.2f} {unit}"
    return f"{v:,.0f} B"


def fmt_gib(kb) -> str:
    """Format a KiB value as GiB."""
    return f"{num(kb) / 1048576:.2f}"


def tool_color(tool: str) -> str:
    """Return the accent colour for a tool or tool variant."""
    base = tool.replace("gfaidx_matched_", "").replace(
        "gfaidx_all_haplotypes", "gfaidx"
    )
    if tool.startswith("gfaidx"):
        base = "gfaidx"
    return TOOL_COLORS.get(base, "#64748b")


def table(header: list[str], rows: list[list[str]], numeric_from: int = 1) -> str:
    """Render one HTML table; columns from numeric_from on are right-aligned."""
    if not rows:
        return '<p class="empty">No data.</p>'
    out = ['<div class="scroll"><table>', "<thead><tr>"]
    for i, h in enumerate(header):
        cls = ' class="num"' if i >= numeric_from else ""
        out.append(f"<th{cls}>{esc(h)}</th>")
    out.append("</tr></thead><tbody>")
    for row in rows:
        out.append("<tr>")
        for i, cell in enumerate(row):
            cls = ' class="num"' if i >= numeric_from else ""
            out.append(f"<td{cls}>{cell if isinstance(cell, str) and cell.startswith('<') else esc(cell)}</td>")
        out.append("</tr>")
    out.append("</tbody></table></div>")
    return "".join(out)


def bar_chart(items: list[tuple[str, float, str]], unit: str, caption: str = "") -> str:
    """Render a horizontal bar chart as inline SVG.

    items is (label, value, colour). Bars scale to the largest value.
    """
    items = [(l, v, c) for l, v, c in items if v is not None]
    if not items:
        return ""
    top = max(v for _, v, _ in items) or 1.0
    row_h, gap, label_w, pad = 26, 8, 190, 8
    val_w = 92
    bar_w = 420
    width = label_w + bar_w + val_w + pad * 2
    height = pad * 2 + len(items) * row_h + max(0, len(items) - 1) * gap

    parts = [
        f'<svg class="chart" viewBox="0 0 {width} {height}" width="100%" '
        f'height="{height}" role="img" aria-label="{esc(caption or "chart")}">'
    ]
    y = pad
    for label, value, color in items:
        w = max(2.0, (value / top) * bar_w)
        parts.append(
            f'<text class="bl" x="{label_w - 8}" y="{y + row_h * 0.68}" '
            f'text-anchor="end">{esc(label)}</text>'
        )
        parts.append(
            f'<rect x="{label_w}" y="{y + 4}" width="{w:.1f}" height="{row_h - 8}" '
            f'rx="3" fill="{color}" fill-opacity="0.85"/>'
        )
        shown = f"{value:,.2f}" if value < 100 else f"{value:,.0f}"
        parts.append(
            f'<text class="bv" x="{label_w + w + 8:.1f}" y="{y + row_h * 0.68}">'
            f"{esc(shown)} {esc(unit)}</text>"
        )
        y += row_h + gap
    parts.append("</svg>")
    body = "".join(parts)
    if caption:
        body += f'<p class="cap">{esc(caption)}</p>'
    return f'<figure class="figure">{body}</figure>'


CSS = """
:root{
  --bg:#ffffff; --fg:#12181f; --muted:#5a6673; --line:#e2e8ee; --soft:#f6f8fa;
  --accent:#3b73d9; --warn-bg:#fff8e6; --warn-line:#e3bd60; --ok:#0f9b6c;
  --code-bg:#f2f5f8;
}
@media (prefers-color-scheme: dark){
  :root{
    --bg:#0f1419; --fg:#e5eaf0; --muted:#9aa7b4; --line:#26303a; --soft:#161d25;
    --accent:#7aa5f0; --warn-bg:#2a2213; --warn-line:#7a6021; --ok:#3ecf9a;
    --code-bg:#171f27;
  }
}
:root[data-theme="dark"]{
  --bg:#0f1419; --fg:#e5eaf0; --muted:#9aa7b4; --line:#26303a; --soft:#161d25;
  --accent:#7aa5f0; --warn-bg:#2a2213; --warn-line:#7a6021; --ok:#3ecf9a;
  --code-bg:#171f27;
}
:root[data-theme="light"]{
  --bg:#ffffff; --fg:#12181f; --muted:#5a6673; --line:#e2e8ee; --soft:#f6f8fa;
  --accent:#3b73d9; --warn-bg:#fff8e6; --warn-line:#e3bd60; --ok:#0f9b6c;
  --code-bg:#f2f5f8;
}
*{box-sizing:border-box}
body{
  margin:0; background:var(--bg); color:var(--fg);
  font:16px/1.65 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;
  -webkit-font-smoothing:antialiased;
}
.wrap{max-width:1000px; margin:0 auto; padding:48px 24px 96px}
header.top{border-bottom:1px solid var(--line); padding-bottom:24px; margin-bottom:36px}
h1{font-size:1.85rem; line-height:1.25; margin:0 0 10px; letter-spacing:-.01em}
h2{font-size:1.3rem; margin:52px 0 14px; letter-spacing:-.01em}
h3{font-size:1.05rem; margin:32px 0 10px}
p{margin:0 0 14px}
.sub{color:var(--muted); margin:0}
.meta{display:flex; flex-wrap:wrap; gap:8px 20px; margin-top:16px; font-size:.85rem; color:var(--muted)}
.meta b{color:var(--fg); font-weight:600}
code,kbd{background:var(--code-bg); padding:.12em .38em; border-radius:4px;
  font:.86em ui-monospace,SFMono-Regular,Menlo,Consolas,monospace}
pre{background:var(--code-bg); padding:14px 16px; border-radius:8px; overflow-x:auto;
  border:1px solid var(--line); font:.84rem/1.5 ui-monospace,SFMono-Regular,Menlo,Consolas,monospace}
pre code{background:none; padding:0}
.scroll{overflow-x:auto; margin:0 0 18px; border:1px solid var(--line); border-radius:8px}
table{border-collapse:collapse; width:100%; font-size:.86rem}
th,td{padding:8px 12px; text-align:left; border-bottom:1px solid var(--line); white-space:nowrap}
th{background:var(--soft); font-weight:600; position:sticky; top:0}
tbody tr:last-child td{border-bottom:none}
tbody tr:hover td{background:var(--soft)}
td.num,th.num{text-align:right; font-variant-numeric:tabular-nums}
.dot{display:inline-block; width:8px; height:8px; border-radius:50%; margin-right:7px; vertical-align:baseline}
.figure{margin:0 0 22px}
.chart{display:block; max-width:100%}
.chart .bl{font-size:12px; fill:var(--fg)}
.chart .bv{font-size:12px; fill:var(--muted); font-variant-numeric:tabular-nums}
.cap{font-size:.82rem; color:var(--muted); margin:6px 0 0}
.note{background:var(--warn-bg); border:1px solid var(--warn-line); border-left-width:4px;
  border-radius:6px; padding:14px 16px; margin:0 0 20px}
.note p:last-child{margin-bottom:0}
.note strong{font-weight:650}
.find{border:1px solid var(--line); border-left:4px solid var(--accent);
  border-radius:6px; padding:16px 18px; margin:0 0 18px; background:var(--soft)}
.find h3{margin:0 0 8px; font-size:1rem}
.find p:last-child,.find div:last-child{margin-bottom:0}
.empty{color:var(--muted); font-style:italic}
ul,ol{margin:0 0 14px; padding-left:24px}
li{margin:0 0 6px}
footer{margin-top:64px; padding-top:20px; border-top:1px solid var(--line);
  color:var(--muted); font-size:.84rem}
.pill{display:inline-block; font-size:.75rem; padding:2px 9px; border-radius:99px;
  border:1px solid var(--line); background:var(--soft); color:var(--muted)}
.pill.ok{color:var(--ok); border-color:var(--ok)}
"""


def dataset_section(facts: dict, gfa_bytes: int) -> str:
    """Render the input-graph description."""
    if not facts:
        return ""
    rows = [
        ["Input GFA", "chr22.gfa", fmt_bytes(gfa_bytes) if gfa_bytes else "—"],
        ["Segments (S)", f"{int(facts['nodes']):,}", "—"],
        ["Links (L)", f"{int(facts['edges']):,}", "—"],
        ["Walks (W)", f"{int(facts['walks']):,}", "—"],
        ["Total sequence", f"{int(facts['bp']):,} bp", "—"],
        ["Node ID range", f"{int(facts['min_node']):,} – {int(facts['max_node']):,}",
         f"span {int(facts['max_node']) - int(facts['min_node']) + 1:,}"],
    ]
    return table(["property", "value", "note"], rows, numeric_from=3)


def findings_section() -> str:
    """Render the narrative findings, which shaped the workflow design."""
    return """
<div class="find">
<h3>1. odgi requires <code>-O</code>, so odgi needs a node-ID mapping</h3>
<p><code>odgi extract</code> (both <code>-n</code> and <code>-r</code>) and
<code>odgi pathindex</code> all refuse a graph whose node IDs are not compacted:</p>
<pre><code>[odgi::extract] error: the node IDs are not compacted.
    Please run 'odgi sort' using -O, --optimize to optimize the graph.
error [xp]: Graph to index is not optimized. Please run 'odgi sort' using -O.</code></pre>
<p>chr22 has 2,782,249 nodes with IDs spanning 53,303,057–56,138,482 — a span of
2,835,426, so the ID space has gaps and fails the check. <code>-O</code> renumbers to
<code>1..N</code>. The check is about <em>compactness</em>, not about starting at 1: a toy
graph whose IDs formed a contiguous block passed <code>extract -n</code> without
<code>-O</code>, which is why this had to be confirmed on the real graph.</p>
<p>The workflow therefore builds an optimized graph for odgi and translates each query
node into odgi's ID space through a reference-path position shared by both ID spaces:</p>
<pre><code>odgi position -i graph.opt.og -p 'CHM13#0#chr22:0-51324926,20000007' -v</code></pre>
<p>Verified by round-trip: odgi node <code>2721237</code> maps back to CHM13 chr22 offset
<code>20000007</code> with <code>dist.to.ref=0</code>. The mapping is setup and is excluded
from all query timings.</p>
</div>

<div class="find">
<h3>2. GBZ renumbers nodes by default, because it chops long segments</h3>
<p><code>vg gbwt</code> splits segments longer than <code>--max-node</code> (default 1024 bp).
That changes the node set and renumbers the whole ID space, so
<code>gbz-base query --node &lt;original id&gt;</code> fails outright:</p>
<pre><code>Error: not found: The graph does not contain handle 106809716</code></pre>
<p>Passing <code>--max-node 0</code> disables chopping and preserves the input IDs, letting
gbz-base be queried with the same node IDs as vg and gfaidx. This workflow does that.
The alternative, for a stock GBZ, is <code>vg gbwt --translation FILE</code> and translating
query nodes through that table.</p>
<p><strong>This is a deliberate deviation from default GBZ construction</strong>, made so the
node-seeded comparison is possible at all. It should be stated in the paper's methods.</p>
</div>

<div class="find">
<h3>3. odgi silently drops W-line paths</h3>
<p><code>odgi build</code> on a W-line GFA exits 0 and produces a graph with
<strong>zero paths</strong> rather than reporting an error. The W&nbsp;→&nbsp;P conversion is a
correctness requirement, not a convenience, so it is a measured pipeline step attributed
to odgi.</p>
</div>
"""


def methods_section() -> str:
    """Render how the tools were made comparable, and the caveats."""
    return """
<p>The four tools do not share query semantics, so the workflow runs three tracks and
records what each tool natively supports:</p>
<ul>
<li><strong>node_steps</strong> — context as expansion steps: <code>vg find -n N -c K</code>,
<code>odgi extract -n N -c K</code>. gbz-base has no step-context query.</li>
<li><strong>node_bases</strong> — context as a base-pair budget:
<code>vg find -n N -c BP -L</code>, <code>odgi extract -n N -L BP</code>,
<code>gbz-base query --node N --context BP</code>. This is the only track where all four
tools appear, and it exists because gbz-base expresses context solely in bp.</li>
<li><strong>region</strong> — coordinate intervals: <code>vg find -p</code>,
<code>odgi extract -r</code>, <code>gbz-base query --interval</code>,
<code>gfaidx get_region --all_haplotypes --with_coords</code>.</li>
</ul>
<p>For the two node-context tracks, <code>gfaidx get_subgraph</code> bounds a BFS by node
count rather than steps or bases. For every (query, context, source tool), the source tool
runs first, its output <code>S</code> lines are counted, and gfaidx is run with
<code>--max_nodes</code> set to that count. These rows appear as
<code>gfaidx_matched_&lt;tool&gt;</code>. Region queries are not node-count matched: gfaidx
runs exact path-supported all-haplotype extraction for the requested interval.</p>
<div class="note">
<p><strong>How to read these numbers.</strong> Outputs are <em>not</em> expected to contain
identical node sets: each tool uses its own path-range or exact path-supported extraction
semantics. The comparison is about wall time, peak RSS, index footprint, and output scale;
matched node counts apply only to the node-context tracks, not the region track.</p>
<p>For node queries all tools start from the same <em>locus</em>, but odgi reports its own
compacted node IDs, so node <em>counts</em> are comparable across tools while node
<em>identity</em> is not, without inverting the odgi map.</p>
</div>
<p>Each command is wrapped by <code>scripts/measure.py</code>, which records wall time and
peak RSS sampled across the whole process tree (so helper processes are counted) and
combined with <code>wait4</code> accounting so very short commands still report memory.
Format conversions needed only for counting output records run <em>outside</em> the timed
command. All jobs run with <code>--cores 1</code>: concurrent jobs would make the timing and
memory numbers meaningless.</p>
"""


def build_report(results: Path, title: str, partial: bool = False) -> str:
    """Assemble the full HTML document."""
    tables_dir = results / "tables"
    index_rows = read_tsv(tables_dir / "index_metrics.tsv")
    size_rows = read_tsv(tables_dir / "index_sizes.tsv")
    query_rows = read_tsv(tables_dir / "query_metrics.tsv")
    versions = read_tsv(tables_dir / "tool_versions.tsv")

    facts = {}
    facts_path = results / "dataset_facts.json"
    if facts_path.exists():
        facts = json.loads(facts_path.read_text())
    gfa = Path("/home/user2/fawaz/chr22.gfa")
    gfa_bytes = gfa.stat().st_size if gfa.exists() else 0

    failures = sum(1 for r in index_rows + query_rows if r.get("exit_code") not in ("0", "", None))
    have_data = bool(index_rows) and bool(query_rows)
    complete = have_data and not partial

    out: list[str] = []
    out.append(f"<title>{esc(title)}</title>")
    out.append(f"<style>{CSS}</style>")
    out.append('<div class="wrap">')

    # ---- header -----------------------------------------------------------
    stamp = dt.datetime.now().strftime("%Y-%m-%d %H:%M")
    if failures:
        status = f'<span class="pill">{failures} non-zero exits</span>'
    elif complete:
        status = '<span class="pill ok">complete run, all commands exited 0</span>'
    else:
        status = '<span class="pill">partial run</span>'
    out.append(f"""<header class="top">
<h1>{esc(title)}</h1>
<p class="sub">Indexing and subgraph-extraction cost for four genome-graph tools on an
HPRC chr22 pangenome graph, measured with a Snakemake workflow.</p>
<div class="meta">
<span><b>Generated</b> {esc(stamp)}</span>
<span><b>Host</b> {esc(socket.gethostname())}</span>
<span><b>Platform</b> {esc(platform.machine())} / {esc(platform.system())}</span>
<span><b>Cores used</b> 1</span>
<span>{status}</span>
</div></header>""")

    if not complete:
        out.append("""<div class="note"><p><strong>Partial run.</strong> This report was
generated while the benchmark was still executing, so the query tables below cover only the
jobs finished so far. Re-run <code>scripts/make_report.py</code> after the workflow completes
to refresh every table.</p></div>""")

    # ---- input graph ------------------------------------------------------
    out.append("<h2>Input graph</h2>")
    out.append(dataset_section(facts, gfa_bytes))

    # ---- tool versions ----------------------------------------------------
    out.append("<h2>Tool versions</h2>")
    vrows = []
    for r in versions:
        dot = f'<span class="dot" style="background:{tool_color(r["tool"])}"></span>'
        vrows.append([dot + esc(r["tool"]), esc(r["version_text"])[:150]])
    out.append(table(["tool", "reported version"], vrows, numeric_from=2)
               if vrows else '<p class="empty">Not yet collected.</p>')

    # ---- indexing ---------------------------------------------------------
    out.append("<h2>Indexing cost</h2>")
    if index_rows:
        rows = []
        per_tool_time = defaultdict(float)
        per_tool_rss = defaultdict(float)
        for r in sorted(index_rows, key=lambda r: (r["tool"], r["step"])):
            dot = f'<span class="dot" style="background:{tool_color(r["tool"])}"></span>'
            rows.append([dot + esc(r["tool"]), r["step"], fmt_secs(r["wall_seconds"]),
                         fmt_gib(r["peak_rss_kb"]), r["exit_code"]])
            per_tool_time[r["tool"]] += num(r["wall_seconds"])
            per_tool_rss[r["tool"]] = max(per_tool_rss[r["tool"]], num(r["peak_rss_kb"]))
        out.append(table(["tool", "step", "seconds", "peak RSS (GiB)", "exit"], rows, 2))

        out.append(bar_chart(
            [(t, per_tool_time[t], tool_color(t)) for t in sorted(per_tool_time)],
            "s", "Total index build time per tool (sum of that tool's steps)."))
        out.append(bar_chart(
            [(t, per_tool_rss[t] / 1048576, tool_color(t)) for t in sorted(per_tool_rss)],
            "GiB", "Peak RSS of the heaviest indexing step per tool."))
    else:
        out.append('<p class="empty">Not yet collected.</p>')

    # ---- index footprint --------------------------------------------------
    out.append("<h2>Index footprint on disk</h2>")
    if size_rows:
        totals = [(r["tool"], num(r["bytes"])) for r in size_rows if r["file"] == "TOTAL"]
        out.append(bar_chart([(t, v / 1e6, tool_color(t)) for t, v in sorted(totals)],
                             "MB", "Total index size per tool."))
        out.append("""<div class="note"><p>odgi's total covers both <code>.og</code> and
<code>.opt.og</code>. Only the optimized graph serves queries, so odgi's
<em>query-ready</em> footprint is <code>.opt.og + .xp + .stpidx</code>; the plain
<code>.og</code> is kept for the build-cost comparison. GBZ construction is attributed to
gbz-base, since gbz-base consumes a GBZ and vg is only the available builder.</p></div>""")
        rows = []
        for r in size_rows:
            dot = f'<span class="dot" style="background:{tool_color(r["tool"])}"></span>'
            label = "TOTAL" if r["file"] == "TOTAL" else r["file"]
            rows.append([dot + esc(r["tool"]), label, fmt_bytes(r["bytes"])])
        out.append(table(["tool", "file", "size"], rows, 2))
    else:
        out.append('<p class="empty">Not yet collected.</p>')

    # ---- methods ----------------------------------------------------------
    out.append("<h2>How the tools were made comparable</h2>")
    out.append(methods_section())

    # ---- queries ----------------------------------------------------------
    out.append("<h2>Extraction cost</h2>")
    if query_rows:
        agg = defaultdict(lambda: [0.0, 0.0, 0, 0.0])
        for r in query_rows:
            key = (r["track"], r["tool"])
            agg[key][0] += num(r["wall_seconds"])
            agg[key][1] = max(agg[key][1], num(r["peak_rss_kb"]))
            agg[key][2] += 1
            agg[key][3] += num(r["out_nodes"])

        out.append("<h3>Mean cost per tool and track</h3>")
        rows = []
        for (track, tool), (secs, rss, n, nodes) in sorted(agg.items()):
            dot = f'<span class="dot" style="background:{tool_color(tool)}"></span>'
            rows.append([track, dot + esc(tool), n, fmt_secs(secs / n),
                         fmt_gib(rss), f"{nodes / n:,.0f}"])
        out.append(table(["track", "tool", "queries", "mean seconds",
                          "max peak RSS (GiB)", "mean out nodes"], rows, 2))

        for track in ("node_steps", "node_bases", "region"):
            keys = [(t, tl) for (t, tl) in agg if t == track]
            if not keys:
                continue
            items = [(tl, agg[(track, tl)][0] / agg[(track, tl)][2], tool_color(tl))
                     for (_, tl) in sorted(keys, key=lambda k: k[1])]
            out.append(bar_chart(items, "s", f"Mean query wall time — {track}."))

        for track, heading in TRACK_TITLES.items():
            subset = [r for r in query_rows if r["track"] == track]
            if not subset:
                continue
            out.append(f"<h3>{esc(heading)}</h3>")
            rows = []
            for r in sorted(subset, key=lambda r: (r["query_id"], num(r["context"]), r["tool"])):
                dot = f'<span class="dot" style="background:{tool_color(r["tool"])}"></span>'
                rows.append([r["query_id"], r["context"] or "—", dot + esc(r["tool"]),
                             fmt_secs(r["wall_seconds"]), fmt_gib(r["peak_rss_kb"]),
                             f"{int(num(r['out_nodes'])):,}" if r["out_nodes"] else "—",
                             f"{int(num(r['out_paths'])):,}" if r["out_paths"] else "—",
                             r["exit_code"]])
            out.append(table(["query", "context", "tool", "seconds", "peak RSS (GiB)",
                              "out nodes", "out paths", "exit"], rows, 3))
    else:
        out.append('<p class="empty">Not yet collected.</p>')

    # ---- findings ---------------------------------------------------------
    out.append("<h2>Findings that shaped the workflow</h2>")
    out.append("<p>Three tool behaviours had to be resolved before any comparison was "
               "meaningful. Two of them concern node-ID remapping, which the project "
               "notes flagged as an open question.</p>")
    out.append(findings_section())

    # ---- reproducing ------------------------------------------------------
    out.append("<h2>Reproducing this</h2>")
    out.append("""<pre><code>conda activate gfaidx_bench
cd /home/user2/fawaz/benchmark
snakemake -s Snakefile --cores 1
python3 scripts/make_report.py --results results --out report.html</code></pre>
<p>Context sweeps live in <code>config.yaml</code>; queries live in
<code>loci.tsv</code>, while resolved tool coordinates and node IDs are recorded in
<code>results/maps/resolved_loci.tsv</code>. Every command is stored
verbatim in <code>results/metrics/**.json</code> and copied into
<code>results/tables/*.tsv</code>, which are the machine-readable source for this page.</p>""")

    out.append(f"""<footer>Generated by <code>scripts/make_report.py</code> on
{esc(stamp)}. Tables: <code>results/tables/</code>. Raw metrics:
<code>results/metrics/</code>. Logs: <code>results/logs/</code>.</footer>""")
    out.append("</div>")
    return "\n".join(out)


def main() -> int:
    """Write the HTML report."""
    args = parse_args()
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(
        build_report(Path(args.results), args.title, partial=args.partial),
        encoding="utf-8",
    )
    print(f"wrote {out} ({out.stat().st_size:,} bytes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

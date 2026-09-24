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

# Only these construction steps are needed to produce the files consumed by
# timed queries. Other recorded builds remain visible as supplementary rows.
QUERY_READY_INDEX_STEPS = {
    "gfaidx": {"index_gfa", "index_coordinates"},
    "vg": {"convert_xg"},
    "odgi": {"w_to_p", "build_optimized"},
    "gbz": {"vg_gbwt_gbz", "construct_db"},
}


def query_ready_index_step(row: dict) -> bool:
    """Return whether an indexing row contributes to the query-ready total."""
    return row.get("step") in QUERY_READY_INDEX_STEPS.get(row.get("tool"), set())


def query_ready_index_file(row: dict) -> bool:
    """Return whether a size row names a file read by a timed query."""
    filename = row.get("file", "")
    tool = row.get("tool", "")
    if filename == "TOTAL":
        return False
    if tool == "gfaidx":
        return filename.endswith(".gfa.gz") or ".gfa.gz." in filename
    if tool == "vg":
        return filename.endswith(".xg")
    if tool == "odgi":
        return filename.endswith(".opt.og")
    if tool == "gbz":
        return filename.endswith(".gbz.db")
    return False


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
    """Render available input-graph facts without assuming a dataset name."""
    if not facts:
        return '<p class="empty">Dataset facts were not collected for this run.</p>'

    configured_name = facts.get("gfa_name") or facts.get("input_gfa") or facts.get("gfa")
    input_name = Path(str(configured_name)).name if configured_name else "configured input graph"
    rows = [["Input GFA", input_name, fmt_bytes(gfa_bytes) if gfa_bytes else "—"]]
    for label, key in (
        ("Segments (S)", "nodes"),
        ("Links (L)", "edges"),
        ("Paths (P)", "paths"),
        ("Walks (W)", "walks"),
    ):
        if facts.get(key) is not None:
            rows.append([label, f"{int(facts[key]):,}", "—"])
    if facts.get("bp") is not None:
        rows.append(["Total sequence", f"{int(facts['bp']):,} bp", "—"])
    if facts.get("min_node") is not None and facts.get("max_node") is not None:
        minimum = int(facts["min_node"])
        maximum = int(facts["max_node"])
        rows.append([
            "Node ID range",
            f"{minimum:,} – {maximum:,}",
            f"span {maximum - minimum + 1:,}",
        ])
    return table(["property", "value", "note"], rows, numeric_from=3)


def findings_section() -> str:
    """Render the general tool behaviours that shaped the workflow design."""
    return """
<div class="find">
<h3>1. ODGI optimization requires a node-ID mapping</h3>
<p><code>odgi extract</code> and ODGI's path indexes require compacted node IDs.
The optimized <code>.opt.og</code> used for queries may therefore renumber a source
GFA whose IDs contain gaps. The workflow resolves the original node through VG and
uses the same reference-path position with <code>odgi position</code> to obtain the
corresponding optimized ODGI node. This mapping is setup work and is excluded from
query timings.</p>
</div>

<div class="find">
<h3>2. GBZ segment chopping would change node identity</h3>
<p><code>vg gbwt</code> normally splits segments longer than its
<code>--max-node</code> threshold. That changes the node set and can make an original
GFA node ID unusable in <code>gbz-base query --node</code>. The benchmark passes
<code>--max-node 0</code> so VG preserves input nodes for this cross-tool comparison.
A workflow that keeps default chopping must instead request VG's translation table
and translate each seed node.</p>
</div>

<div class="find">
<h3>3. W and P records need different path setup</h3>
<p>ODGI does not expose source W records as the paths needed by these coordinate
queries, so the W workflow measures W&nbsp;→&nbsp;P conversion before its optimized build.
The P workflow uses source paths directly. It also parses PanSN path names and
promotes the configured reference sample while constructing the GBZ used by
gbz-base.</p>
</div>

<div class="find">
<h3>4. VG node ranges and path regions use different commands</h3>
<p>Node-seeded extraction uses <code>vg find</code>. In affected VG releases,
<code>vg chunk -r NODE:NODE -l BP</code> can pass the step-context sentinel
<code>-1</code> to unsigned expansion code and return the full connected component
instead of a base-pair neighborhood. Region queries remain on
<code>vg chunk -p PATH:START-END -c 0</code>, which selects a path interval and does
not exercise that node-range context path. VG protobuf-to-GFA conversion is included
inside every measured <code>vg find</code> query.</p>
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
<li><strong>region</strong> — coordinate intervals: <code>vg chunk -p -c 0</code>,
<code>odgi extract -r</code>, <code>gbz-base query --interval --context 0</code>,
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
<p>ODGI node queries and all threaded region tools are repeated for every configured
thread count. <code>vg find</code> and gbz-base expose no node-query thread option, so
each source node extraction is measured once while its matched gfaidx query retains the
full thread sweep. Region queries additionally retain separate gfaidx no-gap results, explicit
gfaidx and ODGI gap values, and ODGI merge-iteration counts. gbz-base has no
query-thread or haplotype-gap option and is measured once; its interval context
is explicitly zero to avoid adding graph-neighborhood nodes outside the requested
path interval.</p>
<p>Each command is wrapped by <code>scripts/measure.py</code>, which records wall time and
peak RSS sampled across the whole process tree (so helper processes are counted) and
combined with <code>wait4</code> accounting so very short commands still report memory.
Every timed query produces GFA text. VG node extraction and
<code>vg convert -f</code> run as one measured pipeline; VG region extraction writes GFA
directly. ODGI extraction and <code>odgi view -g</code> likewise run inside one measured
process tree. Snakemake is given enough
cores for the largest thread setting, while the custom <code>benchmark_job=1</code>
resource serializes measured jobs so they do not compete with one another.</p>
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
    # Dataset facts are optional; avoid embedding a machine-specific source
    # path in reports produced for other W- or P-line graphs.
    gfa_bytes = int(facts.get("gfa_bytes", facts.get("bytes", 0))) if facts else 0

    failures = sum(1 for r in index_rows + query_rows if r.get("exit_code") not in ("0", "", None))
    have_data = bool(index_rows) and bool(query_rows)
    complete = have_data and not partial

    # The long-form query table can contain both numeric thread sweeps and
    # threadless gbz-base rows. Report the actual numeric values rather than a
    # fixed core count that becomes incorrect as soon as the config changes.
    query_threads = sorted({
        int(num(r.get("threads")))
        for r in query_rows
        if str(r.get("threads", "")).isdigit()
    })
    thread_summary = ", ".join(str(value) for value in query_threads) or "not recorded"

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
<p class="sub">Indexing and subgraph-extraction cost for four genome-graph tools,
measured with a Snakemake workflow.</p>
<div class="meta">
<span><b>Generated</b> {esc(stamp)}</span>
<span><b>Host</b> {esc(socket.gethostname())}</span>
<span><b>Platform</b> {esc(platform.machine())} / {esc(platform.system())}</span>
<span><b>Query threads</b> {esc(thread_summary)}</span>
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
            included = query_ready_index_step(r)
            rows.append([
                dot + esc(r["tool"]),
                r["step"],
                "yes" if included else "supplementary",
                fmt_secs(r["wall_seconds"]),
                fmt_gib(r["peak_rss_kb"]),
                r["exit_code"],
            ])
            # A failed construction step did not produce a usable index and
            # must not make a misleading query-ready total.
            if included and r.get("exit_code") == "0":
                per_tool_time[r["tool"]] += num(r["wall_seconds"])
                per_tool_rss[r["tool"]] = max(
                    per_tool_rss[r["tool"]], num(r["peak_rss_kb"])
                )
        out.append(table(
            ["tool", "step", "query-ready total", "seconds", "peak RSS (GiB)", "exit"],
            rows,
            3,
        ))
        out.append(bar_chart(
            [(tool, per_tool_time[tool], tool_color(tool)) for tool in sorted(per_tool_time)],
            "s",
            "Query-ready index construction time per tool.",
        ))
        out.append(bar_chart(
            [(tool, per_tool_rss[tool] / 1048576, tool_color(tool))
             for tool in sorted(per_tool_rss)],
            "GiB",
            "Peak RSS among the steps included in each query-ready index.",
        ))
        out.append("""<div class="note"><p>ODGI's W-line total includes the measured
W&nbsp;→&nbsp;P conversion plus <code>odgi build -O</code>; its P-line total includes the
optimized build directly. The unoptimized build and optional <code>.xp</code> and
<code>.stpidx</code> construction remain in the detailed table as supplementary
measurements but are not used by the timed queries.</p></div>""")
    else:
        out.append('<p class="empty">Not yet collected.</p>')

    # ---- index footprint --------------------------------------------------
    out.append("<h2>Index footprint on disk</h2>")
    query_size_rows = [r for r in size_rows if query_ready_index_file(r)]
    if query_size_rows:
        totals = defaultdict(float)
        for r in query_size_rows:
            totals[r["tool"]] += num(r["bytes"])
        out.append(bar_chart(
            [(tool, total / 1e6, tool_color(tool)) for tool, total in sorted(totals.items())],
            "MB",
            "Files read by timed queries; construction intermediates are excluded.",
        ))
        out.append("""<div class="note"><p>The footprint reports the actual query-ready
files: gfaidx's indexed GFA and sidecars, VG's <code>.xg</code>, ODGI's
<code>.opt.og</code>, and gbz-base's <code>.gbz.db</code>. Raw or unoptimized graphs,
ODGI's unused optional path/step indexes, and the intermediate GBZ are excluded.</p></div>""")
        rows = []
        for r in sorted(query_size_rows, key=lambda r: (r["tool"], r["file"])):
            dot = f'<span class="dot" style="background:{tool_color(r["tool"])}"></span>'
            rows.append([dot + esc(r["tool"]), r["file"], fmt_bytes(r["bytes"])])
        for tool, total in sorted(totals.items()):
            dot = f'<span class="dot" style="background:{tool_color(tool)}"></span>'
            rows.append([dot + esc(tool), "TOTAL", fmt_bytes(total)])
        out.append(table(["tool", "query-ready file", "size"], rows, 2))
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
            # Never mix thread counts or region semantics in a mean. Every
            # configuration remains an independently readable report row.
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

        out.append("<h3>Mean cost per tool and query setting</h3>")
        rows = []
        for (track, tool, threads, gap, iterations, variant), (secs, rss, n, nodes) in sorted(agg.items()):
            dot = f'<span class="dot" style="background:{tool_color(tool)}"></span>'
            rows.append([track, dot + esc(tool), threads, gap, iterations, variant,
                         n, fmt_secs(secs / n),
                         fmt_gib(rss), f"{nodes / n:,.0f}"])
        out.append(table(["track", "tool", "threads", "gap (bp)",
                          "ODGI iterations", "variant", "queries", "mean seconds",
                          "max peak RSS (GiB)", "mean out nodes"], rows, 2))

        for track, heading in TRACK_TITLES.items():
            subset = [r for r in query_rows if r["track"] == track]
            if not subset:
                continue
            out.append(f"<h3>{esc(heading)}</h3>")
            rows = []
            for r in sorted(subset, key=lambda r: (
                r["query_id"], num(r["context"]), r["tool"],
                num(r.get("threads")), num(r.get("haplotype_gap_bp")),
                num(r.get("merging_iterations")), r.get("query_variant", ""),
            )):
                dot = f'<span class="dot" style="background:{tool_color(r["tool"])}"></span>'
                rows.append([r["query_id"], r["context"] or "—", dot + esc(r["tool"]),
                             r.get("threads", "NA"),
                             r.get("haplotype_gap_bp", "NA"),
                             r.get("merging_iterations", "NA"),
                             r.get("query_variant", "legacy"),
                             fmt_secs(r["wall_seconds"]), fmt_gib(r["peak_rss_kb"]),
                             f"{int(num(r['out_nodes'])):,}" if r["out_nodes"] else "—",
                             f"{int(num(r['out_paths'])):,}" if r["out_paths"] else "—",
                             r["exit_code"]])
            out.append(table(["query", "context", "tool", "threads", "gap (bp)",
                              "ODGI iterations", "variant", "seconds",
                              "peak RSS (GiB)", "out nodes", "out paths", "exit"], rows, 3))
    else:
        out.append('<p class="empty">Not yet collected.</p>')

    # ---- findings ---------------------------------------------------------
    out.append("<h2>Findings that shaped the workflow</h2>")
    out.append("<p>Four tool behaviours had to be resolved before the measurements "
               "could be interpreted consistently.</p>")
    out.append(findings_section())

    # ---- reproducing ------------------------------------------------------
    out.append("<h2>Reproducing this</h2>")
    out.append("""<pre><code>conda activate gfaidx_bench
# Choose Snakefile.w/config.yaml or Snakefile.p/config.p.yaml.
snakemake -s benchmark/Snakefile.w --configfile benchmark/config.yaml \
  --cores 8 --resources benchmark_job=1
python3 benchmark/scripts/make_report.py --results benchmark/results --out benchmark/results/report.html</code></pre>
<p>Context sweeps live in the selected config file; queries live in its locus
manifest, while resolved tool coordinates and node IDs are recorded in
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

# Shared Snakemake rules benchmarking gfaidx against vg, odgi, and gbz-base.
#
# Started from benchmark/Snakefile in the gfaidx repository and extended with:
#   - gbz-base (vg gbwt -> GBZ, gbz-base construct, node and interval queries)
#   - a second node-query track measuring context in base pairs, so gbz-base
#     (whose context is bp) can be compared against vg chunk -l and odgi -L
#   - a measured W-line -> P-line conversion step in the W workflow, because
#     odgi build accepts a W-line GFA but silently produces zero paths
#
# Query loci are supplied as coordinates on either concrete W records or
# unsuffixed PanSN P paths. Untimed setup resolves both node-ID spaces and all
# tool-specific path coordinates before measured queries begin.

import csv
import shlex
from pathlib import Path


BENCH_DIR = Path(workflow.basedir)
SCRIPTS = BENCH_DIR / "scripts"

if PATH_FORMAT not in {"W", "P"}:
    raise ValueError(f"unsupported benchmark PATH_FORMAT: {PATH_FORMAT!r}")

RESULTS = Path(config.get("results_dir", "results"))
if not RESULTS.is_absolute():
    RESULTS = BENCH_DIR / RESULTS
TABLES = RESULTS / "tables"

DATA_ROOT = Path(config.get("data_root", str(BENCH_DIR.parent)))

PYTHON = config.get("tools", {}).get("python", "python3")
GFAIDX = config.get("tools", {}).get("gfaidx", "gfaidx")
VG = config.get("tools", {}).get("vg", "vg")
ODGI = config.get("tools", {}).get("odgi", "odgi")
GBZ = config.get("tools", {}).get("gbz_base", "gbz-base")

THREADS = int(config.get("threads", 1))
SAMPLE_INTERVAL = str(config.get("sample_interval_seconds", 0.10))

# Two node-query tracks with different context semantics.
STEP_CONTEXTS = [str(v) for v in config.get("node_contexts_steps", [])]
BASE_CONTEXTS = [str(v) for v in config.get("node_contexts_bases", [])]
TRACK_CONTEXTS = {"node_steps": STEP_CONTEXTS, "node_bases": BASE_CONTEXTS}

# Which tools can express each track natively. gbz-base has no step-context
# query, so it only appears in the bp track.
TRACK_SOURCES = {"node_steps": ("vg", "odgi"), "node_bases": ("vg", "odgi", "gbz")}
REGION_SOURCES = ("vg", "odgi", "gbz")


wildcard_constraints:
    graph=r"[^/]+",
    query=r"[^/]+",
    context=r"[^/]+",
    source=r"vg|odgi|gbz",
    track=r"node_steps|node_bases",


def data_path(value):
    """Resolve a manifest path against data_root unless it is absolute."""
    value = str(value or "").strip()
    if not value:
        return ""
    path = Path(value)
    return str(path if path.is_absolute() else DATA_ROOT / path)


def manifest_path(path_value):
    """Resolve a manifest next to the Snakefile, then against data_root."""
    raw = str(path_value)
    path = Path(raw)
    if not path.is_absolute():
        path = BENCH_DIR / raw
        if not path.exists():
            path = Path(data_path(raw))
    return path


def read_tsv(path_value):
    """Read a TSV manifest, skipping blank lines and comments.

    Manifests live next to this Snakefile; data_root is only a fallback.
    """
    path = manifest_path(path_value)
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        lines = [line for line in handle if line.strip() and not line.startswith("#")]
    if not lines:
        return []
    return list(csv.DictReader(lines, delimiter="\t"))


def clean(value):
    """Normalize optional TSV cells and config values."""
    return str(value or "").strip()


def is_available(value):
    """Return whether an optional manifest field describes runnable work."""
    return clean(value).lower() not in {"", "na", "n/a", "."}


def parse_bool(value, default=False):
    """Parse simple yes/no TSV fields with a caller-provided default."""
    text = clean(value).lower()
    if not text:
        return default
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    raise ValueError(f"invalid boolean value in benchmark TSV: {value!r}")


def extra(section, key):
    """Return one optional extra-argument string from config.yaml."""
    return clean(config.get(section, {}).get(key, ""))


def join_options(*parts):
    """Join optional command-line fragments while ignoring empty values."""
    return " ".join(clean(part) for part in parts if clean(part))


# --------------------------------------------------------------------------
# Manifests
# --------------------------------------------------------------------------

GRAPH_ROWS = read_tsv(config.get("graphs_tsv", "graphs.tsv"))
GRAPHS = {row["graph"]: row for row in GRAPH_ROWS}

LOCI_PATH = manifest_path(config.get("loci_tsv", "loci.tsv"))
LOCI = read_tsv(config.get("loci_tsv", "loci.tsv"))
LOCUS_BY_ID = {(row["graph"], row["query_id"]): row for row in LOCI}


def locus_has_node(row):
    """Return whether this locus requests node-neighborhood benchmarks."""
    return is_available(row.get("node_position", ""))


def locus_has_region(row):
    """Return whether this locus requests coordinate-region benchmarks."""
    return is_available(row.get("region_start", "")) or is_available(row.get("region_end", ""))


NODE_QUERIES = [row for row in LOCI if locus_has_node(row)]
REGION_QUERIES = [row for row in LOCI if locus_has_region(row)]


def require_known_graphs():
    """Fail early when query manifests refer to missing graph IDs."""
    known = set(GRAPHS)
    seen = set()
    for row in LOCI:
        if row["graph"] not in known:
            raise ValueError(f"query {row['query_id']} refers to unknown graph {row['graph']}")
        key = (row["graph"], row["query_id"])
        if key in seen:
            raise ValueError(f"duplicate locus key {row['graph']}/{row['query_id']}")
        seen.add(key)
        for field in ("sample", "haplotype", "seq_id"):
            if not clean(row.get(field, "")):
                raise ValueError(f"locus {row['query_id']} has no {field}")
        if not locus_has_node(row) and not locus_has_region(row):
            raise ValueError(f"locus {row['query_id']} defines no node position or region")
        if locus_has_region(row) and not (
            is_available(row.get("region_start", ""))
            and is_available(row.get("region_end", ""))
        ):
            raise ValueError(
                f"locus {row['query_id']} must provide both region_start and region_end"
            )

    if PATH_FORMAT == "P":
        for graph in known:
            graph_loci = [row for row in LOCI if row["graph"] == graph]
            if not graph_loci:
                raise ValueError(f"P graph {graph} has no loci from which to select paths")
            reference = clean(GRAPHS[graph].get("reference_sample", ""))
            for row in graph_loci:
                # The initial P workflow intentionally excludes fragmented or
                # coordinate-bearing path names; every path starts at zero.
                if any(":" in clean(row[field]) for field in ("sample", "haplotype", "seq_id")):
                    raise ValueError(
                        f"P locus {row['query_id']} contains ':' and is not an "
                        "unsuffixed PanSN path"
                    )
                if locus_has_region(row) and reference and clean(row["sample"]) != reference:
                    raise ValueError(
                        f"P region locus {row['query_id']} uses sample {row['sample']}, "
                        f"but graph {graph} reference_sample is {reference}"
                    )


require_known_graphs()


def graph_gfa(wildcards):
    """Return the input GFA for a graph wildcard."""
    return data_path(GRAPHS[wildcards.graph]["gfa"])


def graph_extra(wildcards, key):
    """Fetch one per-graph extra option string."""
    return clean(GRAPHS[wildcards.graph].get(key, ""))


def odgi_path_indexes_enabled(graph):
    """Return whether ODGI .xp/.stpidx side indexes should be built."""
    return parse_bool(GRAPHS[graph].get("odgi_path_indexes", ""), default=True)


def gfaidx_reference_arg(graph):
    """Return an explicit coordinate-track selection for gfaidx indexing."""
    path_names = clean(GRAPHS[graph].get("gfaidx_path_names_file", ""))
    if path_names:
        return f"--path_names_file {shlex.quote(data_path(path_names))}"

    # P paths cannot be selected by the W-only --reference option. The P
    # workflow derives an exact P selection from its loci after index_gfa.
    if PATH_FORMAT == "P":
        return f"--path_names_file {shlex.quote(str(p_path_selection_file(graph)))}"

    reference = clean(GRAPHS[graph].get("gfaidx_reference", ""))
    if not reference:
        samples = sorted({
            clean(row["sample"])
            for row in LOCI
            if row["graph"] == graph
        })
        if len(samples) == 1:
            reference = samples[0]
        elif len(samples) > 1:
            raise ValueError(
                f"graph {graph} has loci from multiple samples; set "
                "gfaidx_path_names_file in graphs.tsv"
            )
    return f"--reference {shlex.quote(reference)}" if reference else ""

# --------------------------------------------------------------------------
# Path helpers
# --------------------------------------------------------------------------

def p_lines_gfa(graph):
    """Return the derived P-line GFA used by odgi."""
    # A native P workflow sends the source graph to every tool unchanged.
    if PATH_FORMAT == "P":
        return data_path(GRAPHS[graph]["gfa"])
    row = GRAPHS[graph]
    supplied = clean(row.get("gfa_p_lines", ""))
    if supplied:
        return data_path(supplied)
    return str(RESULTS / "inputs" / graph / f"{graph}.p_lines.gfa")


def w_to_p_mapping(graph):
    """Return the explicit W-record to converted-P-name mapping."""
    return RESULTS / "inputs" / graph / f"{graph}.w_to_p.tsv"


def p_path_selection_file(graph):
    """Return the generated gfaidx coordinate-path selection for a P graph."""
    return RESULTS / "inputs" / graph / f"{graph}.coordinate_paths.tsv"


def p_requested_path_names(graph):
    """Return the unique unsuffixed PanSN paths requested by P loci."""
    names = {
        "#".join((clean(row["sample"]), clean(row["haplotype"]), clean(row["seq_id"])))
        for row in LOCI
        if row["graph"] == graph
    }
    return sorted(names)


def odgi_input_gfa(wildcards):
    """Return the odgi build input, converting W lines only when needed."""
    return p_lines_gfa(wildcards.graph)


def gfaidx_prefix(graph):
    return RESULTS / "indexes" / "gfaidx" / graph / f"{graph}.indexed.gfa.gz"


def vg_xg(graph):
    return RESULTS / "indexes" / "vg" / graph / f"{graph}.xg"


def odgi_og(graph):
    """Non-optimized odgi graph (original node ID space)."""
    return RESULTS / "indexes" / "odgi" / graph / f"{graph}.og"


def odgi_og_opt(graph):
    """Optimized odgi graph (node IDs compacted to 1..N).

    odgi refuses to run `extract` (both -n and -r) and `pathindex` on a graph
    whose node IDs are not compacted, so every odgi query runs on this graph and
    node IDs must be translated into its ID space. See odgi_node_map.
    """
    return RESULTS / "indexes" / "odgi" / graph / f"{graph}.opt.og"


def vg_path_names_file(graph):
    """Return VG's actual imported path-name listing."""
    return RESULTS / "maps" / "vg" / graph / "path_names.txt"


def odgi_path_names_file(graph):
    """Return ODGI's actual converted P-path name listing."""
    return RESULTS / "maps" / "odgi" / graph / "path_names.txt"


def locus_resolution_file(graph, query):
    """Return one locus's resolved coordinate JSON."""
    return RESULTS / "maps" / "loci" / graph / f"{query}.json"


def original_node_map_file(graph, query):
    """Return the original node ID selected at one W coordinate."""
    return RESULTS / "maps" / "nodes" / "original" / graph / f"{query}.node_id"


def odgi_node_map_file(graph, query):
    """File holding one query's node ID translated into odgi's ID space."""
    return RESULTS / "maps" / "odgi" / graph / f"{query}.node_id"


def gfaidx_coordinate_inputs(graph):
    """Add an explicit path-selection file as a tracked input when provided."""
    inputs = gfaidx_sidecars(graph)
    path_names = clean(GRAPHS[graph].get("gfaidx_path_names_file", ""))
    if path_names:
        inputs["path_names"] = data_path(path_names)
    elif PATH_FORMAT == "P":
        inputs["path_names"] = str(p_path_selection_file(graph))
    return inputs


def reference_sample(graph):
    """Return the reference sample used for VG/GBZ metadata in a P graph."""
    explicit = clean(GRAPHS[graph].get("reference_sample", ""))
    if explicit:
        return explicit
    samples = sorted({clean(row["sample"]) for row in LOCI if row["graph"] == graph})
    if len(samples) == 1:
        return samples[0]
    raise ValueError(
        f"P graph {graph} needs reference_sample in graphs.p.tsv when loci "
        "contain multiple samples"
    )


def vg_convert_options(wildcards):
    """Combine global, per-graph, and P-reference VG conversion options."""
    options = [extra("vg", "convert_extra"), graph_extra(wildcards, "vg_convert_extra")]
    if PATH_FORMAT == "P":
        options.append(f"--ref-sample {shlex.quote(reference_sample(wildcards.graph))}")
    return join_options(*options)


def vg_chunk_options(wildcards):
    """Return VG chunk options, retaining find_extra as a compatibility fallback."""
    configured = extra("vg", "chunk_extra") or extra("vg", "find_extra")
    graph_configured = (
        graph_extra(wildcards, "vg_chunk_extra")
        or graph_extra(wildcards, "vg_find_extra")
    )
    return join_options(configured, graph_configured)


def vg_gbwt_options(wildcards):
    """Combine user-provided GBZ construction options.

    The P-specific PanSN parser and reference promotion are kept in the GBZ
    wrapper because VG performs reference promotion as a second GBWT step.
    """
    return join_options(
        extra("gbz", "gbwt_extra"),
        graph_extra(wildcards, "gbz_gbwt_extra"),
    )


def vg_gbwt_reference_option(wildcards):
    """Return the P reference option, omitted entirely for the W workflow."""
    if PATH_FORMAT != "P":
        return ""
    return f"--reference-sample {shlex.quote(reference_sample(wildcards.graph))}"


def gbz_file(graph):
    return RESULTS / "indexes" / "gbz" / graph / f"{graph}.gbz"


def gbz_db(graph):
    return RESULTS / "indexes" / "gbz" / graph / f"{graph}.gbz.db"


def gfaidx_sidecars(graph):
    """Return every gfaidx sidecar needed by a query."""
    gz = str(gfaidx_prefix(graph))
    return {
        "gz": gz,
        "idx": gz + ".idx",
        "ndx": gz + ".ndx",
        "lnx": gz + ".lnx",
        "pdx": gz + ".pdx",
        "pcx": gz + ".pcx",
    }


def node_gfa(track, tool, graph, query, context):
    """Return the GFA output of one node query."""
    return RESULTS / "queries" / track / tool / graph / query / f"context_{context}" / "subgraph.gfa"


def region_gfa(tool, graph, query):
    """Return the GFA output of one region query."""
    return RESULTS / "queries" / "region" / tool / graph / query / "subgraph.gfa"


def node_metrics(track, tool, graph, query, context):
    return RESULTS / "metrics" / "queries" / track / tool / graph / query / f"context_{context}.json"


def region_metrics(tool, graph, query):
    return RESULTS / "metrics" / "queries" / "region" / tool / graph / f"{query}.json"


def source_region_available(row, source):
    """Every concrete W or P locus is resolved for each region-capable tool."""
    return locus_has_region(row)


def locus_resolver_args(wildcards):
    """Quote one loci.tsv row for the selected W/P resolver."""
    row = LOCUS_BY_ID[(wildcards.graph, wildcards.query)]
    args = [
        "--graph", wildcards.graph,
        "--query-id", wildcards.query,
        "--sample", clean(row["sample"]),
        "--haplotype", clean(row["haplotype"]),
        "--seq-id", clean(row["seq_id"]),
        "--notes", clean(row.get("notes", "")),
    ]
    if locus_has_node(row):
        args += ["--node-position", clean(row["node_position"])]
    if locus_has_region(row):
        args += [
            "--region-start", clean(row["region_start"]),
            "--region-end", clean(row["region_end"]),
        ]
    return shlex.join(args)


def gbz_path_metadata_file(graph):
    """Return VG's metadata listing for the GBZ used by gbz-base."""
    return RESULTS / "maps" / "gbz" / graph / "path_metadata.tsv"


def locus_resolution_inputs(graph):
    """Return the path metadata required by the selected W/P resolver."""
    inputs = {
        "vg_paths": str(vg_path_names_file(graph)),
        "odgi_paths": str(odgi_path_names_file(graph)),
    }
    if PATH_FORMAT == "W":
        inputs["mapping"] = str(w_to_p_mapping(graph))
    else:
        inputs["gbz_metadata"] = str(gbz_path_metadata_file(graph))
    return inputs


def locus_resolver_command(wildcards):
    """Build the mode-specific resolver command with tracked metadata paths."""
    common = (
        f"--vg-paths {shlex.quote(str(vg_path_names_file(wildcards.graph)))} "
        f"--odgi-paths {shlex.quote(str(odgi_path_names_file(wildcards.graph)))}"
    )
    if PATH_FORMAT == "W":
        return (
            f"{shlex.quote(str(SCRIPTS / 'resolve_locus.py'))} "
            f"--mapping {shlex.quote(str(w_to_p_mapping(wildcards.graph)))} {common}"
        )
    return (
        f"{shlex.quote(str(SCRIPTS / 'resolve_p_locus.py'))} {common} "
        f"--gbz-metadata {shlex.quote(str(gbz_path_metadata_file(wildcards.graph)))}"
    )


def gfaidx_region_reference_arg(wildcards):
    """Restrict W queries by sample; P regions already name the exact path."""
    if PATH_FORMAT == "P":
        return ""
    row = LOCUS_BY_ID[(wildcards.graph, wildcards.query)]
    return f"--reference {shlex.quote(clean(row['sample']))}"


# --------------------------------------------------------------------------
# Target collection
# --------------------------------------------------------------------------

TARGETS = []

for graph in GRAPHS:
    gz = str(gfaidx_prefix(graph))
    TARGETS += [
        RESULTS / "metrics" / "index" / "gfaidx" / graph / "index_gfa.json",
        RESULTS / "metrics" / "index" / "gfaidx" / graph / "index_coordinates.json",
        RESULTS / "metrics" / "index" / "vg" / graph / "convert_xg.json",
        RESULTS / "metrics" / "index" / "odgi" / graph / "build.json",
        RESULTS / "metrics" / "index" / "odgi" / graph / "build_optimized.json",
        RESULTS / "metrics" / "index" / "gbz" / graph / "vg_gbwt_gbz.json",
        RESULTS / "metrics" / "index" / "gbz" / graph / "construct_db.json",
        Path(gz + ".cdx"),
        vg_xg(graph),
        odgi_og(graph),
        gbz_db(graph),
    ]
    if PATH_FORMAT == "W" and not clean(GRAPHS[graph].get("gfa_p_lines", "")):
        TARGETS.append(RESULTS / "metrics" / "index" / "odgi" / graph / "w_to_p.json")
    if PATH_FORMAT == "P":
        TARGETS.append(gbz_path_metadata_file(graph))
        if not clean(GRAPHS[graph].get("gfaidx_path_names_file", "")):
            TARGETS.append(p_path_selection_file(graph))
    if odgi_path_indexes_enabled(graph):
        TARGETS += [
            RESULTS / "metrics" / "index" / "odgi" / graph / "pathindex.json",
            RESULTS / "metrics" / "index" / "odgi" / graph / "stepindex.json",
        ]

for row in NODE_QUERIES:
    graph, query = row["graph"], row["query_id"]
    TARGETS += [
        locus_resolution_file(graph, query),
        original_node_map_file(graph, query),
        odgi_node_map_file(graph, query),
    ]
    for track, contexts in TRACK_CONTEXTS.items():
        for context in contexts:
            for source in TRACK_SOURCES[track]:
                TARGETS += [
                    node_metrics(track, source, graph, query, context),
                    node_gfa(track, source, graph, query, context).with_suffix(".stats.json"),
                    node_metrics(track, f"gfaidx_matched_{source}", graph, query, context),
                    node_gfa(track, f"gfaidx_matched_{source}", graph, query, context).with_suffix(".stats.json"),
                ]

for row in REGION_QUERIES:
    graph, query = row["graph"], row["query_id"]
    TARGETS.append(locus_resolution_file(graph, query))
    TARGETS += [
        region_metrics("gfaidx_all_haplotypes", graph, query),
        region_gfa("gfaidx_all_haplotypes", graph, query).with_suffix(".stats.json"),
    ]
    for source in REGION_SOURCES:
        if not source_region_available(row, source):
            continue
        TARGETS += [
            region_metrics(source, graph, query),
            region_gfa(source, graph, query).with_suffix(".stats.json"),
        ]

# Deduplicate setup artifacts when one locus requests both node and region work.
TARGETS = list(dict.fromkeys(TARGETS))
TARGETS.append(RESULTS / "maps" / "resolved_loci.tsv")


FINAL_TARGETS = [
    TABLES / "tool_versions.tsv",
    TABLES / "index_metrics.tsv",
    TABLES / "index_sizes.tsv",
    TABLES / "query_metrics.tsv",
    TABLES / "query_comparison.tsv",
    RESULTS / "report.html",
]


localrules: tool_versions, collect_loci, collect_results, html_report


rule html_report:
    # Self-contained HTML summary of the finished run.
    input:
        versions=TABLES / "tool_versions.tsv",
        index=TABLES / "index_metrics.tsv",
        sizes=TABLES / "index_sizes.tsv",
        query=TABLES / "query_metrics.tsv",
    output:
        html=RESULTS / "report.html"
    shell:
        """
        {PYTHON} {SCRIPTS}/make_report.py \
          --results {RESULTS} --out {output.html}
        """


rule tool_versions:
    output:
        TABLES / "tool_versions.tsv"
    shell:
        """
        {PYTHON} {SCRIPTS}/tool_versions.py \
          --out {output} \
          --gfaidx {GFAIDX} --vg {VG} --odgi {ODGI} --gbz-base {GBZ}
        """


rule collect_results:
    input:
        TARGETS
    output:
        index=TABLES / "index_metrics.tsv",
        sizes=TABLES / "index_sizes.tsv",
        query=TABLES / "query_metrics.tsv",
        comparison=TABLES / "query_comparison.tsv",
    params:
        loci=lambda w: str(LOCI_PATH),
        graphs=lambda w: " ".join(shlex.quote(graph) for graph in GRAPHS),
    shell:
        """
        {PYTHON} {SCRIPTS}/collect_results.py \
          --results-dir {RESULTS} \
          --region-queries {params.loci} \
          --graphs {params.graphs} \
          --index-out {output.index} \
          --index-sizes-out {output.sizes} \
          --query-out {output.query} \
          --comparison-out {output.comparison}
        """


rule collect_loci:
    # Keep the automatically derived names and both node-ID spaces in one table
    # so every benchmark query can be audited after the run.
    input:
        resolutions=[str(locus_resolution_file(row["graph"], row["query_id"])) for row in LOCI],
        original_nodes=[str(original_node_map_file(row["graph"], row["query_id"])) for row in NODE_QUERIES],
        odgi_nodes=[str(odgi_node_map_file(row["graph"], row["query_id"])) for row in NODE_QUERIES],
    output:
        tsv=RESULTS / "maps" / "resolved_loci.tsv"
    params:
        graphs=lambda w: " ".join(shlex.quote(graph) for graph in GRAPHS),
    shell:
        """
        {PYTHON} {SCRIPTS}/collect_loci.py \
          --results-dir {RESULTS} --graphs {params.graphs} --out {output.tsv}
        """


# --------------------------------------------------------------------------
# Input preparation
# --------------------------------------------------------------------------

rule w_to_p:
    # odgi build accepts a W-line GFA but emits a graph with zero paths, so the
    # conversion is required rather than cosmetic. The Python converter handles
    # chromosome-scale walks in step chunks, and this measured preparation cost
    # remains attributed to odgi.
    input:
        gfa=graph_gfa,
        converter=str(SCRIPTS / "w_to_p.py")
    output:
        gfa=f"{RESULTS}/inputs/{{graph}}/{{graph}}.p_lines.gfa",
        mapping=f"{RESULTS}/inputs/{{graph}}/{{graph}}.w_to_p.tsv",
        metrics=f"{RESULTS}/metrics/index/odgi/{{graph}}/w_to_p.json",
        log=f"{RESULTS}/logs/index/odgi/{{graph}}/w_to_p.log",
    shell:
        """
        {PYTHON} {SCRIPTS}/measure.py \
          --metrics {output.metrics} --log {output.log} \
          --stdout {output.gfa} --sample-interval {SAMPLE_INTERVAL} \
          -- {PYTHON} {input.converter:q} {input.gfa:q} \
             --mapping-out {output.mapping:q}
        """


# --------------------------------------------------------------------------
# Indexing
# --------------------------------------------------------------------------

rule gfaidx_index:
    input:
        gfa=graph_gfa
    output:
        gz=f"{RESULTS}/indexes/gfaidx/{{graph}}/{{graph}}.indexed.gfa.gz",
        idx=f"{RESULTS}/indexes/gfaidx/{{graph}}/{{graph}}.indexed.gfa.gz.idx",
        ndx=f"{RESULTS}/indexes/gfaidx/{{graph}}/{{graph}}.indexed.gfa.gz.ndx",
        lnx=f"{RESULTS}/indexes/gfaidx/{{graph}}/{{graph}}.indexed.gfa.gz.lnx",
        pdx=f"{RESULTS}/indexes/gfaidx/{{graph}}/{{graph}}.indexed.gfa.gz.pdx",
        pcx=f"{RESULTS}/indexes/gfaidx/{{graph}}/{{graph}}.indexed.gfa.gz.pcx",
        metrics=f"{RESULTS}/metrics/index/gfaidx/{{graph}}/index_gfa.json",
        log=f"{RESULTS}/logs/index/gfaidx/{{graph}}/index_gfa.log",
    threads: THREADS
    params:
        extra=lambda w: join_options(extra("gfaidx", "index_extra"), graph_extra(w, "gfaidx_index_extra"))
    shell:
        """
        {PYTHON} {SCRIPTS}/measure.py \
          --metrics {output.metrics} --log {output.log} \
          --sample-interval {SAMPLE_INTERVAL} \
          -- {GFAIDX} index_gfa {input.gfa} {output.gz} {params.extra}
        """


rule p_gfaidx_path_selection:
    # Derive the exact P rows requested by loci from the completed path index.
    # This setup is unmeasured and is only reachable from the P workflow.
    input:
        gz=f"{RESULTS}/indexes/gfaidx/{{graph}}/{{graph}}.indexed.gfa.gz",
        pdx=f"{RESULTS}/indexes/gfaidx/{{graph}}/{{graph}}.indexed.gfa.gz.pdx",
    output:
        paths=f"{RESULTS}/inputs/{{graph}}/{{graph}}.coordinate_paths.tsv"
    params:
        requested=lambda w: shlex.join(p_requested_path_names(w.graph))
    shell:
        """
        {GFAIDX} get_path {input.gz:q} --pdx {input.pdx:q} --print_path_names |
          {PYTHON} {SCRIPTS}/select_p_paths.py \
            --out {output.paths:q} --requested {params.requested}
        """


rule gfaidx_coordinates:
    input:
        unpack(lambda w: gfaidx_coordinate_inputs(w.graph))
    output:
        cdx=f"{RESULTS}/indexes/gfaidx/{{graph}}/{{graph}}.indexed.gfa.gz.cdx",
        metrics=f"{RESULTS}/metrics/index/gfaidx/{{graph}}/index_coordinates.json",
        log=f"{RESULTS}/logs/index/gfaidx/{{graph}}/index_coordinates.log",
    threads: THREADS
    params:
        reference=lambda w: gfaidx_reference_arg(w.graph),
        extra=lambda w: join_options(extra("gfaidx", "coordinate_extra"), graph_extra(w, "gfaidx_coord_extra"))
    shell:
        """
        {PYTHON} {SCRIPTS}/measure.py \
          --metrics {output.metrics} --log {output.log} \
          --sample-interval {SAMPLE_INTERVAL} \
          -- {GFAIDX} index_coordinates {input.gz} {output.cdx} \
             --ndx {input.ndx} --pdx {input.pdx} {params.reference} {params.extra}
        """


rule vg_convert_xg:
    input:
        gfa=graph_gfa
    output:
        xg=f"{RESULTS}/indexes/vg/{{graph}}/{{graph}}.xg",
        metrics=f"{RESULTS}/metrics/index/vg/{{graph}}/convert_xg.json",
        log=f"{RESULTS}/logs/index/vg/{{graph}}/convert_xg.log",
    threads: THREADS
    params:
        extra=vg_convert_options
    shell:
        """
        {PYTHON} {SCRIPTS}/measure.py \
          --metrics {output.metrics} --log {output.log} \
          --stdout {output.xg} --sample-interval {SAMPLE_INTERVAL} \
          -- {VG} convert -g -x -t {threads} {input.gfa} {params.extra}
        """


rule vg_gbwt_gbz:
    # GBZ construction is attributed to the gbz-base pipeline because gbz-base
    # consumes a GBZ; vg is only the available builder.
    input:
        gfa=graph_gfa
    output:
        gbz=f"{RESULTS}/indexes/gbz/{{graph}}/{{graph}}.gbz",
        metrics=f"{RESULTS}/metrics/index/gbz/{{graph}}/vg_gbwt_gbz.json",
        log=f"{RESULTS}/logs/index/gbz/{{graph}}/vg_gbwt_gbz.log",
    threads: THREADS
    params:
        extra=vg_gbwt_options,
        path_format=PATH_FORMAT,
        reference=vg_gbwt_reference_option,
    shell:
        """
        {PYTHON} {SCRIPTS}/measure.py \
          --metrics {output.metrics} --log {output.log} \
          --sample-interval {SAMPLE_INTERVAL} \
          -- {PYTHON} {SCRIPTS}/vg_gbwt_gbz.py \
             --vg {VG} --input {input.gfa:q} --output {output.gbz:q} \
             --path-format {params.path_format} \
             {params.reference} -- {params.extra}
        """


rule gbz_construct_db:
    input:
        gbz=lambda w: gbz_file(w.graph)
    output:
        db=f"{RESULTS}/indexes/gbz/{{graph}}/{{graph}}.gbz.db",
        metrics=f"{RESULTS}/metrics/index/gbz/{{graph}}/construct_db.json",
        log=f"{RESULTS}/logs/index/gbz/{{graph}}/construct_db.log",
    threads: THREADS
    params:
        extra=lambda w: extra("gbz", "construct_extra")
    shell:
        """
        rm -f {output.db}
        {PYTHON} {SCRIPTS}/measure.py \
          --metrics {output.metrics} --log {output.log} \
          --sample-interval {SAMPLE_INTERVAL} \
          -- {GBZ} construct --output {output.db} {input.gbz} {params.extra}
        """


rule odgi_build:
    # No -O here on purpose: -O compacts node IDs to 1..N and breaks
    # `odgi extract -n <original id>`.
    input:
        gfa=odgi_input_gfa
    output:
        og=f"{RESULTS}/indexes/odgi/{{graph}}/{{graph}}.og",
        metrics=f"{RESULTS}/metrics/index/odgi/{{graph}}/build.json",
        log=f"{RESULTS}/logs/index/odgi/{{graph}}/build.log",
    threads: THREADS
    params:
        extra=lambda w: join_options(extra("odgi", "build_extra"), graph_extra(w, "odgi_build_extra"))
    shell:
        """
        {PYTHON} {SCRIPTS}/measure.py \
          --metrics {output.metrics} --log {output.log} \
          --sample-interval {SAMPLE_INTERVAL} \
          -- {ODGI} build -g {input.gfa} -o {output.og} -t {threads} {params.extra}
        """


rule odgi_build_optimized:
    # -O compacts node IDs to 1..N. Required because odgi extract and
    # odgi pathindex both refuse a graph with a non-compacted ID space, which is
    # what any real HPRC subgraph has (chr22 here: IDs 53303057..56138482 with
    # gaps). The original-ID graph is still built, for the record and for the
    # build-cost comparison.
    input:
        gfa=odgi_input_gfa
    output:
        og=f"{RESULTS}/indexes/odgi/{{graph}}/{{graph}}.opt.og",
        metrics=f"{RESULTS}/metrics/index/odgi/{{graph}}/build_optimized.json",
        log=f"{RESULTS}/logs/index/odgi/{{graph}}/build_optimized.log",
    threads: THREADS
    params:
        extra=lambda w: join_options(extra("odgi", "build_extra"), graph_extra(w, "odgi_build_extra"))
    shell:
        """
        {PYTHON} {SCRIPTS}/measure.py \
          --metrics {output.metrics} --log {output.log} \
          --sample-interval {SAMPLE_INTERVAL} \
          -- {ODGI} build -g {input.gfa} -o {output.og} -O -t {threads} {params.extra}
        """


rule vg_path_names:
    # Record the names VG actually imported. In particular, VG gives nonzero W
    # subranges a bracket suffix that differs from ODGI's converted P name.
    input:
        xg=lambda w: vg_xg(w.graph)
    output:
        paths=f"{RESULTS}/maps/vg/{{graph}}/path_names.txt",
        log=f"{RESULTS}/logs/maps/vg/{{graph}}/path_names.log",
    shell:
        """
        {VG} paths -x {input.xg} -L > {output.paths} 2> {output.log}
        """


rule odgi_path_names:
    # Verify the exact P names present after W conversion and ODGI optimization.
    input:
        og=lambda w: odgi_og_opt(w.graph)
    output:
        paths=f"{RESULTS}/maps/odgi/{{graph}}/path_names.txt",
        log=f"{RESULTS}/logs/maps/odgi/{{graph}}/path_names.log",
    shell:
        """
        {ODGI} paths -i {input.og} -L > {output.paths} 2> {output.log}
        """


rule gbz_path_metadata:
    # GBZ-base coordinate queries depend on VG classifying the requested P
    # sample as REFERENCE. Keep the actual metadata as an auditable setup file.
    input:
        gbz=lambda w: gbz_file(w.graph)
    output:
        metadata=f"{RESULTS}/maps/gbz/{{graph}}/path_metadata.tsv",
        log=f"{RESULTS}/logs/maps/gbz/{{graph}}/path_metadata.log",
    shell:
        """
        {VG} paths -M -x {input.gbz} > {output.metadata} 2> {output.log}
        """


rule resolve_locus:
    # Resolve either an absolute W interval or a zero-based PanSN P interval
    # into the exact coordinate syntax accepted by every benchmarked tool.
    input:
        unpack(lambda w: locus_resolution_inputs(w.graph))
    output:
        json=f"{RESULTS}/maps/loci/{{graph}}/{{query}}.json"
    params:
        resolver=locus_resolver_command,
        args=locus_resolver_args,
    shell:
        """
        {PYTHON} {params.resolver} {params.args} --out {output.json:q}
        """


rule original_node_map:
    # Resolve the original graph node at the selected W/P coordinate through
    # VG, which preserves numeric GFA node IDs. This setup is intentionally not
    # measured as part of any extraction query.
    input:
        xg=lambda w: vg_xg(w.graph),
        locus=lambda w: locus_resolution_file(w.graph, w.query),
    output:
        node_id=f"{RESULTS}/maps/nodes/original/{{graph}}/{{query}}.node_id",
        gfa=f"{RESULTS}/maps/nodes/original/{{graph}}/{{query}}.position.gfa",
        log=f"{RESULTS}/logs/maps/nodes/original/{{graph}}/{{query}}.log",
    threads: THREADS
    shell:
        """
        region="$({PYTHON} {SCRIPTS}/json_value.py \
          --input {input.locus:q} --key vg_node_region)"
        {VG} chunk -x {input.xg} -p "$region" -c 0 -O gfa -t {threads} \
          > {output.gfa} 2> {output.log}
        {PYTHON} {SCRIPTS}/parse_seed_node.py \
          --gfa {output.gfa} --out {output.node_id} 2>> {output.log}
        """


rule odgi_node_map:
    # Translate the same path coordinate into ODGI's compacted node-ID space.
    # This is setup rather than measured query work.
    input:
        og=lambda w: odgi_og_opt(w.graph),
        locus=lambda w: locus_resolution_file(w.graph, w.query),
    output:
        node_id=f"{RESULTS}/maps/odgi/{{graph}}/{{query}}.node_id",
        position=f"{RESULTS}/maps/odgi/{{graph}}/{{query}}.position.tsv",
        log=f"{RESULTS}/logs/maps/odgi/{{graph}}/{{query}}.log",
    shell:
        """
        locus="$({PYTHON} {SCRIPTS}/json_value.py \
          --input {input.locus:q} --key odgi_node_position)"
        {ODGI} position -i {input.og} -p "$locus" -v \
          > {output.position} 2> {output.log}
        {PYTHON} {SCRIPTS}/parse_odgi_position.py \
          --input {output.position} --out {output.node_id} 2>> {output.log}
        """


rule odgi_pathindex:
    input:
        og=lambda w: odgi_og_opt(w.graph)
    output:
        xp=f"{RESULTS}/indexes/odgi/{{graph}}/{{graph}}.xp",
        metrics=f"{RESULTS}/metrics/index/odgi/{{graph}}/pathindex.json",
        log=f"{RESULTS}/logs/index/odgi/{{graph}}/pathindex.log",
    threads: THREADS
    params:
        extra=lambda w: join_options(extra("odgi", "pathindex_extra"), graph_extra(w, "odgi_pathindex_extra"))
    shell:
        """
        {PYTHON} {SCRIPTS}/measure.py \
          --metrics {output.metrics} --log {output.log} \
          --sample-interval {SAMPLE_INTERVAL} \
          -- {ODGI} pathindex -i {input.og} -o {output.xp} -t {threads} {params.extra}
        """


rule odgi_stepindex:
    input:
        og=lambda w: odgi_og_opt(w.graph)
    output:
        stpidx=f"{RESULTS}/indexes/odgi/{{graph}}/{{graph}}.stpidx",
        metrics=f"{RESULTS}/metrics/index/odgi/{{graph}}/stepindex.json",
        log=f"{RESULTS}/logs/index/odgi/{{graph}}/stepindex.log",
    threads: THREADS
    params:
        extra=lambda w: join_options(extra("odgi", "stepindex_extra"), graph_extra(w, "odgi_stepindex_extra"))
    shell:
        """
        {PYTHON} {SCRIPTS}/measure.py \
          --metrics {output.metrics} --log {output.log} \
          --sample-interval {SAMPLE_INTERVAL} \
          -- {ODGI} stepindex -i {input.og} -o {output.stpidx} -t {threads} {params.extra}
        """


# --------------------------------------------------------------------------
# Node queries
# --------------------------------------------------------------------------

def vg_context_flags(wildcards):
    """Return vg chunk context flags for the requested node-query track."""
    if wildcards.track == "node_bases":
        return f"-l {wildcards.context}"
    return f"-c {wildcards.context}"


def odgi_context_flags(wildcards):
    """Return odgi extract context flags for the requested track."""
    if wildcards.track == "node_bases":
        return f"-L {wildcards.context}"
    return f"-c {wildcards.context}"


rule vg_node_query:
    input:
        xg=lambda w: vg_xg(w.graph),
        node_map=lambda w: original_node_map_file(w.graph, w.query),
    output:
        gfa=f"{RESULTS}/queries/{{track}}/vg/{{graph}}/{{query}}/context_{{context}}/subgraph.gfa",
        metrics=f"{RESULTS}/metrics/queries/{{track}}/vg/{{graph}}/{{query}}/context_{{context}}.json",
        log=f"{RESULTS}/logs/queries/{{track}}/vg/{{graph}}/{{query}}/context_{{context}}.log",
    threads: THREADS
    params:
        context=vg_context_flags,
        extra=vg_chunk_options,
    shell:
        """
        node="$(cat {input.node_map})"
        {PYTHON} {SCRIPTS}/measure.py \
          --metrics {output.metrics} --log {output.log} \
          --stdout {output.gfa} --sample-interval {SAMPLE_INTERVAL} \
          -- {VG} chunk -x {input.xg} -r "$node:$node" {params.context} \
             -O gfa -t {threads} {params.extra}
        """


rule odgi_node_query:
    input:
        og=lambda w: odgi_og_opt(w.graph),
        node_map=lambda w: odgi_node_map_file(w.graph, w.query),
    output:
        gfa=f"{RESULTS}/queries/{{track}}/odgi/{{graph}}/{{query}}/context_{{context}}/subgraph.gfa",
        metrics=f"{RESULTS}/metrics/queries/{{track}}/odgi/{{graph}}/{{query}}/context_{{context}}.json",
        log=f"{RESULTS}/logs/queries/{{track}}/odgi/{{graph}}/{{query}}/context_{{context}}.log",
    threads: THREADS
    params:
        context=odgi_context_flags,
        extra=lambda w: extra("odgi", "extract_extra"),
        temp=lambda w: str(node_gfa(w.track, "odgi", w.graph, w.query, w.context).parent),
    shell:
        """
        node="$(cat {input.node_map})"
        {PYTHON} {SCRIPTS}/measure.py \
          --metrics {output.metrics} --log {output.log} \
          --stdout {output.gfa} --sample-interval {SAMPLE_INTERVAL} \
          -- {PYTHON} {SCRIPTS}/odgi_extract_to_gfa.py \
             --odgi {ODGI} --input {input.og} --threads {threads} \
             --temp-dir {params.temp:q} -- -n "$node" {params.context} {params.extra}
        """


rule gbz_node_query:
    # gbz-base context is a base-pair budget, so this only applies to node_bases.
    # It writes GFA on stdout, so no conversion step is needed.
    input:
        db=lambda w: gbz_db(w.graph),
        node_map=lambda w: original_node_map_file(w.graph, w.query),
    output:
        gfa=f"{RESULTS}/queries/node_bases/gbz/{{graph}}/{{query}}/context_{{context}}/subgraph.gfa",
        metrics=f"{RESULTS}/metrics/queries/node_bases/gbz/{{graph}}/{{query}}/context_{{context}}.json",
        log=f"{RESULTS}/logs/queries/node_bases/gbz/{{graph}}/{{query}}/context_{{context}}.log",
    threads: THREADS
    params:
        extra=lambda w: extra("gbz", "query_extra"),
    shell:
        """
        node="$(cat {input.node_map})"
        {PYTHON} {SCRIPTS}/measure.py \
          --metrics {output.metrics} --log {output.log} \
          --stdout {output.gfa} --sample-interval {SAMPLE_INTERVAL} \
          -- {GBZ} query --node "$node" --context {wildcards.context} \
             {input.db} {params.extra}
        """


rule gfaidx_node_matched:
    # gfaidx bounds a BFS by node count, so it cannot be given a step or bp
    # context directly. The source tool runs first and its output node count
    # becomes --max_nodes, which puts both tools at the same output scale.
    input:
        unpack(lambda w: gfaidx_sidecars(w.graph)),
        node_map=lambda w: original_node_map_file(w.graph, w.query),
        source_stats=lambda w: node_gfa(w.track, w.source, w.graph, w.query, w.context).with_suffix(".stats.json"),
    output:
        gfa=f"{RESULTS}/queries/{{track}}/gfaidx_matched_{{source}}/{{graph}}/{{query}}/context_{{context}}/subgraph.gfa",
        metrics=f"{RESULTS}/metrics/queries/{{track}}/gfaidx_matched_{{source}}/{{graph}}/{{query}}/context_{{context}}.json",
        log=f"{RESULTS}/logs/queries/{{track}}/gfaidx_matched_{{source}}/{{graph}}/{{query}}/context_{{context}}.log",
    threads: THREADS
    params:
        extra=lambda w: extra("gfaidx", "get_subgraph_extra"),
    shell:
        """
        node="$(cat {input.node_map})"
        max_nodes="$({PYTHON} {SCRIPTS}/json_value.py --input {input.source_stats} --key nodes --minimum 1)"
        {PYTHON} {SCRIPTS}/measure.py \
          --metrics {output.metrics} --log {output.log} \
          --sample-interval {SAMPLE_INTERVAL} \
          -- {GFAIDX} get_subgraph {input.gz} "$node" {output.gfa} \
             --idx {input.idx} --ndx {input.ndx} --pdx {input.pdx} \
             --lnx {input.lnx} --pcx {input.pcx} \
             --max_nodes "$max_nodes" --with_coords {params.extra}
        """


# --------------------------------------------------------------------------
# Region queries
# --------------------------------------------------------------------------

rule vg_region_query:
    input:
        xg=lambda w: vg_xg(w.graph),
        locus=lambda w: locus_resolution_file(w.graph, w.query),
    output:
        gfa=f"{RESULTS}/queries/region/vg/{{graph}}/{{query}}/subgraph.gfa",
        metrics=f"{RESULTS}/metrics/queries/region/vg/{{graph}}/{{query}}.json",
        log=f"{RESULTS}/logs/queries/region/vg/{{graph}}/{{query}}.log",
    threads: THREADS
    params:
        extra=vg_chunk_options,
    shell:
        """
        region="$({PYTHON} {SCRIPTS}/json_value.py \
          --input {input.locus:q} --key vg_region)"
        {PYTHON} {SCRIPTS}/measure.py \
          --metrics {output.metrics} --log {output.log} \
          --stdout {output.gfa} --sample-interval {SAMPLE_INTERVAL} \
          -- {VG} chunk -x {input.xg} -p "$region" -c 0 \
             -O gfa -t {threads} {params.extra}
        """


rule odgi_region_query:
    input:
        og=lambda w: odgi_og_opt(w.graph),
        locus=lambda w: locus_resolution_file(w.graph, w.query),
    output:
        gfa=f"{RESULTS}/queries/region/odgi/{{graph}}/{{query}}/subgraph.gfa",
        metrics=f"{RESULTS}/metrics/queries/region/odgi/{{graph}}/{{query}}.json",
        log=f"{RESULTS}/logs/queries/region/odgi/{{graph}}/{{query}}.log",
    threads: THREADS
    params:
        extra=lambda w: extra("odgi", "extract_extra"),
        temp=lambda w: str(region_gfa("odgi", w.graph, w.query).parent),
    shell:
        """
        region="$({PYTHON} {SCRIPTS}/json_value.py \
          --input {input.locus:q} --key odgi_region)"
        {PYTHON} {SCRIPTS}/measure.py \
          --metrics {output.metrics} --log {output.log} \
          --stdout {output.gfa} --sample-interval {SAMPLE_INTERVAL} \
          -- {PYTHON} {SCRIPTS}/odgi_extract_to_gfa.py \
             --odgi {ODGI} --input {input.og} --threads {threads} \
             --temp-dir {params.temp:q} -- -r "$region" {params.extra}
        """


rule gbz_region_query:
    input:
        db=lambda w: gbz_db(w.graph),
        locus=lambda w: locus_resolution_file(w.graph, w.query),
    output:
        gfa=f"{RESULTS}/queries/region/gbz/{{graph}}/{{query}}/subgraph.gfa",
        metrics=f"{RESULTS}/metrics/queries/region/gbz/{{graph}}/{{query}}.json",
        log=f"{RESULTS}/logs/queries/region/gbz/{{graph}}/{{query}}.log",
    threads: THREADS
    params:
        extra=lambda w: extra("gbz", "query_extra"),
    shell:
        """
        sample="$({PYTHON} {SCRIPTS}/json_value.py \
          --input {input.locus:q} --key gbz_sample)"
        contig="$({PYTHON} {SCRIPTS}/json_value.py \
          --input {input.locus:q} --key gbz_contig)"
        interval="$({PYTHON} {SCRIPTS}/json_value.py \
          --input {input.locus:q} --key gbz_interval)"
        {PYTHON} {SCRIPTS}/measure.py \
          --metrics {output.metrics} --log {output.log} \
          --stdout {output.gfa} --sample-interval {SAMPLE_INTERVAL} \
          -- {GBZ} query --sample "$sample" --contig "$contig" \
             --interval "$interval" {input.db} {params.extra}
        """


rule gfaidx_region_all_haplotypes:
    # Coordinate intervals use exact path-supported selection. This is a
    # different operation from the node-count-bounded BFS used by get_subgraph,
    # so no --max_nodes value is supplied or matched to another tool.
    input:
        unpack(lambda w: gfaidx_sidecars(w.graph)),
        cdx=lambda w: str(gfaidx_prefix(w.graph)) + ".cdx",
        locus=lambda w: locus_resolution_file(w.graph, w.query),
    output:
        gfa=f"{RESULTS}/queries/region/gfaidx_all_haplotypes/{{graph}}/{{query}}/subgraph.gfa",
        metrics=f"{RESULTS}/metrics/queries/region/gfaidx_all_haplotypes/{{graph}}/{{query}}.json",
        log=f"{RESULTS}/logs/queries/region/gfaidx_all_haplotypes/{{graph}}/{{query}}.log",
    threads: THREADS
    params:
        extra=lambda w: extra("gfaidx", "get_region_extra"),
        reference=gfaidx_region_reference_arg,
    shell:
        """
        region="$({PYTHON} {SCRIPTS}/json_value.py \
          --input {input.locus:q} --key gfaidx_region)"
        {PYTHON} {SCRIPTS}/measure.py \
          --metrics {output.metrics} --log {output.log} \
          --sample-interval {SAMPLE_INTERVAL} \
          -- {GFAIDX} get_region {input.gz} "$region" {output.gfa} \
             --cdx {input.cdx} --idx {input.idx} --ndx {input.ndx} \
             --pdx {input.pdx} --lnx {input.lnx} --pcx {input.pcx} \
             {params.reference} --with_coords --all_haplotypes \
             {params.extra}
        """


rule gfa_stats:
    input:
        gfa="{prefix}.gfa"
    output:
        stats="{prefix}.stats.json"
    shell:
        "{PYTHON} {SCRIPTS}/graph_stats.py --gfa {input.gfa} --out {output.stats}"

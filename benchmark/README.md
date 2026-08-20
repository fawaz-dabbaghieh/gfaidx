# gfaidx / vg / odgi / gbz-base benchmark

This Snakemake workflow compares graph indexing and subgraph extraction with
gfaidx, VG, ODGI, and gbz-base. It records wall time, peak memory, output size,
index size, graph statistics, tool versions, exact commands, and logs.

There are separate entry points for GFA graphs with `W` records and graphs with
`P` records. They share the measurement and reporting rules, but keep their
path-name and coordinate setup separate.

## Contents

- [Requirements](#requirements)
- [Workflow files](#workflow-files)
- [Choose a workflow](#choose-a-workflow)
- [Configure the tools](#configure-the-tools)
- [W-line graphs](#w-line-graphs)
- [P-line graphs](#p-line-graphs)
- [Query design](#query-design)
- [Run the workflow](#run-the-workflow)
- [What is measured](#what-is-measured)
- [Outputs](#outputs)
- [Plot the results](#plot-the-results)
- [Tool differences](#tool-differences)

## Requirements

Install Snakemake 9 and the four benchmarked tools:

- `gfaidx`
- `vg`
- `odgi`
- `gbz-base`
- `python3`
- `matplotlib` (only for `scripts/plot_results.py`; the benchmark and HTML report do not require it)

The tools may be installed in different environments. Configure an absolute
path for each executable when it is not available on `PATH`.

For reproducible resource measurements, allow enough cores for the largest
configured query thread count and serialize benchmark jobs with the custom
resource `benchmark_job=1`. For the default eight-thread maximum, use
`--cores 8 --resources benchmark_job=1`. This lets one measured command use
eight threads without allowing multiple measurements to compete for CPU,
memory, or storage bandwidth.

## Workflow files

```text
benchmark/Snakefile                  backward-compatible W-line entry point
benchmark/Snakefile.w                explicit W-line entry point
benchmark/Snakefile.p                P-line entry point
benchmark/rules/common.smk           shared indexing, query, and report rules
benchmark/config.yaml                example W-line configuration
benchmark/config.p.yaml              example P-line configuration
benchmark/graphs.tsv                 example W-line graph manifest
benchmark/graphs.p.tsv               example P-line graph manifest
benchmark/loci.tsv                   example W-coordinate queries
benchmark/loci.p.tsv                 example P-coordinate queries
benchmark/scripts/measure.py         process-tree time and peak-RSS measurement
benchmark/scripts/resolve_locus.py   W-record coordinate resolver
benchmark/scripts/resolve_p_locus.py P-path coordinate resolver
benchmark/scripts/w_to_p.py          W-to-P conversion required by ODGI
benchmark/scripts/vg_find_to_gfa.py  measured VG find-to-GFA streaming wrapper
benchmark/scripts/collect_results.py final result tables
```

Relative manifest paths are resolved below `benchmark/`. Relative graph paths
inside a manifest are resolved below `data_root`. A relative `results_dir` is
also created below `benchmark/`, independent of the launch directory.

## Choose a workflow

Use `Snakefile.w` when the source graph represents haplotypes with GFA 1.1 `W`
records:

```bash
snakemake --snakefile benchmark/Snakefile.w \
  --configfile benchmark/config.yaml --cores 8 \
  --resources benchmark_job=1 --dry-run
```

`benchmark/Snakefile` remains an alias for this workflow so existing commands
continue to work.

Use `Snakefile.p` when the source graph represents haplotypes with GFA 1.0 `P`
records whose names use unsuffixed PanSN form:

```text
sample#haplotype#contig
```

For example, `CHM13#0#chr1` and `HG00099#1#JBHDWO010000005.1` are supported.
The P workflow currently rejects fragmented coordinate-bearing names such as
`CHM13#0#chr1:1000000-2000000`. Every P path is treated as beginning at zero.

```bash
snakemake --snakefile benchmark/Snakefile.p \
  --configfile benchmark/config.p.yaml --cores 8 \
  --resources benchmark_job=1 --dry-run
```

## Configure the tools

Edit the `tools` section in the selected config file:

```yaml
tools:
  gfaidx: /absolute/path/to/gfaidx
  vg: /absolute/path/to/vg
  odgi: /absolute/path/to/odgi
  gbz_base: /absolute/path/to/gbz-base
  python: python3
```

Then set the graph and locus manifests, results directory, and context sweeps:

```yaml
data_root: /data/genome_graphs
results_dir: results_chr1
graphs_tsv: graphs.tsv
loci_tsv: loci.tsv
threads: 1
query_threads: [1, 2, 4, 8]
haplotype_gaps_bp: [50000, 100000, 200000, 500000]
odgi_merging_iterations: [1, 3, 6]
include_gfaidx_no_gap: true
include_odgi_default: true

node_contexts_steps: [1, 10, 100, 1000]
node_contexts_bases: [1000, 10000, 100000, 1000000]
```

`threads` is the fixed indexing and setup thread count. `query_threads` is an
independent extraction sweep. Region queries use every numeric value in
`haplotype_gaps_bp` for gfaidx and ODGI; ODGI also runs every configured merge
iteration. The two `include_*` switches retain the tools' special nonnumeric
baselines alongside the explicit parameter sweep.

Use `graphs.p.tsv` and `loci.p.tsv` with `Snakefile.p`. Large contexts can
produce very large subgraphs, and every additional context, thread, gap, locus,
or ODGI iteration multiplies the job count. Start with short arrays on a new
graph, then expand them for the final run.

Optional global tool arguments live in the `gfaidx`, `vg`, `odgi`, and `gbz`
sections. `vg.find_extra` applies only to node-seeded `vg find` queries;
`vg.chunk_extra` applies only to path-coordinate `vg chunk` queries. Keep them
separate because the two commands do not accept the same options. Per-graph
`*_extra` columns in the graph manifest extend these global values.

## W-line graphs

### Graph manifest

Add one row to `graphs.tsv`:

```tsv
graph	gfa	gfaidx_path_names_file	gfaidx_reference	odgi_path_indexes	gfaidx_index_extra	gfaidx_coord_extra	vg_convert_extra	vg_find_extra	vg_chunk_extra	odgi_build_extra	odgi_pathindex_extra	odgi_stepindex_extra	gbz_gbwt_extra
hprc_chr1	/data/hprc_chr1.gfa		CHM13	1			--ref-sample CHM13
```

Important fields:

| Column | Meaning |
| --- | --- |
| `graph` | File-safe ID used in output paths |
| `gfa` | Absolute graph path or a path below `data_root` |
| `gfaidx_path_names_file` | Optional filtered `get_path --print_path_names` output |
| `gfaidx_reference` | W sample coordinate-indexed by gfaidx when no names file is supplied |
| `odgi_path_indexes` | Build supplementary `.xp` and `.stpidx` artifacts; timed queries use neither |
| remaining fields | Optional per-graph tool arguments |

When every locus uses one sample, `gfaidx_reference` may be empty and the
workflow selects that sample. If loci use multiple samples, provide a path
names file containing every W record that gfaidx should coordinate-index.

The reference sample must also be imported by VG as a reference path. If it is
not named by an `RS:Z` header tag, add `--ref-sample SAMPLE` to
`vg_convert_extra` after confirming support in the installed VG version.

### Locus manifest

Each row in `loci.tsv` identifies one concrete W coordinate namespace:

```tsv
graph	query_id	sample	haplotype	seq_id	node_position	region_start	region_end	notes
hprc_chr1	chr1_148m	CHM13	0	chr1	148000000	148000000	148010000	10 kb locus
```

Coordinates are zero-based absolute W coordinates. For this record:

```gfa
W	CHM13	0	chr1	1000000	249000000	...
```

the manifest still uses the absolute coordinate `148000000`; the user does not
subtract `SeqStart` or construct VG and ODGI path names.

The W resolver:

1. Converts W records to coordinate-named P records for ODGI and writes a
   W-to-P mapping table.
2. Lists the paths actually imported by VG and ODGI.
3. Selects the one W fragment containing the requested absolute interval.
4. Converts the interval to each tool's path-local coordinate system.
5. Resolves the original node ID through VG and translates it to ODGI's
   compacted node-ID space.

A region spanning multiple W fragments is rejected. W records with `*`
coordinates cannot be used for absolute-coordinate queries.

## P-line graphs

### Input assumptions

The P workflow assumes:

- Path names have exact `sample#haplotype#contig` form.
- The haplotype field is an integer.
- Paths begin at coordinate zero.
- Paths have no coordinate suffix and are not fragmented coordinate records.
- The chosen reference path has no overlaps that change its path coordinates.

### Graph manifest

Add one row to `graphs.p.tsv`:

```tsv
graph	gfa	reference_sample	gfaidx_path_names_file	odgi_path_indexes	gfaidx_index_extra	gfaidx_coord_extra	vg_convert_extra	vg_find_extra	vg_chunk_extra	odgi_build_extra	odgi_pathindex_extra	odgi_stepindex_extra	gbz_gbwt_extra
pggb_chr1	/data/pggb_chr1.gfa	CHM13		1
```

`reference_sample` is required when several samples occur in the loci. It is
used to mark paths from that sample as references in VG/GBZ metadata so
gbz-base can perform coordinate queries. A region locus must use this sample.
Node-only loci may use another sample.

When `gfaidx_path_names_file` is empty, the workflow runs
`get_path --print_path_names` after `index_gfa` and selects the exact P paths
named by `loci.p.tsv`. This generated file is then supplied to
`index_coordinates`. An explicit names file may be supplied instead.

For the GBZ used by gbz-base, the workflow:

1. Parses P names with `([^#]+)#([0-9]+)#(.+)` and maps the captures to sample,
   haplotype, and contig metadata.
2. Builds an initial GBZ.
3. Runs a second VG GBWT operation that promotes `reference_sample` to
   `REFERENCE` sense.
4. Records `vg paths -M` metadata and checks the requested reference before
   running a timed interval query.

Both GBZ stages are included in the `vg_gbwt_gbz` indexing measurement.

### Locus manifest

Use the same columns as the W workflow, but identify one exact P name:

```tsv
graph	query_id	sample	haplotype	seq_id	node_position	region_start	region_end	notes
pggb_chr1	chr1_1m_100kb	CHM13	0	chr1	1000000	1000000	1100000	P-line example
```

This row resolves to `CHM13#0#chr1`. Coordinates are zero-based path-local
coordinates, and `region_end` is excluded.

The P resolver verifies the exact path in VG and ODGI. For region queries it
also verifies that VG stored the matching GBZ path as a reference. Unlike the W
workflow, no W-to-P conversion or fragment lookup is needed.

## Query design

`node_position` and `region_start`/`region_end` are independent:

```tsv
# Node query only
hprc_chr1	node_50m	CHM13	0	chr1	50000000			Node contexts

# Region query only
hprc_chr1	region_50m_100kb	CHM13	0	chr1		50000000	50100000	100 kb region

# Both query types
hprc_chr1	locus_148m	CHM13	0	chr1	148000000	148000000	148010000	Node and region
```

At least one query type must be present. Region start and end must be supplied
together as a nonempty half-open interval.

For scaling plots, use one or a few reproducible node positions with several
configured contexts. Use separate region rows for several interval widths,
preferably sharing a start coordinate. Do not add one row per node context;
the config arrays create that sweep automatically.

Repeated loci are supported. Several region rows may have the same interval
length at different coordinates, and several node-only rows may identify
different seed nodes. All individual runs stay in `query_metrics.tsv`; the
overview plots average replicates with the same interval length or node
context.

## Run the workflow

From the repository root, dry-run and execute the W workflow with:

```bash
conda activate extgfa

snakemake --snakefile benchmark/Snakefile.w \
  --configfile benchmark/config.yaml \
  --cores 8 --resources benchmark_job=1 --dry-run --printshellcmds

snakemake --snakefile benchmark/Snakefile.w \
  --configfile benchmark/config.yaml \
  --cores 8 --resources benchmark_job=1 --printshellcmds
```

For a P graph, change both files:

```bash
snakemake --snakefile benchmark/Snakefile.p \
  --configfile benchmark/config.p.yaml \
  --cores 8 --resources benchmark_job=1 --dry-run --printshellcmds

snakemake --snakefile benchmark/Snakefile.p \
  --configfile benchmark/config.p.yaml \
  --cores 8 --resources benchmark_job=1 --printshellcmds
```

A custom config may be stored anywhere:

```bash
snakemake --snakefile benchmark/Snakefile.p \
  --configfile /path/to/config.pggb.yaml \
  --cores 8 --resources benchmark_job=1
```

Generate a DAG for either workflow with Graphviz:

```bash
snakemake --snakefile benchmark/Snakefile.w \
  --configfile benchmark/config.yaml --cores 8 \
  --resources benchmark_job=1 --dag |
dot -Tpng > benchmark/dag.w.png
```

Set `--cores` to at least the largest value in `query_threads`. Keep
`--resources benchmark_job=1`; without it, Snakemake may run independent
measurements concurrently and distort timing and memory results.

## What is measured

### Indexing

| Tool | Measured steps |
| --- | --- |
| gfaidx | `index_gfa`, then `index_coordinates` |
| VG | `vg convert -g -x` to `.xg` |
| ODGI, W workflow | W-to-P conversion, normal `.og`, optimized `.opt.og`, optional `.xp` and `.stpidx` |
| ODGI, P workflow | Normal `.og`, optimized `.opt.og`, optional `.xp` and `.stpidx` |
| gbz-base, W workflow | VG GFA-to-GBZ construction, then `gbz-base construct` |
| gbz-base, P workflow | VG GFA-to-GBZ construction and reference promotion, then `gbz-base construct` |

The primary query-ready index plots combine gfaidx graph and coordinate indexing.
For ODGI, a W-line graph includes the measured W-to-P conversion plus the
optimized build; a P-line graph includes the optimized build directly. The
unoptimized build and optional path/step indexes remain in detailed and
supplementary outputs but are excluded from the query-ready total.

### Node queries

Two context tracks are generated:

- `node_steps`: `vg find -n NODE -c N` and `odgi extract -n NODE -c N`.
- `node_bases`: `vg find -n NODE -c N -L`, `odgi extract -n NODE -L N`,
  and `gbz-base query --node NODE --context N`.

`vg find` emits VG protobuf, so the workflow streams it through
`vg convert -f -`. Both processes run below the same measurement wrapper and
there is no unmeasured intermediate file.

gfaidx bounds BFS by node count rather than steps or bases. Each source tool is
run first, its output S lines are counted, and gfaidx is run with that count as
`--max_nodes` plus `--with_coords`. Results are named
`gfaidx_matched_vg`, `gfaidx_matched_odgi`, and `gfaidx_matched_gbz`.

ODGI node queries run at every `query_threads` value. `vg find` and gbz-base
have no node-query thread option, so each of those source extractions is
measured once and recorded with `threads=NA`; the matched gfaidx side still runs
at every configured thread value. The gfaidx thread setting applies to its
indexed path/subpath formatting work, so scaling may be small when graph
traversal or materialization dominates.

### Region queries

- VG uses `vg chunk -p PATH:START-END -c 0 -O gfa -t THREADS`. It has no
  haplotype-gap equivalent.
- ODGI uses `odgi extract -r PATH:START-END -t THREADS`, followed by
  `odgi view -g -t THREADS`. Explicit sweep rows add
  `-d GAP_BP -e ITERATIONS`; a separate `default` variant omits both flags.
- gbz-base uses a reference sample, contig, and interval with `--context 0`.
  It has no query thread or haplotype-gap option, so it is measured once per
  locus.
- gfaidx uses `get_region --all_haplotypes --with_coords --threads THREADS`.
  Explicit sweep rows add `--haplotype_gap GAP_BP`; a separate `no_gap`
  variant omits the flag and preserves gfaidx's outermost-anchor behavior.

Manifest intervals are half-open. `vg chunk -p` uses an inclusive end, so the
resolver sends `region_end - 1` only to VG.

All timed query outputs are complete GFA text. VG node queries run `vg find`
and stream its protobuf output through `vg convert -f`; VG region queries use
`vg chunk -O gfa` directly. Extraction and serialization are included in the
same measurement. ODGI extraction and GFA serialization likewise execute under
one measurement, including the temporary `.og` output and `odgi view -g`.

Path resolution, original-node lookup, and ODGI node-ID translation are setup
operations and are not included in query timings. Their results remain in the
audit tables.

## Outputs

With `results_dir: results_chr1`, outputs are written below:

```text
benchmark/results_chr1/
```

Important files and directories:

```text
report.html                    self-contained summary report
tables/index_metrics.tsv       indexing time and memory
tables/index_sizes.tsv         query-ready file sizes and per-tool totals
tables/query_metrics.tsv       long-form query measurements
tables/query_comparison.tsv    parameter-keyed wide comparison/audit table
tables/tool_versions.tsv       executable versions
maps/resolved_loci.tsv         resolved coordinates and both node-ID spaces
metrics/                       raw JSON measurements and exact commands
logs/                          command logs
indexes/                       generated indexes
queries/                       extracted GFA graphs
```

For W graphs, `inputs/<graph>/<graph>.w_to_p.tsv` records every W fragment and
its generated ODGI P name. For P graphs,
`inputs/<graph>/<graph>.coordinate_paths.tsv` records the P paths selected for
gfaidx coordinate indexing, unless an explicit names file was supplied.

`query_metrics.tsv` records `threads`, `haplotype_gap_bp`,
`merging_iterations`, and `query_variant` on every row. `NA` means that a
parameter is not available for that tool. Use this long table for custom sweep
plots. `query_comparison.tsv` pivots only exactly matching parameter keys, so
cells are intentionally empty or `NA` where tools do not expose equivalent
settings.

## Plot the results

After the final tables are collected:

```bash
python3 benchmark/scripts/plot_results.py \
  --results-dir benchmark/results_chr1
```

PNG, PDF, and SVG figures are written below `results_chr1/plots/`. Use
`--formats png` or `--output-dir DIR` to change this behavior.

The plots cover indexing time and memory, total and component index sizes,
interval scaling, node-context scaling, output size, and the relative cost of
each source query and its node-count-matched gfaidx query. `plots.tsv` lists all
generated figures.

When several region queries have the same requested length, interval plots show
one arithmetic-mean point per tool and length. Time, peak memory, output scale,
and per-query relative-cost ratios are averaged independently. The unaggregated
measurements remain available in `query_metrics.tsv` and
`query_comparison.tsv`.

Node plots similarly combine all node-query rows for a graph. At each step or
base-pair context, they show the arithmetic-mean time and peak memory across the
seed nodes in `loci.tsv`. Relative-cost plots calculate each seed node's source
tool to matched-gfaidx ratio first and then average those ratios. Individual
node measurements remain in the result tables.

The existing overview plots select the smallest configured numeric query
thread, the gfaidx `no_gap` variant, the ODGI `default` variant, standard VG,
and gbz-base with context zero. They therefore do not mix experimental
settings. All other thread/gap/iteration combinations remain in the TSVs for
dedicated scaling and supplementary plots.

## Tool differences

### Why node queries use `vg find` and regions use `vg chunk`

Node-seeded queries deliberately use `vg find`, while path-coordinate queries
continue to use `vg chunk`:

```bash
# Node context in graph steps. VG emits protobuf; the workflow converts it to GFA.
vg find -x graph.xg -n NODE -c STEPS | vg convert -f -t 1 -

# Node context in base pairs (-L changes how -c is interpreted).
vg find -x graph.xg -n NODE -c BASES -L | vg convert -f -t 1 -

# Exact path interval with no graph-neighborhood expansion.
vg chunk -x graph.xg -p PATH:START-END -c 0 -O gfa -t THREADS
```

This split avoids a VG `chunk` node-range bug found while validating the
benchmark. In affected VG source, the `-l/--context-length` parser correctly
stores the requested base-pair length and sets step context to `-1`. However,
`PathChunker::extract_id_range()` calls step expansion unconditionally and then
passes the step-context variable to length expansion too, instead of passing
the requested length. Because the expansion APIs take an unsigned value, the
`-1` sentinel becomes a huge context. A query such as
`vg chunk -r NODE:NODE -l 1000` can consequently expand over the full connected
component and produce a graph many orders of magnitude larger than the intended
1 kb neighborhood. The relevant implementation can be inspected in
[`src/chunker.cpp` in VG 1.73.0](https://github.com/vgteam/vg/blob/v1.73.0/src/chunker.cpp#L245-L258).

`vg find` implements the desired node-seeded operations directly: `-c N` for
step context and `-c BP -L` for a minimum base-pair distance. It has no
query-thread option, so the benchmark measures each VG source node query once
and includes its `vg convert` process in the same time and peak-RSS result.

The region case is different. `vg chunk -p PATH:START-END -c 0` starts from a
path interval, emits GFA directly, supports the configured query threads, and
explicitly disables extra graph-step context. It does not use the problematic
node-ID range plus length-context code path, so it remains the appropriate
command for region extraction.

### ODGI node IDs

ODGI queries use an optimized graph whose IDs are compacted to `1..N`; these do
not necessarily match input GFA IDs. The workflow resolves the original node
through VG and uses `odgi position` at the same path coordinate to record the
corresponding compacted ID.

### ODGI and W records

ODGI does not retain the source W records as queryable paths in this workflow.
The measured `w_to_p.py` step creates coordinate-named P records, and the
mapping table prevents later code from guessing the generated names.

### VG W names

VG normally keeps a zero-start W name as `sample#haplotype#sequence` and may
name a nonzero subrange `sample#haplotype#sequence[start-end]`. The W resolver
checks VG's actual path list before creating a query.

### Output node sets

Path intervals, graph-step context, base-pair context, and a node-count-bounded
BFS do not have identical semantics. The workflow therefore records output
nodes, links, paths, and bytes rather than claiming that all extracted node sets
must match.

### Region context and haplotype gaps

VG's `-c 0` and gbz-base's `--context 0` suppress graph-neighborhood expansion
around the requested path interval. This prevents gbz-base's nonzero default
context from silently adding extra nodes. ODGI's `-d` and gfaidx's
`--haplotype_gap` both use base-pair thresholds for nearby path-supported
pieces, but their algorithms are not identical; the table records the numeric
settings without claiming identical output node sets. gfaidx's omitted-gap
behavior is a separate baseline rather than being treated as another numeric
gap.

### GBZ node chopping

The example configs pass `vg gbwt --max-node 0`, which prevents the default
segment chopping and keeps numeric source IDs usable for node comparisons. If
chopping is enabled, query nodes must be translated with VG's translation
table before a gbz-base node query.

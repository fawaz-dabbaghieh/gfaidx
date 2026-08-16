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

The tools may be installed in different environments. Configure an absolute
path for each executable when it is not available on `PATH`.

For reproducible resource measurements, run one benchmark job at a time with
`--cores 1`. Concurrent jobs compete for CPU, memory, and storage bandwidth.

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
  --configfile benchmark/config.yaml --cores 1 --dry-run
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
  --configfile benchmark/config.p.yaml --cores 1 --dry-run
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

node_contexts_steps: [1, 10, 100, 1000]
node_contexts_bases: [1000, 10000, 100000, 1000000]
```

Use `graphs.p.tsv` and `loci.p.tsv` with `Snakefile.p`. Large contexts can
produce very large subgraphs, so start with a short sweep on a new graph.

Optional global tool arguments live in the `gfaidx`, `vg`, `odgi`, and `gbz`
sections. `vg.chunk_extra` is applied to VG node and interval extraction.
Per-graph `*_extra` columns in the graph manifest extend these global values.

## W-line graphs

### Graph manifest

Add one row to `graphs.tsv`:

```tsv
graph	gfa	gfaidx_path_names_file	gfaidx_reference	odgi_path_indexes	gfaidx_index_extra	gfaidx_coord_extra	vg_convert_extra	vg_chunk_extra	odgi_build_extra	odgi_pathindex_extra	odgi_stepindex_extra	gbz_gbwt_extra
hprc_chr1	/data/hprc_chr1.gfa		CHM13	1			--ref-sample CHM13
```

Important fields:

| Column | Meaning |
| --- | --- |
| `graph` | File-safe ID used in output paths |
| `gfa` | Absolute graph path or a path below `data_root` |
| `gfaidx_path_names_file` | Optional filtered `get_path --print_path_names` output |
| `gfaidx_reference` | W sample coordinate-indexed by gfaidx when no names file is supplied |
| `odgi_path_indexes` | Build `.xp` and `.stpidx`; normally `1` for a graph with paths |
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
graph	gfa	reference_sample	gfaidx_path_names_file	odgi_path_indexes	gfaidx_index_extra	gfaidx_coord_extra	vg_convert_extra	vg_chunk_extra	odgi_build_extra	odgi_pathindex_extra	odgi_stepindex_extra	gbz_gbwt_extra
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

## Run the workflow

From the repository root, dry-run and execute the W workflow with:

```bash
conda activate extgfa

snakemake --snakefile benchmark/Snakefile.w \
  --configfile benchmark/config.yaml \
  --cores 1 --dry-run --printshellcmds

snakemake --snakefile benchmark/Snakefile.w \
  --configfile benchmark/config.yaml \
  --cores 1 --printshellcmds
```

For a P graph, change both files:

```bash
snakemake --snakefile benchmark/Snakefile.p \
  --configfile benchmark/config.p.yaml \
  --cores 1 --dry-run --printshellcmds

snakemake --snakefile benchmark/Snakefile.p \
  --configfile benchmark/config.p.yaml \
  --cores 1 --printshellcmds
```

A custom config may be stored anywhere:

```bash
snakemake --snakefile benchmark/Snakefile.p \
  --configfile /path/to/config.pggb.yaml --cores 1
```

Generate a DAG for either workflow with Graphviz:

```bash
snakemake --snakefile benchmark/Snakefile.w \
  --configfile benchmark/config.yaml --dag |
dot -Tpng > benchmark/dag.w.png
```

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

The primary index plots combine gfaidx graph and coordinate indexing. They use
only ODGI's optimized build as the main ODGI index. Detailed tables retain all
individual measured steps.

### Node queries

Two context tracks are generated:

- `node_steps`: `vg chunk -c N` and `odgi extract -c N`.
- `node_bases`: `vg chunk -l N`, `odgi extract -L N`, and
  `gbz-base query --context N`.

gfaidx bounds BFS by node count rather than steps or bases. Each source tool is
run first, its output S lines are counted, and gfaidx is run with that count as
`--max_nodes` plus `--with_coords`. Results are named
`gfaidx_matched_vg`, `gfaidx_matched_odgi`, and `gfaidx_matched_gbz`.

### Region queries

- VG uses `vg chunk -p PATH:START-END -c 0 -O gfa`.
- ODGI uses `odgi extract -r PATH:START-END` followed by `odgi view -g`.
- gbz-base uses a reference sample, contig, and interval.
- gfaidx uses `get_region --all_haplotypes --with_coords`.

Manifest intervals are half-open. `vg chunk -p` uses an inclusive end, so the
resolver sends `region_end - 1` only to VG.

All timed query outputs are complete GFA text. VG writes GFA directly; it no
longer uses `vg find` or an intermediate protobuf graph. ODGI extraction and
GFA serialization are executed under one measurement, including the temporary
`.og` output and `odgi view -g`.

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
tables/index_sizes.tsv         index file sizes and per-tool totals
tables/query_metrics.tsv       long-form query measurements
tables/query_comparison.tsv    source and matched-gfaidx comparisons
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

## Tool differences

### VG chunk semantics

The workflow uses `vg chunk`, not `vg find`, for node and coordinate queries.
`chunk` can emit GFA directly and supports explicit step or base-pair context.
For interval queries, `-c 0` prevents additional graph-step expansion.

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

### GBZ node chopping

The example configs pass `vg gbwt --max-node 0`, which prevents the default
segment chopping and keeps numeric source IDs usable for node comparisons. If
chopping is enabled, query nodes must be translated with VG's translation
table before a gbz-base node query.

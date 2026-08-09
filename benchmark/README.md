# gfaidx / vg / odgi / gbz-base benchmark

This Snakemake workflow compares indexing and subgraph extraction with gfaidx,
VG, ODGI, and gbz-base. It records wall time, peak memory, output size, index
size, graph statistics, tool versions, commands, and logs.

The workflow accepts query loci as absolute coordinates on GFA `W` records. It
automatically resolves the original graph node ID, ODGI's compacted node ID,
and the different path-coordinate syntax used by each tool.

## Contents

- [Requirements](#requirements)
- [Files](#files)
- [Configure the tools](#configure-the-tools)
- [Add a new graph](#add-a-new-graph)
- [Describe query loci](#describe-query-loci)
- [How automatic locus resolution works](#how-automatic-locus-resolution-works)
- [Run the workflow](#run-the-workflow)
- [Outputs](#outputs)
- [What is measured](#what-is-measured)
- [Comparison details](#comparison-details)
- [Known tool differences](#known-tool-differences)

## Requirements

Install Snakemake 9 and the four benchmarked tools:

- `gfaidx`
- `vg`
- `odgi`
- `gbz-base`
- `python3`

The tools do not have to be installed in the same environment. Absolute binary
paths can be configured in `config.yaml`.

Run one benchmark job at a time. Concurrent indexing or query jobs compete for
CPU, memory, and storage bandwidth and make the measurements difficult to
compare.

## Files

```text
benchmark/Snakefile                 workflow rules
benchmark/config.yaml               tools, results directory, and context sizes
benchmark/graphs.tsv                one row per input graph
benchmark/loci.tsv                  W-coordinate loci to benchmark
benchmark/scripts/measure.py        time and process-tree peak-RSS measurement
benchmark/scripts/w_to_p.py         W-to-P conversion used for ODGI
benchmark/scripts/resolve_locus.py  coordinate and path-name resolver
benchmark/scripts/collect_results.py final result tables
benchmark/results/                  default generated output directory
benchmark/report.html               default self-contained HTML report
```

`node_queries.tsv` and `region_queries.tsv` are no longer needed. Their
tool-specific fields are generated from `loci.tsv`.

## Configure the tools

Edit the `tools` section of `benchmark/config.yaml`:

```yaml
tools:
  gfaidx: /absolute/path/to/gfaidx
  vg: /absolute/path/to/vg
  odgi: /absolute/path/to/odgi
  gbz_base: /absolute/path/to/gbz-base
  python: python3
```

Also set the input and output roots:

```yaml
data_root: /data/genome_graphs
results_dir: results_big_graph

graphs_tsv: graphs.tsv
loci_tsv: loci.tsv
threads: 1
```

Relative graph paths in `graphs.tsv` are resolved under `data_root`. A relative
`results_dir` is created under `benchmark/`, regardless of the directory from
which Snakemake is launched.

The context sweeps are also configured here:

```yaml
node_contexts_steps: [1, 10, 100, 1000]
node_contexts_bases: [1000, 10000, 100000, 1000000]
```

Large contexts can produce very large subgraphs. Start with a smaller set when
testing a new whole-chromosome graph.

## Add a new graph

Add one row to `benchmark/graphs.tsv`. The important columns are:

| Column | Meaning |
| --- | --- |
| `graph` | File-safe identifier used in result paths |
| `gfa` | Absolute path, or path relative to `data_root` |
| `gfaidx_path_names_file` | Optional filtered `get_path --print_path_names` output |
| `gfaidx_reference` | Optional W sample to coordinate-index |
| `odgi_path_indexes` | `1` for graphs with W/P paths; `0` for pathless graphs |
| remaining `*_extra` columns | Optional per-graph command-line arguments |

Example:

```tsv
graph	gfa	gfaidx_path_names_file	gfaidx_reference	odgi_path_indexes	gfaidx_index_extra	gfaidx_coord_extra	vg_convert_extra	odgi_build_extra	odgi_pathindex_extra	odgi_stepindex_extra
hprc_chr1	/data/hprc_chr1.gfa		CHM13	1
```

If all loci for the graph use one W sample, `gfaidx_reference` may be left
empty and the workflow selects that sample automatically. When loci use
multiple samples, provide `gfaidx_path_names_file` containing every W track
that should be coordinate-indexed.

The selected W sample must be imported by VG as a reference path. Usually it is
listed in the GFA header's `RS:Z` tag. If it is not, add an appropriate option,
for example `--ref-sample CHM13`, to the row's `vg_convert_extra` field and
confirm that the installed VG version supports it.

For automatic W-locus resolution, let this workflow perform its own W-to-P
conversion. Supplying a separately converted ODGI GFA with different P naming
rules can make the generated W-to-P mapping disagree with the ODGI graph.

## Describe query loci

Edit `benchmark/loci.tsv`:

```tsv
graph	query_id	sample	haplotype	seq_id	node_position	region_start	region_end	notes
hprc_chr1	chr1_148m	CHM13	0	chr1	148000000	148000000	148010000	10 kb locus
```

The W identity is the combination of `sample`, `haplotype`, and `seq_id`. For a
record such as:

```gfa
W	CHM13	0	chr1	1000000	249000000	...
```

the corresponding locus fields are `CHM13`, `0`, and `chr1`. All positions in
`loci.tsv` are zero-based **absolute W coordinates**, so users do not subtract
`SeqStart` and do not construct VG or ODGI path names.

The query columns behave as follows:

| Column | Meaning |
| --- | --- |
| `node_position` | Optional single coordinate used for node-context queries |
| `region_start` / `region_end` | Optional half-open interval used for region queries |

At least one query type must be present. Examples:

```tsv
# Node query only
hprc_chr1	node_50m	CHM13	0	chr1	50000000			Node contexts

# Region query only
hprc_chr1	region_50m_100kb	CHM13	0	chr1		50000000	50100000	100 kb region

# Both query types at one locus
hprc_chr1	locus_148m	CHM13	0	chr1	148000000	148000000	148010000	Node and region
```

For a representative benchmark, use several reproducible positions distributed
across the W track and several region widths. Avoid selecting only one unusually
simple or unusually complex locus.

Each row must fit completely inside one concrete W record. If a reference is
split into several W fragments, the resolver automatically chooses the fragment
containing the requested coordinates. A region spanning two fragments is
rejected and should be represented by separate rows. W records whose
`SeqStart`/`SeqEnd` are `*` cannot support absolute-coordinate loci.

## How automatic locus resolution works

The mapping is setup work and is not included in query timings:

1. `w_to_p.py` converts W records to P records for ODGI.
2. The same conversion writes:
   ```text
   results_dir/inputs/<graph>/<graph>.w_to_p.tsv
   ```
   This table records the W identity, W interval, and exact generated P name.
3. The workflow asks VG and ODGI to list the paths they actually indexed.
4. `resolve_locus.py` selects the W fragment containing the absolute locus,
   verifies its VG and ODGI paths, and calculates path-local offsets.
5. A single-position `vg find` lookup obtains the original graph node ID used
   by VG, gfaidx, and the unchopped GBZ.
6. `odgi position` translates the same path coordinate into the compacted ODGI
   node-ID space.

The final audit table is:

```text
results_dir/maps/resolved_loci.tsv
```

It contains absolute and local positions, W/P/VG path names, original node IDs,
ODGI node IDs, and every generated region argument. Check this table before
interpreting benchmark results.

Resolution stops with an error if a W record is missing, coordinates are out of
range, paths are ambiguous, VG did not import the selected path, ODGI did not
contain the generated P path, or a single-position lookup does not identify
exactly one node.

## Run the workflow

From the repository root:

```bash
conda activate extgfa

# Validate manifests and inspect commands without running them.
snakemake \
  --snakefile benchmark/Snakefile \
  --configfile benchmark/config.yaml \
  --cores 1 \
  --dry-run \
  --printshellcmds

# Run the benchmark serially.
snakemake \
  --snakefile benchmark/Snakefile \
  --configfile benchmark/config.yaml \
  --cores 1 \
  --printshellcmds
```

To use a separate configuration:

```bash
snakemake \
  --snakefile benchmark/Snakefile \
  --configfile /path/to/config.big_graph.yaml \
  --cores 1
```

Generate a DAG image with Graphviz:

```bash
snakemake --snakefile benchmark/Snakefile \
  --configfile benchmark/config.yaml --dag |
dot -Tpng > benchmark/dag.png
```

## Outputs

With `results_dir: results_big_graph`, generated data are written under:

```text
benchmark/results_big_graph/
```

Important outputs:

```text
tables/index_metrics.tsv       indexing time and memory
tables/index_sizes.tsv         index file sizes and per-tool totals
tables/query_metrics.tsv       long-form query results
tables/query_comparison.tsv    side-by-side query comparison
tables/tool_versions.tsv       executable versions
maps/resolved_loci.tsv         automatically resolved query coordinates and IDs
metrics/                       raw JSON measurements and exact commands
logs/                          stdout/stderr logs
indexes/                       generated tool indexes
queries/                       extracted graphs
```

The self-contained report is written to `benchmark/report.html`.

## What is measured

Indexing steps:

| Tool | Steps |
| --- | --- |
| gfaidx | `index_gfa`, then `index_coordinates` |
| VG | `vg convert -g -x` to `.xg` |
| ODGI | measured W-to-P conversion, `.og`, optimized `.opt.og`, `.xp`, `.stpidx` |
| gbz-base | `vg gbwt` to `.gbz`, then `gbz-base construct` |

Query tracks:

- `node_steps`: VG and ODGI expansion by graph steps, plus node-count-matched
  `gfaidx get_subgraph --with_coords`.
- `node_bases`: VG, ODGI, and gbz-base expansion by bases, plus node-count-matched
  `gfaidx get_subgraph --with_coords`.
- `region`: coordinate extraction by VG, ODGI, and gbz-base, compared with one
  exact `gfaidx get_region --all_haplotypes --with_coords` run.

W-to-P conversion is attributed to ODGI because it is required for ODGI to
retain W walks as paths. Locus resolution and node-ID translation are setup and
are not timed as extraction operations.

## Comparison details

For node-context queries, gfaidx bounds BFS by node count rather than steps or
bases. For every source query, the workflow counts output S lines and reruns
`get_subgraph --with_coords` with that count as `--max_nodes`. These rows are
named `gfaidx_matched_vg`, `gfaidx_matched_odgi`, and
`gfaidx_matched_gbz`.

Coordinate-interval queries use different semantics. They run gfaidx once with
`get_region --all_haplotypes --with_coords`; they do not pass `--max_nodes` and
do not create node-count-matched gfaidx variants.

The outputs need not contain identical node sets because path-range extraction,
step context, base context, and node-count-bounded BFS have different semantics.
The benchmark compares time, memory, index footprint, and output scale while
recording the exact commands and output graph statistics.

## Known tool differences

### ODGI node IDs

ODGI queries require an optimized graph whose IDs are compacted to `1..N`.
Those IDs do not match the input GFA. The workflow resolves an ODGI ID through
the same path coordinate used to find the original graph node and records both
IDs in `maps/resolved_loci.tsv`.

### ODGI and W records

ODGI does not retain W records as paths in this workflow's direct GFA import.
The measured `w_to_p.py` preparation step creates coordinate-named P records.
The generated W-to-P table prevents the resolver from reconstructing or
guessing those names independently.

### VG W names

VG normally keeps a zero-start W name as `sample#haplotype#sequence` and gives a
nonzero subrange a bracketed name such as
`sample#haplotype#sequence[start-end]`. The resolver checks the actual VG path
list instead of trusting the naming rule alone.

### Path-local and absolute coordinates

gfaidx and gbz-base queries use the absolute W coordinate namespace. VG and
ODGI path queries use local offsets beginning at zero for each imported W/P
record. `resolve_locus.py` performs this subtraction using the matching W
record's `SeqStart`.

### GBZ node chopping

The default configuration passes `vg gbwt --max-node 0`, disabling segment
chopping so GBZ keeps the original numeric node IDs. With default chopping, the
node set and IDs change and node-seeded comparisons require VG's translation
table.

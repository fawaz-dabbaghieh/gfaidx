# gfaidx / vg / odgi Benchmark

This directory contains a Snakemake workflow for benchmarking `gfaidx` against
`vg` and `odgi` on comparable graph-indexing and subgraph-extraction tasks.

The workflow is deliberately conservative about interpretation. The tools do
not expose identical query semantics, so the pipeline records output graph
statistics and uses matched output node counts when comparing `gfaidx` against
`vg` or `odgi`.

## What Is Compared

Indexing:

- `gfaidx`: `index_gfa`, then `index_coordinates`
- `vg`: `vg convert -g -v` to `.vg`, then `vg index -x` to `.xg`
- `odgi`: `odgi build` to `.og`, plus optional `odgi pathindex` to `.xp`
  and `odgi stepindex` to `.stpidx`

The main query-ready index size is:

- `gfaidx`: `.gfa.gz + .idx + .ndx + .pdx + .cdx`
- `vg`: `.vg + .xg`
- `odgi`: `.og + .xp + .stpidx` when path-side indexes are enabled, or only
  `.og` for graphs marked as pathless

Node-neighborhood extraction:

- `vg find -x graph.xg -n NODE -c CONTEXT`
- `odgi extract -i graph.og -n NODE -c CONTEXT`
- `gfaidx get_subgraph ... --max_nodes N`

For node queries, `N` is not chosen directly. The workflow first runs the source
tool (`vg` or `odgi`), counts the number of `S` lines in its output after
conversion to GFA, then runs `gfaidx` with `--max_nodes` set to that count. This
creates two comparisons:

- `vg_context_vs_gfaidx_cap`
- `odgi_context_vs_gfaidx_cap`

Coordinate-region extraction:

- `vg find -x graph.xg -p path:start-end`
- `odgi extract -i graph.og -r path:start-end`
- `gfaidx get_region graph.gfa.gz sequence:start-end --max_nodes N`
- direct `gfaidx get_region` using the manifest-provided `gfaidx_max_nodes`

For matched region queries, `N` is again taken from the source-tool output node
count. The workflow also runs a direct `gfaidx` coordinate query for every
region row. This keeps rGFA/minigraph coordinate extraction benchmarked even
when `vg` or `odgi` cannot express the query because the graph has no P/W paths.
Skipped source-tool region tasks are reported as `NA` in the final query table.
The output node sets are not expected to be identical because `vg`/`odgi` path
range extraction and `gfaidx` interval-seeded BFS are different operations. The
benchmark is therefore about runtime, memory, disk footprint, and output scale.

## Requirements

Install or make available:

- Snakemake 9 or newer
- `gfaidx`
- `vg`
- `odgi`
- Python 3

Tool paths are configured in `config.yaml`. On this machine, Snakemake is
available in the `extgfa` conda environment:

```bash
conda run -n extgfa snakemake --version
```

## Configure Graphs

Edit `graphs.tsv`. Paths may be absolute or relative to the repository root.

Required columns:

- `graph`: file-safe graph ID used in output paths
- `gfa`: input GFA

Optional columns:

- `gfaidx_path_names_file`: filtered output from
  `gfaidx get_path <graph>.pdx --print_path_names`
- `gfaidx_reference`: reference sample for `gfaidx index_coordinates`
- `odgi_path_indexes`: defaults to `1`; set to `0` for graphs with no P/W
  paths so the workflow skips `.xp` and `.stpidx`
- `*_extra`: per-graph extra options for each indexing command

Example:

```text
graph	gfa	gfaidx_path_names_file	gfaidx_reference	odgi_path_indexes	gfaidx_index_extra	gfaidx_coord_extra	vg_convert_extra	vg_index_extra	odgi_build_extra	odgi_pathindex_extra	odgi_stepindex_extra
chr22	test_graphs/hprc_chr22/chr22.gfa	benchmark/path_names/chr22.paths.tsv	CHM13	1
```

If both `gfaidx_path_names_file` and `gfaidx_reference` are present, the path
names file takes precedence for coordinate indexing.

Some rGFA/minigraph inputs have segment coordinate tags but no P/W path records.
For those graphs, set `odgi_path_indexes` to `0`. ODGI can still build the
`.og`, but some ODGI builds abort when `odgi pathindex` is run on a pathless
graph.

## Configure Node Queries

Edit `node_queries.tsv`:

```text
graph	query_id	node_id	notes
chr22	n1	123456	pilot node
```

The context values used for `vg` and `odgi` are in `config.yaml`:

```yaml
node_contexts: [0, 1, 2, 5, 10]
```

For each context, the workflow runs `vg` and `odgi`, counts output nodes, then
runs `gfaidx` with the matched `--max_nodes` value.

## Configure Region Queries

Edit `region_queries.tsv`:

```text
graph	query_id	gfaidx_region	gfaidx_reference	gfaidx_max_nodes	vg_region	odgi_region	notes
chr22	r1	chr22:0-1000000	CHM13	10000	CHM13#0#chr22:0-1000000	CHM13#0#chr22:0-1000000	pilot interval
```

`gfaidx_max_nodes` controls the direct `gfaidx` coordinate run. For matched
comparisons, `gfaidx` still uses the node count produced by the source tool.

Use the path names as each tool sees them. Leave `vg_region` or `odgi_region`
empty, `NA`, `N/A`, or `.` when that tool cannot run the coordinate query for
the graph. Those rows are skipped in the DAG and reported as `NA` in
`query_metrics.tsv`. Validate path names before large runs:

```bash
gfaidx get_path graph.gfa.gz.pdx --print_path_names
vg find -x graph.xg -I
odgi paths -i graph.og -L
```

## Run

From the repository root:

```bash
conda run -n extgfa snakemake -s benchmark/Snakefile --cores 1 -n
```

If Snakemake cannot write to the default user cache directory, point the cache
inside the repository:

```bash
XDG_CACHE_HOME="$PWD/benchmark/.cache" \
  conda run -n extgfa snakemake -s benchmark/Snakefile --cores 1 -n
```

Run the benchmark:

```bash
conda run -n extgfa snakemake -s benchmark/Snakefile --cores 8
```

Use a custom config:

```bash
conda run -n extgfa snakemake -s benchmark/Snakefile \
  --configfile benchmark/config.yaml \
  --cores 8
```

## Outputs

Generated files are written under `benchmark/results/`.

Important tables:

- `benchmark/results/tables/tool_versions.tsv`
- `benchmark/results/tables/index_metrics.tsv`
- `benchmark/results/tables/query_metrics.tsv`

Logs:

- `benchmark/results/logs/`

Raw metric JSON files:

- `benchmark/results/metrics/`

Indexes:

- `benchmark/results/indexes/gfaidx/`
- `benchmark/results/indexes/vg/`
- `benchmark/results/indexes/odgi/`

Query outputs:

- `benchmark/results/queries/`

## Notes

- Query output statistics are counted from GFA. Native `vg` and `odgi` outputs
  are converted to GFA after the timed command, and that conversion is not
  included in query runtime.
- The benchmark does not assume that source-tool and `gfaidx` outputs contain
  identical node sets. It records node, edge, path, and walk counts so results
  can be interpreted at the same output scale.
- Keep graph IDs and query IDs file-safe. Avoid slashes and whitespace.
- For stable measurements, run repeated full workflows on an otherwise idle
  machine and keep thread counts fixed.

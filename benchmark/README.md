# gfaidx / vg / odgi / gbz-base benchmark

Snakemake workflow that takes a GFA graph and measures wall time and peak RSS
for every indexing and extraction step of four genome-graph tools, then emits
comparison tables.

Started from `benchmark/` in the gfaidx repository and extended with gbz-base, a
base-pair context track, and a measured W-line to P-line conversion.

## Layout

```
Snakefile             the workflow
config.yaml           tool paths, context sweeps, extra CLI options
graphs.tsv            one row per input graph
node_queries.tsv      one row per node-neighborhood query
region_queries.tsv    one row per coordinate-interval query
scripts/measure.py    runs one command, records wall time + process-tree peak RSS
scripts/graph_stats.py counts S/L/P/W records in an output GFA
scripts/collect_results.py  joins metrics into the final tables
scripts/w_to_p.awk    W-line to P-line converter (from the gfaidx repo)
results/              everything generated
```

## Run

```bash
conda activate gfaidx_bench
cd /home/user2/fawaz/benchmark
snakemake -s Snakefile --cores 1
```

All tool executables are taken from the `tools` section of `config.yaml`.
Set `tools.gfaidx` to the gfaidx binary you want to benchmark (an absolute
path is recommended); the default configuration uses the native build at
`/home/user3/gfaidx/build/gfaidx`.

Use `--cores 1`. Running jobs concurrently makes the wall-time and peak-RSS
numbers meaningless, since the tools then compete for CPU and memory bandwidth.

## What is measured

Indexing, one row per command in `results/tables/index_metrics.tsv`:

| tool | steps |
| --- | --- |
| gfaidx | `index_gfa`, then `index_coordinates` (`.cdx`) |
| vg | `vg convert -g -x` to `.xg` |
| odgi | `w_to_p` conversion, `odgi build` to `.og`, `odgi build -O` to `.opt.og`, `odgi pathindex` to `.xp`, `odgi stepindex` to `.stpidx` |
| gbz-base | `vg gbwt -g` to `.gbz`, then `gbz-base construct` to `.gbz.db` |

`results/tables/index_sizes.tsv` reports every index file plus a `TOTAL` row per
tool, so the on-disk footprint comparison is explicit about what it counts. Note
that odgi's total covers both `.og` and `.opt.og`: all queries run on the
optimized graph, and the plain one is kept for the build-cost comparison, so
odgi's *query-ready* footprint is `.opt.og + .xp + .stpidx`.
GBZ construction is attributed to gbz-base because gbz-base consumes a GBZ and
vg is only the available builder.

Extraction, one row per command in `results/tables/query_metrics.tsv`, with a
per-query tool pivot in `results/tables/query_comparison.tsv`. There are three
tracks:

**`node_steps`** — context as a number of expansion steps.

- `vg find -x g.xg -n NODE -c K`
- `odgi extract -i g.og -n NODE -c K`
- gfaidx, matched (see below)

**`node_bases`** — context as a base-pair budget. This track exists because
gbz-base only expresses context in bp, and it is the only track where all four
tools appear.

- `vg find -x g.xg -n NODE -c BP -L`
- `odgi extract -i g.og -n NODE -L BP`
- `gbz-base query --node NODE --context BP g.gbz.db`
- gfaidx, matched

**`region`** — coordinate intervals.

- `vg find -x g.xg -p PATH:START-END`
- `odgi extract -i g.og -r PATH:START-END`
- `gbz-base query --sample S --contig C --interval START..END g.gbz.db`
- `gfaidx get_region g.gfa.gz SEQ:START-END --reference S --max_nodes N`
  (run both "direct" with the manifest cap and matched to each source tool)

### How gfaidx is made comparable

gfaidx has neither step nor bp context: `get_subgraph` bounds a BFS by node
count. So for every (query, context, source tool) the workflow runs the source
tool first, counts `S` lines in its output, and reruns gfaidx with
`--max_nodes` set to that count. That yields `gfaidx_matched_vg`,
`gfaidx_matched_odgi`, and `gfaidx_matched_gbz` rows sitting at the same output
scale as the tool they are matched to.

The outputs are not expected to contain identical node sets — path-range
extraction and interval-seeded BFS are different operations. The comparison is
about time, memory, index footprint, and output scale at matched node counts.

## Three findings that shaped the design

**odgi requires `-O`, and therefore requires a node-ID mapping.** `odgi extract`
(both `-n` and `-r`) and `odgi pathindex` all refuse a graph whose node IDs are
not compacted:

```
[odgi::extract] error: the node IDs are not compacted.
    Please run 'odgi sort' using -O, --optimize to optimize the graph.
error [xp]: Graph to index is not optimized. Please run 'odgi sort' using -O.
```

Any real HPRC graph trips this. chr22 here has 2,782,249 nodes with IDs spanning
53,303,057..56,138,482 — a range of 2,835,426, so there are gaps and the ID space
is not compacted. `-O` renumbers to `1..N`.

Note the check is about *compactness*, not about starting at 1: a small test
graph whose IDs formed a contiguous block (2891 IDs spanning exactly 2891 values,
offset to start at 53303057) passed `extract -n` without `-O`. That is why this
has to be verified on the real graph rather than a toy one.

The other three tools preserve the input ID space:

| path | node IDs |
| --- | --- |
| `vg convert -g -x` | preserved |
| `vg gbwt -g` to GBZ, then `gbz-base query` | preserved |
| `gfaidx index_gfa` | preserved |
| `odgi build` (no `-O`) | preserved, but unusable for queries |
| `odgi build -O` | compacted to `1..N` |

So vg, gbz-base and gfaidx are queried with original node IDs, and odgi needs a
translation. The `odgi_node_map` rule builds it the reliable way — through a
coordinate both ID spaces agree on:

```bash
odgi position -i graph.opt.og -p 'CHM13#0#chr22:0-51324926,20000007' -v
```

That returns odgi's node at the same reference locus as the original node ID.
`node_queries.tsv` therefore carries both `node_id` (original, for vg/gbz-base/
gfaidx) and `odgi_ref_locus` (for odgi). The mapping is setup, and is not
included in any query timing.

Consequence for interpretation: for node queries all four tools start from the
same *locus*, but odgi reports its own node IDs in its output. Node **counts**
are comparable; node **identity** across odgi and the rest is not, without
inverting the map. Region queries are unaffected, since those are specified by
coordinates rather than IDs.

**GBZ does renumber nodes by default, because it chops long segments.** The
project notes suspected GBZ renumbers from 1, and it does — but the cause is
segment chopping, not a deliberate relabelling. `vg gbwt` splits segments longer
than `--max-node` (default 1024 bp), which changes the node set and renumbers the
whole ID space:

| GBZ build | nodes | node ID range |
| --- | --- | --- |
| `vg gbwt -g` (default, chops at 1024 bp) | 2,791,965 | 1..2,791,965 |
| `vg gbwt --max-node 0 -g` (no chopping) | 2,782,249 | 53,303,057..56,138,482 |
| `vg convert -g -x` (`.xg`, for reference) | 2,782,249 | 53,303,057..56,138,482 |

With the default, `gbz-base query --node 53404858` fails outright:

```
Error: not found: The graph does not contain handle 106809716
```

This workflow therefore passes `--max-node 0` (set in `config.yaml` under
`gbz.gbwt_extra`), which makes the GBZ keep the input ID space so gbz-base can be
queried with the same node IDs as vg and gfaidx. The alternative, if a
default-chopped GBZ is wanted, is `vg gbwt --translation FILE` to dump the
segment-to-node table and translate query nodes through it.

Note this is a deliberate deviation from stock GBZ construction, made so the
node-seeded comparison is possible at all; it should be stated in the paper.

**odgi silently drops W-line paths.** `odgi build` on a W-line GFA exits 0 and
produces a graph with **zero paths** rather than erroring. The `w_to_p`
conversion is therefore required for correctness, not convenience, and it is a
measured step attributed to odgi.

## Region dialects

The three tools disagree about coordinates, so `region_queries.tsv` carries one
column set per tool rather than translating:

- **gbz-base** takes **absolute** contig coordinates: `--sample CHM13
  --contig chr22 --interval 20000000..20010000`.
- **odgi** takes **path-local** offsets appended to the full GFA path name. When
  `w_to_p.awk` produces a name like `CHM13#0#chr22:0-51324926`, the odgi
  argument is `CHM13#0#chr22:0-51324926:20000000-20010000` — everything before
  the final `:start-end` is the path name. odgi does not interpret the name's
  own coordinate suffix.
- **vg** takes path-local offsets too, but renames subrange W lines with
  brackets: a W line starting at a nonzero offset becomes
  `CHM13#0#chr6[31350872-31363898]`, and that bracketed form is what `-p` needs.
  A W line starting at 0 keeps the plain name.

For CHM13 chr22 the walk starts at 0, so path-local and absolute offsets
coincide and all four dialects address the same interval. That is a property of
this graph, not a general one. Confirm names before adding queries:

```bash
vg paths -x g.xg -L | head
odgi paths -i g.og -L | head
gfaidx get_region g.gfa.gz --print_path_names
```

## Adding work

Append rows to the manifests; no Snakefile edit is needed. Context sweeps live
in `config.yaml` (`node_contexts_steps`, `node_contexts_bases`). Every recorded
command is stored verbatim in the metrics JSON and copied into the tables, and
`results/tables/tool_versions.tsv` records the version of each tool.

# Repeated gfaidx and gbz-base interval benchmark

This benchmark compares coordinate-based subgraph extraction in `gfaidx` and
`gbz-base` with the same context length, so both tools select the same graph
neighborhood around a reference interval. It samples several genomic locations
at each requested interval length, queries each location at one or more
context lengths, and repeats each exact query. This separates variation among
graph regions from the operating-system page-cache effect seen when the same
query is executed again.

The benchmark is intentionally independent of the larger Snakemake workflow.
It consists of:

- `benchmark/scripts/benchmark_gfaidx_gbz_intervals.sh`, which runs and measures queries
- `benchmark/scripts/summarize_gfaidx_gbz_intervals.py`, which summarizes and plots them
- `benchmark/scripts/gfaidx_gbz_intervals.example.tsv`, an example interval manifest

## Comparisons

Both tools extract a context subgraph the same way: starting from the nodes of
the reference interval, they add every node reachable within `--context N`
base pairs. Each context length is measured in two paired comparisons, plus an
optional gfaidx-only variant:

| Comparison | gbz-base | gfaidx | What is compared |
|---|---|---|---|
| `reference_only` | `--context N --reference-only` | `--mode reference --context N --with_coords` | Identical output: the same nodes and links and one reference walk with coordinates |
| `all_haplotypes`, matched work | `--context N` | `--mode bfs --context N` | The same subgraph and haplotype fragments; neither tool computes subwalk coordinates |
| `all_haplotypes`, full output | (same gbz-base run) | `--mode bfs --context N --with_coords` | gfaidx additionally names every subwalk and computes its coordinates |
| `haplotype_span` (optional) | none | `--mode all --with_coords` | gfaidx's own haplotype-span selection, without a context |

gbz-base writes every non-reference haplotype as an anonymous walk,
`W unknown <n> <contig> 0 <length>`: it does not identify the sample or
compute coordinates. gfaidx `--mode bfs` without `--with_coords` writes the
real sample and haplotype with `<sequence>#subpath_<a>_<b> * *` and also skips
coordinate calculation, so it is the matched-work counterpart. With
`--with_coords`, every subwalk carries its real sequence coordinates.

A suggested framing for reporting the results:

- `reference_only` is the like-for-like headline: both tools produce the same
  graph and the same reference walk, so only speed and memory differ.
- `all_haplotypes` without coordinates shows the cost of extracting the same
  subgraph and haplotype fragments with matched work.
- `all_haplotypes` with coordinates shows what the additional output costs.
  Timing it next to the matched-work variant separates the cost of the extra
  data from the cost of the extraction itself.

## Requirements

- an indexed GFA accepted by `gfaidx get_region`, built with its `.pdx` and
  `.cdx` sidecars
- a `gbz-base` database (or GBZ) built from the same source graph
- the `gfaidx` and `gbz-base` executables
- GNU `time`, normally `/usr/bin/time` on Linux (Homebrew installs it as
  `gtime` on macOS)
- Python 3 for output counts and parity hashes (`benchmark/scripts/gfa_output_parity.py`,
  run once per query, context, and variant), plus Matplotlib for summaries and
  plots

The two indexes must represent the same graph version and reference paths.
Otherwise, the tools can extract different workloads even when given the same
coordinate interval. The raw table records output node, edge, path, and byte
counts and parity hashes, so any difference remains visible.

### Building the GBZ for exact parity

Build the GBZ without node chopping, for example with `vg gbwt --max-node 0`.
By default, VG splits segments longer than 1024 bp and renumbers the node ID
space. The subgraph can then be the same while every node identifier differs,
so the parity hashes never match. Context distances are unchanged by chopping,
but the chopped pieces of a long node can be selected individually where the
unchopped node is selected as a whole, so node counts can also differ.

Use local scratch storage for the output directory when possible. Writing
large result graphs or indexes over a network filesystem can dominate the
measurement.

## Interval TSV

The manifest is tab-separated and has six required columns:

```text
query_id	locus_id	reference	contig	start	end
anchor01_1kb	anchor01	CHM13	chr1	9999500	10000500
anchor01_10kb	anchor01	CHM13	chr1	9995000	10005000
anchor02_1kb	anchor02	CHM13	chr1	49999500	50000500
anchor02_10kb	anchor02	CHM13	chr1	49995000	50005000
```

Coordinates are zero-based and half-open. The fields mean:

- `query_id`: unique name for one exact interval
- `locus_id`: shared name for intervals centered on the same genomic location
- `reference`: reference sample passed to both tools, such as `CHM13`
- `contig`: sequence or contig name, such as `chr1`
- `start`, `end`: half-open coordinate boundaries

To estimate general performance for a 1 kb query, include several 1 kb rows at
different locations. Ten locations is a reasonable starting point. To compare
1 kb, 10 kb, and 100 kb intervals without changing the underlying locations,
use the same `locus_id` and center for all three sizes.

### Stratified manifests

`benchmark/scripts/make_benchmark_intervals.py` draws loci uniformly at random, either
genome-wide or from BED annotations, and writes every interval size around each
center. Near a chromosome end, a large window is shifted inward, so it still
contains the smaller windows of its locus. `benchmark/scripts/make_hprc_chm13_intervals.sh` uses it to build the CHM13
manifests in `benchmark/scripts/intervals/`. It downloads the T2T centromere/satellite
annotation and the GIAB v3.3 stratifications, then samples 33 loci, each with
windows of 1 kb, 10 kb, 100 kb, 1 Mb, 2 Mb, 5 Mb, and 10 Mb, in these strata:

- `random`: 10 loci uniform over chr1-22 and chrX, the unbiased baseline
- `named`: five fixed loci with complex structural variation (AMY1A, SMN1, LPA,
  KANSL1, CYP2D6)
- `centromere_hor`, `pericentromere`, `acrocentric`: centromere and satellite
  strata
- `mhc`, `kir`, `vdj`: immune loci
- `segdup`, `tandem`: segmental duplications and tandem repeats

`hprc_chm13_loci.tsv` records each locus's stratum and annotation. Locus IDs
are `<stratum>-<nn>`, so per-stratum summaries can be made from one combined
run by filtering `raw_runs.tsv` on the `locus_id` column:

```bash
for stratum in random named centromere_hor pericentromere acrocentric mhc kir vdj segdup tandem; do
  mkdir -p results/strata/$stratum
  awk -F'\t' -v s="$stratum" 'NR == 1 || index($4, s "-") == 1' \
    results/raw_runs.tsv > results/strata/$stratum/raw_runs.tsv
  python3 benchmark/scripts/summarize_gfaidx_gbz_intervals.py \
    --raw results/strata/$stratum/raw_runs.tsv --output-dir results/strata/$stratum
done
```

Running all strata in one shuffled run spreads machine noise evenly across
them. In the default (clipped) Minigraph-Cactus graphs, long unaligned
haplotype sequence is removed, so centromeric loci are mostly reference-only.
The `output_paths` column shows how much haplotype content each locus has.

## Running

Copy and edit the example manifest:

```bash
cp benchmark/scripts/gfaidx_gbz_intervals.example.tsv intervals.tsv
```

Run five repetitions per variant, interval, and context:

```bash
benchmark/scripts/benchmark_gfaidx_gbz_intervals.sh \
  --gfaidx-bin /home/fawaz/projects/gfaidx/current_build/gfaidx \
  --gbz-base-bin /home/fawaz/tools/gbz-base/target/release/gbz-base \
  --gfaidx-graph /path/to/graph.indexed.gfa.gz \
  --gbz-db /path/to/graph.gbz.db \
  --queries intervals.tsv \
  --repeats 5 \
  --contexts 0,100,1000,10000 \
  --gfaidx-threads 1,8 \
  --out-dir /local/scratch/gfaidx_gbz_repeated
```

For one interval and context `N`, the timed commands are equivalent to:

```bash
# reference_only
gbz-base query --sample CHM13 --contig chr1 \
  --interval START..END --context N --reference-only graph.gbz.db
gfaidx get_region graph.indexed.gfa.gz chr1:START-END output.gfa \
  --reference CHM13 --mode reference --context N --with_coords --threads T

# all_haplotypes
gbz-base query --sample CHM13 --contig chr1 \
  --interval START..END --context N graph.gbz.db
gfaidx get_region graph.indexed.gfa.gz chr1:START-END output.gfa \
  --reference CHM13 --mode bfs --context N --threads T
gfaidx get_region graph.indexed.gfa.gz chr1:START-END output.gfa \
  --reference CHM13 --mode bfs --context N --with_coords --threads T
```

`--contexts` accepts a comma-separated list of non-negative base counts; the
default is `100`, the gbz-base default. The same value is passed to both
tools. Context `0` restricts both tools to the reference interval nodes, which
gives a linear subgraph without the variant alleles, so it should not be the
only value reported.

`--gfaidx-threads` accepts any comma-separated list of positive integers, such
as `1,2,4,8`, and applies to every gfaidx variant. Variant labels include the
thread count:

| Variant | Comparison |
|---|---|
| `gbz-base-ref` | `reference_only` |
| `gfaidx-ref-coords-<T>t` | `reference_only` |
| `gbz-base-all` | `all_haplotypes` |
| `gfaidx-bfs-<T>t` | `all_haplotypes` (matched work) |
| `gfaidx-bfs-coords-<T>t` | `all_haplotypes` (full output) |
| `gfaidx-span-coords-<T>t` | `haplotype_span`, only with `--include-all-mode` |

Duplicate thread counts or contexts are rejected. `--include-all-mode` adds
`gfaidx --mode all --with_coords` once per interval without a context.

One execution unit is one interval at one context. Units are shuffled with a
recorded seed, and all repetitions of a unit remain together so later
repetitions can measure reuse of cached pages. Within every repetition, the
runner rotates which variant runs first; the exact position is recorded as
`order_in_block` in `raw_runs.tsv`. Outputs are deleted after their counts,
sizes, and parity hashes are recorded. Add `--keep-outputs` to retain all
extracted GFAs.

Use `--no-plots` when Matplotlib is unavailable or when only raw measurements
are wanted. The summaries can be generated later:

```bash
python3 benchmark/scripts/summarize_gfaidx_gbz_intervals.py \
  --raw /path/to/results/raw_runs.tsv \
  --output-dir /path/to/results
```

## Output parity

After each timed command finishes, the runner records two hashes of its
output. Neither contributes to the measured time or memory.

- `node_set_sha1` hashes the sorted segment identifiers. Equal hashes mean the
  two tools selected exactly the same nodes.
- `reference_walk_sha1` hashes the reference walk, independent of the
  direction in which it is written. For gbz-base, this is the walk named after
  the reference sample; every other walk is `unknown`. For gfaidx, the walk
  must match the reference sample and queried contig and, when coordinates
  were written, overlap the queried interval. The hash is `NA` unless exactly
  one walk qualifies, which can happen for gfaidx `--mode bfs` output without
  coordinates when the reference passes through the subgraph more than once.
  `reference_walks` records the number of candidates.

The summarizer pairs each gfaidx output with the gbz-base output for the same
interval, comparison, and context. Node sets are compared for both paired
comparisons. The reference-walk match is the second parity check for
`reference_only`; for `all_haplotypes` it is reported when both hashes exist.
The summarizer prints a warning when any paired output differs.

## Outputs

The result directory contains:

```text
raw_runs.tsv
metadata.tsv
logs/<query>/<variant>.ctx<N>.run_<R>.time.log
logs/<query>/<variant>.ctx<N>.run_<R>.stderr.log
tables/location_summary.tsv
tables/parity_by_query.tsv
tables/size_summary.tsv
tables/repetition_summary.tsv
tables/cache_summary.tsv
plots/*.png
plots/*.svg
```

`raw_runs.tsv` is the primary result. It has one row per variant, interval,
context, and repetition and includes:

- `comparison`, `tool` (`gfaidx` or `gbz-base`), `variant`, `context_bp`,
  `with_coords`, and `gfaidx_threads`
- elapsed seconds and maximum RSS
- user and system CPU time
- major and minor page faults
- filesystem input and output counts
- output node, edge, path, and byte counts
- `node_set_sha1`, `reference_walks`, `reference_walk_steps`, and
  `reference_walk_sha1`
- execution order, command, log paths, and exit status

Keeping every variant in one long-form table preserves the exact query,
context, and repetition pairing.

The other tables are derived from it:

- `location_summary.tsv` keeps one first-execution value and one warm value per
  exact query, context, and variant. The warm value is the median of
  repetitions 2 through N. It also carries the output counts and hashes.
- `parity_by_query.tsv` lists, for every gfaidx output in a paired comparison,
  the gbz-base and gfaidx node, edge, and path counts, reference-walk lengths,
  and whether the node sets and reference walks match (`yes`, `no`, or `NA`).
- `size_summary.tsv` averages per-location values for each comparison,
  context, interval size, and variant, with a bootstrap 95% confidence interval
  across locations. It reports first and warm time and peak RSS; mean output
  nodes, edges, paths, and bytes; gfaidx warm time and memory relative to the
  paired gbz-base run (ratios are formed per location before averaging); and
  parity as `matching/compared` in `node_set_matches` and
  `reference_walk_matches`.
- `repetition_summary.tsv` keeps every repetition number separate for each
  comparison, context, and interval size.
- `cache_summary.tsv` expresses each run relative to run 1 of the same exact
  query, then aggregates those ratios across queries.

Raw tables written by the earlier runner, which compared gfaidx
`--all_haplotypes` with gbz-base `--context 0`, can still be summarized. They
appear as a single `legacy` comparison.

## Plots

`overview_ctx<N>` draws every variant at one context on a single pair of axes,
time on the left and peak RSS on the right. gbz-base uses warm colors and
square markers, gfaidx uses cool colors, the gfaidx marker shows the thread
count, and output without coordinates is dashed. It uses warm values, or
first-execution values when the run had one repetition.

The remaining plots are written per comparison. `<comparison>` is `reference_only`,
`all_haplotypes`, or `haplotype_span`, and `<context>` is `ctx<N>` (or `noctx`
for the haplotype-span variant):

- `<comparison>_<context>_time_first_and_warm`: mean wall time and 95%
  confidence intervals against interval length
- `<comparison>_<context>_memory_first_and_warm`: mean peak RSS and confidence
  intervals
- `<comparison>_<context>_time_location_variation`: every warm per-location
  value behind the aggregate estimate
- `<comparison>_<context>_cache_effect_by_interval_size`: actual time by
  repetition for every size
- `<comparison>_<context>_output_nodes`: returned graph size for checking
  workload agreement
- `<comparison>_warm_time_by_context` and `<comparison>_warm_memory_by_context`:
  warm time and peak RSS against context length, one panel per interval size;
  written only when the run has two or more contexts
- `<comparison>_cache_effect_normalized`: repeated-query time relative to run
  1, one panel per context

gbz-base is drawn in magenta. gfaidx colors follow the thread count. Variants
that write coordinates use solid lines and filled markers, and the
matched-work `--mode bfs` variant without coordinates uses dashed lines and
open markers, so the cost of `--with_coords` is visible next to gbz-base.

PNG and SVG are written by default. Use `--plot-formats png,svg,pdf` in the
runner or `--formats png,svg,pdf` in the summarizer to change this.

## Interpreting Caching

Each invocation is a new process. A faster second execution is therefore
mostly caused by the operating system retaining index and graph pages, not by
an in-process gfaidx or gbz-base cache.

All gfaidx variants read the same files, and so do both gbz-base variants. One
variant can therefore warm pages later used by another, and a unit can benefit
from an earlier unit at the same location with a different context or
interval size. Rotating variant order and shuffling units reduce systematic
bias but do not create a truly cold cache. This is appropriate for comparing
normal repeated use; use the recorded `order_in_block` and `query_order`
columns when checking whether execution order affected a result.

Run 1 is called the **first execution**, not a guaranteed cold-cache run.
Queries run earlier in the experiment may already have cached file metadata or
overlapping data pages. Producing a strictly cold-cache measurement requires
dropping the Linux page cache before each command, which needs administrator
access and should not be done on a shared server.

For the main scaling figure, use the warm mean and confidence interval when the
goal is normal interactive use. Show first-execution values separately. The
location scatter plot should accompany the averages when regional graph
complexity is important to the interpretation.

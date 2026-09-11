# Repeated gfaidx thread-scaling and gbz-base interval benchmark

This benchmark compares coordinate-based graph extraction in `gfaidx` at one
or more thread counts and in `gbz-base`. By default, it treats `gfaidx` with 1
thread, `gfaidx` with 8 threads, and `gbz-base` as three tool variants. It
samples several genomic locations at each requested interval length and repeats
each exact query. This separates variation among graph regions from the
operating-system page-cache effect seen when the same query is executed again.

The benchmark is intentionally independent of the larger Snakemake workflow.
It consists of:

- `scripts/benchmark_gfaidx_gbz_intervals.sh`, which runs and measures queries
- `scripts/summarize_gfaidx_gbz_intervals.py`, which summarizes and plots them
- `scripts/gfaidx_gbz_intervals.example.tsv`, an example interval manifest

## Requirements

- an indexed GFA accepted by `gfaidx get_region`
- a `gbz-base` database built from the same source graph
- the `gfaidx` and `gbz-base` executables
- GNU `time`, normally `/usr/bin/time` on Linux
- Python 3 and Matplotlib for summaries and plots

The two indexes must represent the same graph version and reference paths.
Otherwise, the tools can extract different workloads even when given the same
coordinate interval. The raw table records output node, edge, path, and byte
counts so this difference remains visible.

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

Query rows are shuffled with a recorded seed. All repetitions of one exact
query remain together so later repetitions can measure reuse of cached pages.

## Running

Copy and edit the example manifest:

```bash
cp scripts/gfaidx_gbz_intervals.example.tsv intervals.tsv
```

Run five repetitions per tool variant and interval:

```bash
scripts/benchmark_gfaidx_gbz_intervals.sh \
  --gfaidx-bin /home/fawaz/projects/gfaidx/current_build/gfaidx \
  --gbz-base-bin /home/fawaz/tools/gbz-base/target/release/gbz-base \
  --gfaidx-graph /path/to/graph.indexed.gfa.gz \
  --gbz-db /path/to/graph.gbz.db \
  --queries intervals.tsv \
  --repeats 5 \
  --gfaidx-threads 1,8 \
  --out-dir /local/scratch/gfaidx_gbz_repeated
```

The commands being compared are equivalent to:

```bash
gfaidx get_region graph.indexed.gfa.gz chr1:START-END output.1t.gfa \
  --reference CHM13 --all_haplotypes --with_coords --threads 1

gfaidx get_region graph.indexed.gfa.gz chr1:START-END output.8t.gfa \
  --reference CHM13 --all_haplotypes --with_coords --threads 8

gbz-base query --sample CHM13 --contig chr1 \
  --interval START..END --context 0 graph.gbz.db
```

`--gfaidx-threads` accepts any comma-separated list of positive integers, such
as `1,2,4,8`. Each count is reported as a distinct tool label (`gfaidx-1t`,
`gfaidx-2t`, and so on). Duplicate thread counts are rejected.

The runner rotates which tool variant is first within every repetition. The
exact position is recorded as `order_in_block` in `raw_runs.tsv`. Outputs are
deleted after their counts and sizes are recorded. Add `--keep-outputs` to
retain all extracted GFAs.

Use `--no-plots` when Matplotlib is unavailable or when only raw measurements
are wanted. The summaries can be generated later:

```bash
python3 scripts/summarize_gfaidx_gbz_intervals.py \
  --raw /path/to/results/raw_runs.tsv \
  --output-dir /path/to/results
```

## Outputs

The result directory contains:

```text
raw_runs.tsv
metadata.tsv
logs/<query>/<tool-variant>.run_<N>.time.log
logs/<query>/<tool-variant>.run_<N>.stderr.log
tables/location_summary.tsv
tables/size_summary.tsv
tables/repetition_summary.tsv
tables/cache_summary.tsv
plots/*.png
plots/*.svg
```

`raw_runs.tsv` is the primary result. It has one row per tool variant,
interval, and repetition and includes:

- elapsed seconds and maximum RSS
- user and system CPU time
- major and minor page faults
- filesystem input and output counts
- output node, edge, path, and byte counts
- tool-variant name, gfaidx thread count, execution order, command, log paths,
  and exit status

Keeping the three variants in one long-form table preserves the exact query and
repetition pairing. The `tool` column separates `gfaidx-1t`, `gfaidx-8t`, and
`gbz-base`, so the summary tables and plotting script provide the same logical
separation as three files without making them difficult to join reliably.

The other tables are derived from it:

- `location_summary.tsv` keeps one first-execution value and one warm value per
  exact query and tool. The warm value is the median of repetitions 2 through N.
- `size_summary.tsv` averages those per-location values for each interval size
  and calculates a bootstrap 95% confidence interval across locations.
- `repetition_summary.tsv` keeps every repetition number separate for each
  interval size.
- `cache_summary.tsv` expresses each run relative to run 1 of the same exact
  query, then aggregates those ratios across queries.

## Plots

The plotting script writes:

- `interval_time_first_and_warm`: mean wall time and 95% confidence intervals
- `interval_memory_first_and_warm`: mean peak RSS and confidence intervals
- `interval_time_location_variation`: every warm per-location value behind the
  aggregate estimate
- `cache_effect_normalized`: repeated-query time relative to run 1
- `cache_effect_by_interval_size`: actual time by repetition for every size
- `interval_output_nodes`: returned graph size for checking workload agreement

PNG and SVG are written by default. Use `--plot-formats png,svg,pdf` in the
runner or `--formats png,svg,pdf` in the summarizer to change this.

## Interpreting Caching

Each invocation is a new process. A faster second execution is therefore
mostly caused by the operating system retaining index and graph pages, not by
an in-process gfaidx or gbz-base cache.

The gfaidx variants read the same files. A 1-thread execution can therefore
warm pages later used by an 8-thread execution, and vice versa. Rotating their
order reduces systematic bias but does not create a truly cold cache. This is
appropriate for comparing normal repeated use; use the recorded
`order_in_block` column when checking whether execution order affected a result.

Run 1 is called the **first execution**, not a guaranteed cold-cache run.
Queries run earlier in the experiment may already have cached file metadata or
overlapping data pages. Producing a strictly cold-cache measurement requires
dropping the Linux page cache before each command, which needs administrator
access and should not be done on a shared server.

For the main scaling figure, use the warm mean and confidence interval when the
goal is normal interactive use. Show first-execution values separately. The
location scatter plot should accompany the averages when regional graph
complexity is important to the interpretation.

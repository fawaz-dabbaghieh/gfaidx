# Subgraph And Interval Extraction Optimizations

This document records the extraction work performed on 2026-08-11 with the
HPRC minigraph-cactus chr1 and PGGB chr22 indexes in
`/home/user3/optimize_gfaidx`. The benchmark resource logs are retained under
`/home/user3/optimize_gfaidx/benchmarks/<version>/`. Versions 1.9.1 through
1.9.3 optimize serial algorithms; version 1.9.4 adds ordered parallel output.

## Benchmark inputs

| Graph | Indexed GFA | Path syntax |
| --- | --- | --- |
| minigraph-cactus chr1 | `hprc2_mc_chr1/hprc_v2_mc_chr1.indexed.gfa.gz` | W |
| PGGB chr22 | `hprc2_pggb_chr22/20251014_hprc25272.p98-k311.chr22.indexed.gfa.gz` | P |

The machine had 20 logical CPUs and 121 GiB RAM. Each extraction used one
gfaidx process and GNU `time -v`. Version 1.9.4 was tested with 1, 2, 4, and 8
P/W formatting threads. Benchmarks were run sequentially to avoid intentional
storage contention. Filesystem cache state was not reset, so very small timing
differences should be treated as noise.

## Queries

### Node-based extraction

The seed nodes came from S records in the already saved region outputs. Both
queries use a 1,000-node BFS cap and coordinate-bearing path output.

```bash
gfaidx get_subgraph \
  hprc2_pggb_chr22/20251014_hprc25272.p98-k311.chr22.indexed.gfa.gz \
  3771302 out.gfa --max_nodes 1000 --with_coords --threads N

gfaidx get_subgraph \
  hprc2_mc_chr1/hprc_v2_mc_chr1.indexed.gfa.gz \
  2931334 out.gfa --max_nodes 1000 --with_coords --threads N
```

The PGGB query emits 1,000 S, 1,924 L, and 1,229,332 P records in a 250 MB
GFA. The minigraph-cactus query emits 1,000 S, 1,381 L, and 464 W records in a
2.5 MB GFA.

### Region extraction

The principal stress test is the existing PGGB 5 kb all-haplotype query. The
minigraph-cactus query covers 1 Mb and exercises W output.

```bash
gfaidx get_region \
  hprc2_pggb_chr22/20251014_hprc25272.p98-k311.chr22.indexed.gfa.gz \
  CHM13#0#chr22:4000000-4005000 out.gfa \
  --all_haplotypes --with_coords --threads N

gfaidx get_region \
  hprc2_mc_chr1/hprc_v2_mc_chr1.indexed.gfa.gz \
  chr1:36000000-37000000 out.gfa \
  --reference CHM13 --all_haplotypes --with_coords --threads N
```

The PGGB query selects 3,266 reference nodes, decodes 78,104,201 postings, and
matches 778 paths. Conservative outermost-anchor bounds cover 1,303,313,151
path steps, which collapse to 2,150,883 unique graph nodes. The result has
2,150,883 S, 3,245,519 L, and 778 P records and occupies about 12 GB.

The minigraph-cactus query selects 27,419 reference nodes, decodes 12,114,478
postings, and matches 464 paths. It covers 12,752,164 path steps that collapse
to 42,296 unique nodes. The output is about 101 MB.

## Results

All memory values below are GNU `time -v` maximum RSS values converted from kB
to decimal GB. Each reported process exited successfully.

### PGGB 5 kb all-haplotype stress test

| Version | Threads | Wall time (s) | User + system (s) | Peak RSS (GB) | Change from 1.9.0 |
| --- | ---: | ---: | ---: | ---: | --- |
| 1.9.0 | 1 | 169.96 | 168.68 | 6.698 | baseline |
| 1.9.1 | 1 | 128.80 | 127.69 | 0.666 | 24.2% faster, 90.1% less RSS |
| 1.9.2 | 1 | 75.73 | 75.59 | 0.761 | 55.4% faster, 88.6% less RSS |
| 1.9.3 | 1 | 49.72 | 46.45 | 0.547 | 70.7% faster, 91.8% less RSS |
| 1.9.4 | 1 | 48.28 | 48.21 | 0.550 | 71.6% faster, 91.8% less RSS |
| 1.9.4 | 2 | 44.74 | 66.03 | 0.781 | 73.7% faster, 88.3% less RSS |
| 1.9.4 | 4 | 29.51 | 63.12 | 0.934 | 82.6% faster, 86.1% less RSS |
| 1.9.4 | 8 | 22.23 | 64.35 | 1.252 | 86.9% faster, 81.3% less RSS |

Version 1.9.4 with eight threads is 7.65 times faster than 1.9.0, 2.24
times faster than 1.9.3, and 2.17 times faster than its own one-thread run.

### Other queries

| Query | 1.9.0 wall / RSS | 1.9.1 wall / RSS | 1.9.2 wall / RSS | 1.9.3 wall / RSS |
| --- | --- | --- | --- | --- |
| PGGB node, 1,000 | 4.90 s / 258 MB | 4.68 s / 259 MB | 4.28 s / 259 MB | 4.44 s / 258 MB |
| MC node, 1,000 | 0.04 s / 113 MB | 0.05 s / 113 MB | 0.05 s / 113 MB | 0.05 s / 113 MB |
| MC region, 1 Mb | 1.09 s / 318 MB | 0.80 s / 268 MB | 0.65 s / 268 MB | 0.67 s / 267 MB |

The node-query changes are small because versions 1.9.1 and 1.9.3 target
large all-haplotype rank accumulation and large distinct-name caches. Version
1.9.2 helps long records more than the many short records in the PGGB node
query. The 1.9.2/1.9.3 difference on the small queries is measurement noise.

## Version 1.9.1: dense selected-node ranks and phase timers

The previous all-haplotype algorithm appended one uint32 node rank for every
selected path step and then sorted and deduplicated the complete vector. The
PGGB stress test therefore retained 1.303 billion temporary ranks to produce
only 2.151 million unique ranks.

Version 1.9.1 replaces that occurrence-sized vector with a dense bitset indexed
by node rank. Every reference and selected path-step rank sets one bit. A final
ascending scan of the bitset emits the same sorted unique vector expected by
materialization. Temporary selection memory is now proportional to graph node
count rather than selected path-step count. No index format changed.

This round also adds timers for:

- posting decoding;
- selected path-step scanning;
- sorted rank materialization from the bitset;
- graph S/L materialization;
- subpath interval discovery;
- coordinate-index setup; and
- combined P/W coordinate calculation, formatting, and output.

On the PGGB stress test, the new selection phases took 0.444 s for postings,
1.811 s for selected steps, and 0.010 s for rank materialization. The P/W
output phase then accounted for 119.855 s of the 128.80 s total, identifying
the next bottleneck.

## Version 1.9.2: one buffered write per P/W record

Coordinate-bearing output previously invoked formatted stream insertion for
every node name, orientation, and delimiter. The PGGB stress test has only 778
P records, but those records collectively contain 1.303 billion path steps.

Version 1.9.2 builds one output string for each already materialized path slice
and writes that string to the stream once. Only one record is buffered at a
time, preserving deterministic path order and bounding extra memory by the
largest emitted record. Selection and coordinate algorithms are unchanged.
The explicit stream check also reports a failed large-record write immediately.

The PGGB P/W output phase fell from 119.855 s to 66.735 s. Peak RSS rose from
0.666 GB to 0.761 GB because the largest record buffer exists alongside its
step slice, while remaining far below the 6.698 GB baseline.

## Version 1.9.3: adaptive rank-addressed node-name cache

`PathIndexReader::get_node_name()` originally used an unordered map for every
lookup. This is efficient for a small sparse query, but the PGGB output loop
performed about 1.303 billion hash lookups. Name misses also populated a second
node-metadata hash table even though output does not reuse that metadata.

Version 1.9.3 keeps the sparse hash cache for the first 65,536 distinct node
names. At that threshold it promotes the cache to:

- a uint32 array mapping graph node rank to a compact dense-name id; and
- a deque containing only names actually loaded by the query.

The deque keeps returned string views stable as new names are appended. The
rank table avoids allocating one string object for every graph node. Name
misses read their metadata without retaining it in the posting-oriented
metadata cache. Small queries never allocate the rank table.

The PGGB P/W output phase fell from 66.735 s to 41.671 s, and peak RSS fell
from 0.761 GB to 0.547 GB. The ordinary node queries did not cross the promotion
threshold and retained their previous memory use.

## Version 1.9.4: bounded ordered P/W formatting threads

Version 1.9.4 adds `--threads N` to both `get_subgraph` and `get_region`.
The accepted range is 1 through 256 and the default remains 1, so existing
commands retain the version 1.9.3 serial behavior. The option applies only to
P/W coordinate calculation and record formatting. BFS, all-haplotype
selection, S/L materialization, and the final output stream remain serial.

The parallel path has the following design:

- each worker owns a `PathIndexReader`, so mutable `ifstream` seek/read state is
  never shared;
- the calling reader loads all selected node names before workers start, then
  freezes that cache for immutable concurrent lookups instead of duplicating a
  potentially multi-million-name cache per worker;
- short runs are grouped into jobs of at most 65,536 path steps or 4,096
  records, avoiding millions of synchronization operations for node queries;
- a run that already exceeds the step target remains one job, preserving
  parallelism for chromosome-scale P/W records;
- a ring contains at most one in-flight job buffer per effective worker, so
  memory is bounded by the worker count and the largest bounded jobs; and
- the main thread consumes ring slots in sequence order, emits warnings in the
  same order, performs the only writes to the GFA stream, propagates worker
  exceptions, and joins every worker on success or failure.

The effective worker count is the smaller of the requested count and the
number of formatting jobs. The PGGB node query turns 1,229,332 short P records
into 348 jobs. The PGGB interval query retains 776 jobs for 778 long records,
so batching removes short-record overhead without merging most large records.

### Version 1.9.4 thread sweep: PGGB 5 kb interval

The P/W phase includes the one-time immutable node-name cache preparation for
parallel runs (about 2.4 seconds here).

| Threads | Wall (s) | P/W phase (s) | CPU use | Peak RSS (GB) | Speedup vs 1 thread |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 48.28 | 39.974 | 99% | 0.550 | 1.00x |
| 2 | 44.74 | 36.396 | 147% | 0.781 | 1.08x |
| 4 | 29.51 | 21.157 | 213% | 0.934 | 1.64x |
| 8 | 22.23 | 13.833 | 289% | 1.252 | 2.17x |

Eight threads provide the best tested result on this 20-CPU machine. The
larger RSS is expected: several long output records can exist concurrently,
but the ring prevents the number of such buffers from growing with path count.

### Version 1.9.4 thread sweep: node extraction

| Graph | Threads | Wall (s) | P/W phase (s) | Peak RSS (MB) | Speedup vs 1 thread |
| --- | ---: | ---: | ---: | ---: | ---: |
| PGGB | 1 | 4.33 | 3.346 | 259 | 1.00x |
| PGGB | 2 | 2.71 | 1.786 | 260 | 1.60x |
| PGGB | 4 | 1.85 | 0.885 | 260 | 2.34x |
| PGGB | 8 | 1.35 | 0.450 | 260 | 3.21x |
| minigraph-cactus | 1 | 0.05 | 0.010 | 113 | 1.00x |
| minigraph-cactus | 2 | 0.05 | 0.021 | 120 | 1.00x |
| minigraph-cactus | 4 | 0.05 | 0.020 | 123 | 1.00x |
| minigraph-cactus | 8 requested / 5 effective | 0.06 | 0.020 | 127 | 0.83x |

Batching is important for the PGGB node query: a preliminary per-record queue
made synchronization dominate, whereas the final 348-job implementation
scales to 3.21x at eight threads. The minigraph-cactus node query is only 0.05
seconds serially, so thread startup and cache preparation cannot pay back.

### Version 1.9.4 thread sweep: minigraph-cactus 1 Mb interval

| Threads | Wall (s) | P/W phase (s) | Peak RSS (MB) | Speedup vs 1 thread |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 0.60 | 0.345 | 267 | 1.00x |
| 2 | 0.56 | 0.299 | 275 | 1.07x |
| 4 | 0.46 | 0.213 | 282 | 1.30x |
| 8 | 0.39 | 0.148 | 298 | 1.54x |

### Linux and macOS portability

The implementation uses only C++17 `std::thread`, `std::mutex`, and
`std::condition_variable`. CMake resolves the platform thread support with
`find_package(Threads REQUIRED)` and links `Threads::Threads`; there are no
direct pthread calls, Linux-only scheduling APIs, or platform-specific atomics.
This lets CMake select the correct compiler/linker flags for Linux and macOS.
The source was built and tested with GCC on Linux; a macOS Clang CI job remains
the recommended way to continuously verify that platform.

## Correctness validation

Versions 1.9.1 through 1.9.3 were validated as follows:

- all seven CTest regression tests passed;
- the minigraph-cactus region output was byte-identical to the fresh 1.9.0
  output and the pre-existing `chr1_1mb.gfa`;
- both node-query outputs were byte-identical to fresh 1.9.0 outputs; and
- the complete 12 GB PGGB output was byte-identical to the pre-existing
  `5kb_region.gfa` and to the preceding optimized version.

Version 1.9.4 added parallel-specific validation:

- regression tests compare one- and four-thread output for P and W coordinate
  formatting, non-coordinate fallback formatting, BFS, and interval extraction;
- all four PGGB interval outputs were byte-identical to `5kb_region.gfa`;
- all four minigraph-cactus interval outputs were byte-identical to
  `chr1_1mb.gfa`, and each parallel node output matched its fresh serial output;
- the release and AddressSanitizer/UndefinedBehaviorSanitizer builds passed all
  seven CTest tests.

The comparisons used `cmp`, not record counts or normalized GFA output.
ThreadSanitizer compiled, but its runtime stopped before executing the tests with
`unexpected memory mapping` on this ARM Linux host. That is a host/runtime
limitation rather than a reported race; TSan should still be run in supported CI.

## Remaining bottleneck and semantic decision

At version 1.9.4 with eight threads, 13.833 s of the 22.23 s stress-test runtime
is still coordinate calculation, formatting, and ordered output. The final GFA
writer remains intentionally serial, as do selection and graph materialization;
serial cache preparation and writing 12 GB now limit further thread scaling.

The best thread count depends on record size and query duration. On this
20-logical-CPU machine, eight threads won for both substantial queries, while
four threads already captured much of the gain with lower memory. Tiny queries
should keep the default of one thread because startup overhead can dominate.

The unexpectedly large output is caused by current `--all_haplotypes`
semantics, not by BFS or coordinate-index lookup. For each non-reference path,
the algorithm retains every step between the minimum and maximum occurrence of
any queried reference node. Repeated anchors can therefore span most of a path
even for a 5 kb reference interval.

A compact local-haplotype mode would require a deliberate semantic choice, for
example clustering anchor occurrences or selecting a best collinear anchor
chain. Such a mode should be opt-in until its behavior for repeats, inversions,
and duplications is specified.

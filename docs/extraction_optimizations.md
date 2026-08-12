# Subgraph And Interval Extraction Optimizations

This document records the extraction work performed on 2026-08-11 and
2026-08-12 with the
HPRC minigraph-cactus chr1 and PGGB chr22 indexes in
`/home/user3/optimize_gfaidx`. The benchmark resource logs are retained under
`/home/user3/optimize_gfaidx/benchmarks/<version>/`. Versions 1.9.1 through
1.9.3 optimize serial algorithms; version 1.9.4 adds ordered parallel output;
version 1.9.5 removes duplicate name ownership and copies; version 1.9.6 fuses
checkpoint coordinate boundaries with direct formatting for large records;
version 1.9.7 adds opt-in gap-limited local haplotype intervals for repeat-rich
graphs while leaving the established minimum/maximum behavior as the default.

## Benchmark inputs

| Graph | Indexed GFA | Path syntax |
| --- | --- | --- |
| minigraph-cactus chr1 | `hprc2_mc_chr1/hprc_v2_mc_chr1.indexed.gfa.gz` | W |
| PGGB chr22 | `hprc2_pggb_chr22/20251014_hprc25272.p98-k311.chr22.indexed.gfa.gz` | P |

The machine had 20 logical CPUs and 121 GiB RAM. Each extraction used one
gfaidx process and GNU `time -v`. Versions 1.9.4 through 1.9.6 were tested
with 1, 2, 4, and 8 P/W formatting threads. The version 1.9.7 gap sweep used one
thread to isolate selection behavior; one PGGB case was repeated with eight
threads for byte determinism. Benchmarks were run sequentially to avoid
intentional storage contention. Filesystem cache state was not reset, so very
small timing differences should be treated as noise.

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
| 1.9.5 | 1 | 50.37 | 50.31 | 0.382 | 70.4% faster, 94.3% less RSS |
| 1.9.5 | 2 | 34.43 | 48.51 | 0.680 | 79.7% faster, 89.8% less RSS |
| 1.9.5 | 4 | 24.83 | 51.74 | 0.800 | 85.4% faster, 88.1% less RSS |
| 1.9.5 | 8 | 18.61 | 50.44 | 1.180 | 89.1% faster, 82.4% less RSS |
| 1.9.6 | 1 | 43.75 | 43.66 | 0.382 | 74.3% faster, 94.3% less RSS |
| 1.9.6 | 2 | 31.57 | 46.37 | 0.557 | 81.4% faster, 91.7% less RSS |
| 1.9.6 | 4 | 24.53 | 47.55 | 0.698 | 85.6% faster, 89.6% less RSS |
| 1.9.6 | 8 | 18.64 | 48.00 | 1.010 | 89.0% faster, 84.9% less RSS |

Version 1.9.6 with eight threads is 9.12 times faster than 1.9.0 and 2.67
times faster than 1.9.3. Its 18.64-second result is effectively equal to the
version 1.9.5 eight-thread sample while using 14.4% less peak RSS; at one thread
version 1.9.6 is 13.1% faster than version 1.9.5.

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

## Version 1.9.5: one node-name owner and direct record handoff

Graph materialization already loaded an owned string for every selected node.
Version 1.9.4 then reread the same names from `.pdx` before parallel P/W output
and retained a second copy in `PathIndexReader`'s frozen cache. On the PGGB
stress query, that serial preparation took about 2.4 seconds for 2,150,883
names and overlapped in memory with the graph membership set.

Version 1.9.5 keeps the original selected-name vector as the only string owner:

- graph S/L filtering uses an `unordered_set<string_view>` whose views point
  into that stable vector;
- the graph membership hash is released immediately after S/L output, before
  coordinate slices and formatted P/W buffers are allocated;
- `SelectedNodeNameLookup` maps node rank to an index in the owning vector and
  stores no node-name bytes itself;
- selections below 65,536 names use a sparse rank map, avoiding a graph-sized
  allocation for small queries, while larger selections use a direct uint32
  rank table for the hot per-step lookup; and
- the lookup is built before workers start and is immutable during formatting,
  preserving the version 1.9.4 thread-safety contract without shared file I/O.

The stress-query lookup now takes about 0.10 seconds. The same lookup is also
used by the one-thread path, so serial extraction avoids constructing a second
name cache. Its normal-output wall measurement was 4.3% slower than the single
version 1.9.4 sample, but peak RSS fell by 30.6%; filesystem cache and a 12 GB
write make a difference of this size inconclusive without repeated trials.

This version also removes an avoidable long-record copy. Formatters fill a
caller-owned record buffer; the first record in a job swaps its allocation into
the ordered result slot instead of being appended into a second string. Batched
short-record jobs reuse a worker scratch buffer. Completed large slots are
released after writing rather than retaining their capacities indefinitely.
Finally, the ordered writer calls `notify_one` when one queue position opens;
shutdown and failure paths retain `notify_all` because every waiter must stop.
No index format, extraction semantics, record order, or output bytes changed.

### Version 1.9.5 thread sweep: PGGB 5 kb interval

| Threads | Wall (s) | P/W phase (s) | CPU use | Peak RSS (GB) | Speedup vs 1 thread | Change vs 1.9.4 |
| ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 1 | 50.37 | 42.223 | 99% | 0.382 | 1.00x | 4.3% slower, 30.6% less RSS |
| 2 | 34.43 | 24.274 | 140% | 0.680 | 1.46x | 23.0% faster, 12.9% less RSS |
| 4 | 24.83 | 15.424 | 208% | 0.800 | 2.03x | 15.9% faster, 14.3% less RSS |
| 8 | 18.61 | 8.501 | 271% | 1.180 | 2.71x | 16.3% faster, 5.7% less RSS |

At eight threads the combined user and system time also fell from 64.35 to
50.44 seconds. This is consistent with removing duplicate name reads and the
second long-record copy rather than merely shifting work among threads.

### Version 1.9.5 thread sweep: node extraction

| Graph | Threads | Wall (s) | P/W phase (s) | Peak RSS (MB) | Speedup vs 1 thread |
| --- | ---: | ---: | ---: | ---: | ---: |
| PGGB | 1 | 4.40 | 3.423 | 259 | 1.00x |
| PGGB | 2 | 2.72 | 1.773 | 259 | 1.62x |
| PGGB | 4 | 1.94 | 0.897 | 259 | 2.27x |
| PGGB | 8 | 1.51 | 0.463 | 259 | 2.91x |
| minigraph-cactus | 1 | 0.03 | 0.008 | 113 | 1.00x |
| minigraph-cactus | 2 | 0.05 | 0.027 | 121 | 0.60x |
| minigraph-cactus | 4 | 0.06 | 0.021 | 123 | 0.50x |
| minigraph-cactus | 8 requested / 5 effective | 0.04 | 0.019 | 128 | 0.75x |

The PGGB node results are close to version 1.9.4; variations of 0.01 to 0.16
seconds are small relative to graph setup and output. The 0.03-second
minigraph-cactus serial query remains too small to benefit from worker startup.

### Version 1.9.5 thread sweep: minigraph-cactus 1 Mb interval

| Threads | Wall (s) | P/W phase (s) | Peak RSS (MB) | Speedup vs 1 thread |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 0.52 | 0.263 | 265 | 1.00x |
| 2 | 0.56 | 0.270 | 275 | 0.93x |
| 4 | 0.42 | 0.143 | 284 | 1.24x |
| 8 | 0.37 | 0.097 | 301 | 1.41x |


## Version 1.9.6: adaptive checkpoint/formatting fusion

Version 1.9.5 used a checkpoint only for the start of a selected path interval.
It then scanned from that checkpoint through the complete interval, looked up
every selected node length, appended every selected `StepRecord` to a temporary
`CoordinateSlice`, and traversed that vector again to format the P/W record.
For chromosome-scale records, the selected range dominated both coordinate I/O
and temporary memory traffic.

Version 1.9.6 separates the two coordinate boundaries from record formatting:

- the start coordinate is recovered from the checkpoint at or before
  `start_step`, followed by at most `checkpoint_stride - 1` node-length reads;
- the exclusive end coordinate is recovered the same way at
  `start_step + step_count`;
- after both coordinates are known, selected steps stream directly from `.pdx`
  into the final P/W string, with node name, orientation, and P delimiters added
  in one pass; and
- W `SeqStart`/`SeqEnd`, encoded P coordinate offsets, overlap rejection,
  warnings, tags, output order, and fallback bytes retain their existing rules.

The new route is deliberately adaptive. It is selected only when `.pcx` is
valid and the interval contains at least two checkpoint strides. Shorter
records retain the version 1.9.5 single-scan `CoordinateSlice` path because two
endpoint scans plus a formatting scan cost more than one small slice scan.
Graphs without `.pcx` also retain that established fallback. No index format or
command-line behavior changed.

This crossover was measured rather than assumed. Applying endpoint fusion to
every record slowed the 1,229,332-record PGGB node query from 4.40 to 6.56
seconds at one thread because range-call overhead dominated. The adaptive build
restored it to 4.47 seconds while retaining fusion for the long interval
records.

### Version 1.9.6 thread sweep: PGGB 5 kb interval

| Threads | Wall (s) | P/W phase (s) | CPU use | Peak RSS (GB) | Speedup vs 1 thread | Change vs 1.9.5 |
| ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 1 | 43.75 | 35.959 | 99% | 0.382 | 1.00x | 13.1% faster |
| 2 | 31.57 | 23.392 | 146% | 0.557 | 1.39x | 8.3% faster, 18.1% less RSS |
| 4 | 24.53 | 13.987 | 193% | 0.698 | 1.78x | 1.2% faster, 12.8% less RSS |
| 8 | 18.64 | 8.428 | 257% | 1.010 | 2.35x | same wall time, 14.4% less RSS |

At one thread, the P/W phase fell 14.8% from 42.223 to 35.959 seconds. At eight
threads the 0.03-second wall difference is noise, but the P/W phase still fell
from 8.501 to 8.428 seconds and peak RSS dropped by 170 MB. The unchanged 12 GB
output is increasingly the limiting cost at higher thread counts.

GNU `time -v` also shows fewer minor faults in the parallel long-record runs.
Major faults and filesystem-input counts were near zero and cache-dependent;
filesystem output stayed at about 23.8 million kB because output bytes did not
change.

| Threads | Minor faults, 1.9.5 -> 1.9.6 | Voluntary switches, 1.9.5 -> 1.9.6 | Involuntary switches, 1.9.5 -> 1.9.6 |
| ---: | ---: | ---: | ---: |
| 1 | 99,028 -> 91,552 | 3 -> 17 | 486 -> 429 |
| 2 | 1,316,186 -> 761,153 | 2,312 -> 1,495 | 601 -> 352 |
| 4 | 1,358,961 -> 929,998 | 1,978 -> 2,590 | 388 -> 276 |
| 8 | 1,511,801 -> 904,910 | 3,441 -> 2,501 | 285 -> 276 |

Context switches fluctuate with scheduling and should not be interpreted from
one sample in isolation. The consistent 31-42% minor-fault reduction at two to
eight threads agrees with removing live temporary step vectors from long jobs.

### Version 1.9.6 thread sweep: node extraction

| Graph | Threads | Wall (s) | P/W phase (s) | Peak RSS (MB) | Speedup vs 1 thread |
| --- | ---: | ---: | ---: | ---: | ---: |
| PGGB | 1 | 4.47 | 3.358 | 259 | 1.00x |
| PGGB | 2 | 2.80 | 1.798 | 259 | 1.60x |
| PGGB | 4 | 1.97 | 0.887 | 259 | 2.27x |
| PGGB | 8 | 1.48 | 0.458 | 259 | 3.02x |
| minigraph-cactus | 1 | 0.03 | 0.008 | 113 | 1.00x |
| minigraph-cactus | 2 | 0.04 | 0.018 | 121 | 0.75x |
| minigraph-cactus | 4 | 0.04 | 0.018 | 126 | 0.75x |
| minigraph-cactus | 8 requested / 5 effective | 0.06 | 0.021 | 128 | 0.50x |

These short-record results remain within normal run-to-run variation from
version 1.9.5, confirming that the crossover avoids the all-fused regression.

### Version 1.9.6 thread sweep: minigraph-cactus 1 Mb interval

| Threads | Wall (s) | P/W phase (s) | Peak RSS (MB) | Speedup vs 1 thread |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 0.53 | 0.200 | 257 | 1.00x |
| 2 | 0.47 | 0.188 | 266 | 1.13x |
| 4 | 0.41 | 0.132 | 276 | 1.29x |
| 8 | 0.37 | 0.085 | 293 | 1.43x |

The total time is sub-second and noisy, but the one-thread P/W phase fell 24.0%
from 0.263 seconds and the output remained byte-identical to `chr1_1mb.gfa`.

### Version 1.9.6 minigraph-cactus 10 Mb stress windows

Three larger chr1 windows exercise different graph regions. The comparison was
repeated after warming graph data so version 1.9.5 did not always take the cold
cache. Each result below is one normal-output sample; the output-size column is
decimal GB.

| Window | Selected nodes / communities | Output (GB) | 1.9.5 t1 wall / P/W (s) | 1.9.6 t1 wall / P/W (s) | t1 wall change | 1.9.6 t8 wall / P/W (s) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| chr1:5,000,000-15,000,000 | 565,498 / 165 | 1.276 | 6.39 / 3.116 | 5.65 / 2.320 | 11.6% faster | 3.97 / 0.754 |
| chr1:120,000,000-130,000,000 | 1,077,032 / 298 | 0.941 | 6.79 / 3.427 | 6.60 / 3.082 | 2.8% faster | 4.12 / 0.871 |
| chr1:220,000,000-230,000,000 | 442,163 / 134 | 1.002 | 5.55 / 2.828 | 4.64 / 1.958 | 16.4% faster | 3.29 / 0.892 |

The denser centromeric window has almost 2.5 times as many selected nodes as the
q-arm window and shows the smallest total-time improvement, but its P/W phase
still improves 10.1%. The p- and q-arm P/W phases improve 25.5% and 30.8%.
All three version 1.9.6 serial outputs were byte-identical to the preserved
version 1.9.5 binary, and all eight-thread outputs matched their serial output.

## Version 1.9.7: optional ODGI-style gap-limited intervals

The conservative all-haplotype rule is useful on many graphs, especially the
minigraph-cactus input above: each non-reference path retains every step from
its minimum to maximum occurrence of a reference node. On repeat-rich PGGB
graphs, however, one distant occurrence can widen a 50 kb query to most of a
chromosome path. Version 1.9.7 adds `--haplotype_gap LIMIT` as an explicit
alternative. Omitting the option executes the version 1.9.6 min/max path
unchanged and does not open `.lnx` or allocate local-anchor state.

The optional algorithm works as follows:

- reference-node postings still identify every anchor occurrence and aggregate
  each path's outer bounds;
- a dense bitset over absolute packed `.pdx` steps marks anchor occurrences,
  avoiding an occurrence-sized vector and global sort for up to 100 million
  postings in this benchmark;
- each matched non-reference path is scanned once between its outer anchors;
  consecutive anchors remain in the current run while the summed `.lnx`
  lengths of intervening non-anchor nodes are at most the requested gap;
- after the gap is exceeded, the next anchor begins another `SubpathRun`, and
  only nodes in the retained runs enter the materialized graph union; and
- coordinate-selected reference runs override this process and remain exact.

This is deliberately described as ODGI-style gap limiting rather than a best
alignment or collinear-chain inference. It clusters nearby path occurrences in
one pass and can emit a singleton-anchor run. It does not perform iterative
graph-context expansion. Limits accept bare bases and case-insensitive decimal
`bp`, `kb`, `mb`, and `gb` suffixes. An explicit zero joins adjacent anchors
only; the omitted option remains semantically different.

### PGGB chr22 50 kb gap sweep

This test used `CHM13#0#chr22:4000000-4050000`, one formatting thread, normal
GFA output, and the regenerated `50kb_region_time_1.9.6.gfa` as the default
byte reference. Selection time is the sum of logged posting, selected-step,
and rank-materialization phases. Output sizes and RSS are decimal units.

| Gap policy | Wall (s) | Selection (s) | Unique nodes | Selected path steps | P records | Split paths | Output (GB) | Peak RSS (GB) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| default min/max | 49.78 | 2.043 | 2,150,991 | 1,303,360,559 | 778 | 0 | 12.202 | 0.383 |
| 1 kb | 7.40 | 4.998 | 27,428 | 135,670,699 | 12,871 | 774 | 1.223 | 0.374 |
| 10 kb | 8.02 | 5.057 | 52,496 | 146,330,132 | 6,990 | 774 | 1.320 | 0.376 |
| 100 kb | 8.87 | 5.925 | 167,634 | 196,093,161 | 3,814 | 708 | 1.773 | 0.378 |

The 1 kb policy is 6.73 times faster than the default and writes 90.0% fewer
bytes. The 10 kb and 100 kb samples are 6.21 and 5.61 times faster. Local
selection itself is about 2.4-2.9 times slower because it initializes the
anchor bitset and scans the outer path ranges with node lengths. The total
command wins because far fewer unique graph nodes, edges, and path-step bytes
are materialized. At 1 kb, 12,871 local P records still encode 135.7 million
step occurrences, explaining why the 27,428-node graph remains 1.22 GB.

The default version 1.9.7 output was byte-identical to both regenerated 1.9.0
and 1.9.6 outputs. The 10 kb result was also byte-identical between one and
eight threads; the eight-thread run took 6.34 s, but selection remained serial
and dominated its shorter output, so the main table reports the cleaner
one-thread sweep.

### Minigraph-cactus chr1 1 Mb control

The existing `chr1:36000000-37000000` CHM13 query shows why local clustering
is optional rather than the new default.

| Gap policy | Wall (s) | Selection (s) | Unique nodes | Selected path steps | W records | Split paths | Output (MB) | Peak RSS (MB) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| default min/max | 0.45 | 0.162 | 42,296 | 12,752,164 | 464 | 0 | 105.008 | 257 |
| 1 kb | 0.71 | 0.227 | 42,266 | 12,752,122 | 471 | 7 | 104.990 | 428 |
| 10 kb | 0.67 | 0.222 | 42,296 | 12,752,164 | 464 | 0 | 105.008 | 428 |
| 100 kb | 0.63 | 0.223 | 42,296 | 12,752,164 | 464 | 0 | 105.008 | 428 |

At 10 kb and 100 kb no path is split, so both outputs are byte-identical to the
default while wall time rises 40-49% and peak RSS rises about 171 MB. That
memory is the graph-wide packed-step anchor bitset; PGGB's smaller selected
records offset it in the process peak, whereas this minigraph-cactus query has
no such saving. The 1 kb policy changes only seven paths and 42 selected steps,
which is not enough to recover its setup cost. Users should therefore keep the
default for graphs where outer anchors already describe local haplotypes and
enable a gap only when repeat widening is visible.

## Rejected low-risk experiments

These ablations used the PGGB 5 kb query with eight threads and `/dev/null` as
the output, so they isolate CPU and memory behavior but are not directly
comparable to the normal-output tables above.

| Build / buffer policy | Wall (s) | Peak RSS (GB) | Decision |
| --- | ---: | ---: | --- |
| ordinary Release, release completed buffers | 15.90 | 1.123 | retained as final design |
| ordinary Release, retain ring buffer capacities | 14.82 | 1.688 | rejected: 50.3% more RSS for 6.8% less wall time |
| Release LTO, release completed buffers | 17.82 | 1.242 | rejected: 12.1% slower and 10.6% more RSS |

LTO was tested with CMake's supported interprocedural-optimization mechanism,
but the measured regression means versions 1.9.5 and 1.9.6 keep the ordinary
Release
build on both Linux and macOS. Retaining every historical long-record capacity
was also rejected; only the bounded current jobs and worker scratch buffers
remain live.

## Linux and macOS portability

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

Version 1.9.5 validation repeated the same checks after changing name ownership
and worker buffer handoff:

- all four PGGB interval outputs were byte-identical to `5kb_region.gfa`;
- all four minigraph-cactus interval outputs were byte-identical to
  `chr1_1mb.gfa`;
- every parallel PGGB and minigraph-cactus node output matched its fresh serial
  output; and
- both Release and AddressSanitizer/UndefinedBehaviorSanitizer builds passed all
  seven CTest tests.

Version 1.9.6 validation additionally covered the adaptive coordinate path:

- the repeated-anchor fixture rebuilds `.pcx` with a two-step stride, exercising
  exact and between-checkpoint start/end boundaries in both serial and
  four-thread P formatting;
- that fixture then removes `.pcx` and verifies the legacy `CoordinateSlice`
  fallback independently;
- all PGGB and minigraph-cactus standard thread-sweep outputs retained the same
  byte comparisons as version 1.9.5;
- three 10 Mb minigraph-cactus windows matched the preserved version 1.9.5
  binary at one thread, and each eight-thread output matched version 1.9.6
  serial output; and
- final Release and AddressSanitizer/UndefinedBehaviorSanitizer builds passed
  all seven CTest tests.

Version 1.9.7 validation adds local multi-run and compatibility coverage:

- the repeated-anchor fixture checks exact local P runs for zero and one-base
  gaps, case-insensitive `bp`/`kb` parsing, invalid flag combinations, and
  byte-identical one- versus four-thread output;
- the default 50 kb PGGB output matches the user-provided version 1.9.6 file,
  and those regenerated version 1.9.0 and 1.9.6 files match each other;
- a real 10 kb-gap PGGB output is byte-identical at one and eight threads;
- the default minigraph-cactus output matches the preserved version 1.9.6
  binary, while its unsplit 10 kb and 100 kb outputs match the default; and
- final Release and AddressSanitizer/UndefinedBehaviorSanitizer builds pass all
  seven CTest tests.

The comparisons used `cmp`, not record counts or normalized GFA output.
ThreadSanitizer compiled, but its runtime stopped before executing the tests with
`unexpected memory mapping` on this ARM Linux host. That is a host/runtime
limitation rather than a reported race; TSan should still be run in supported CI.

## Remaining bottleneck and semantic decision

At version 1.9.6 with eight threads, 8.428 s of the 18.64 s PGGB
stress-test runtime is coordinate calculation, formatting, and ordered output.
The final GFA writer remains intentionally serial, as do selection and graph
materialization. Writing 12 GB and resolving/formatting the remaining selected
steps now dominate further scaling; increasing the worker count beyond eight
is unlikely to help unless the output path changes too.

The next contained candidate is an mmap-backed or bulk-offset reader for the
`.pdx` step table. A syscall profile of the PGGB node query observed about
1.238 million `read` calls and 1.238 million `lseek` calls. Version 1.9.6 avoids
full selected-range length reads for large records, but millions of short
records still use the one-scan slice path and pay per-range reader overhead.

The best thread count depends on record size and query duration. On this
20-logical-CPU machine, eight threads won for both substantial queries, while
four threads already captured much of the gain with lower memory. Tiny queries
should keep the default of one thread because startup overhead can dominate.

The unexpectedly large default output is caused by conservative
`--all_haplotypes` semantics, not by BFS or coordinate-index lookup. For each
non-reference path, the default retains every step between the minimum and
maximum occurrence of any queried reference node. Repeated anchors can
therefore span most of a path even for a 5 kb reference interval.

Version 1.9.7 implements the contained anchor-clustering choice as an opt-in
gap. A best collinear anchor chain remains a separate, substantially more
complex semantic option: it would need explicit rules for reference order,
orientation changes, duplicate anchors, ties, and paths with only one anchor.

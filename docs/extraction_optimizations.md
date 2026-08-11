# Subgraph And Interval Extraction Optimizations

This document records the serial extraction work performed on 2026-08-11 with
the HPRC minigraph-cactus chr1 and PGGB chr22 indexes in
`/home/user3/optimize_gfaidx`. The benchmark resource logs are retained under
`/home/user3/optimize_gfaidx/benchmarks/<version>/`.

## Benchmark inputs

| Graph | Indexed GFA | Path syntax |
| --- | --- | --- |
| minigraph-cactus chr1 | `hprc2_mc_chr1/hprc_v2_mc_chr1.indexed.gfa.gz` | W |
| PGGB chr22 | `hprc2_pggb_chr22/20251014_hprc25272.p98-k311.chr22.indexed.gfa.gz` | P |

The machine had 20 logical CPUs and 121 GiB RAM. Each extraction used one
gfaidx process and GNU `time -v`. Benchmarks were run sequentially to avoid
intentional storage contention. Filesystem cache state was not reset, so very
small timing differences should be treated as noise.

## Queries

### Node-based extraction

The seed nodes came from S records in the already saved region outputs. Both
queries use a 1,000-node BFS cap and coordinate-bearing path output.

```bash
gfaidx get_subgraph \
  hprc2_pggb_chr22/20251014_hprc25272.p98-k311.chr22.indexed.gfa.gz \
  3771302 out.gfa --max_nodes 1000 --with_coords

gfaidx get_subgraph \
  hprc2_mc_chr1/hprc_v2_mc_chr1.indexed.gfa.gz \
  2931334 out.gfa --max_nodes 1000 --with_coords
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
  --all_haplotypes --with_coords

gfaidx get_region \
  hprc2_mc_chr1/hprc_v2_mc_chr1.indexed.gfa.gz \
  chr1:36000000-37000000 out.gfa \
  --reference CHM13 --all_haplotypes --with_coords
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

| Version | Wall time (s) | User + system (s) | Peak RSS (GB) | Change from 1.9.0 |
| --- | ---: | ---: | ---: | --- |
| 1.9.0 | 169.96 | 168.68 | 6.698 | baseline |
| 1.9.1 | 128.80 | 127.69 | 0.666 | 24.2% faster, 90.1% less RSS |
| 1.9.2 | 75.73 | 75.59 | 0.761 | 55.4% faster, 88.6% less RSS |
| 1.9.3 | 49.72 | 46.45 | 0.547 | 70.7% faster, 91.8% less RSS |

Version 1.9.3 is 3.42 times faster than 1.9.0 for this query.

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

## Correctness validation

After each round:

- all seven CTest regression tests passed;
- the minigraph-cactus region output was byte-identical to the fresh 1.9.0
  output and the pre-existing `chr1_1mb.gfa`;
- both node-query outputs were byte-identical to fresh 1.9.0 outputs; and
- the complete 12 GB PGGB output was byte-identical to the pre-existing
  `5kb_region.gfa` and to the preceding optimized version.

The comparisons used `cmp`, not record counts or normalized GFA output.

## Remaining bottleneck and semantic decision

At version 1.9.3, 41.671 s of the 49.72 s stress-test runtime is coordinate
calculation plus writing 12 GB of P records. Further serial improvements face
the cost of generating the requested output itself.

The unexpectedly large output is caused by current `--all_haplotypes`
semantics, not by BFS or coordinate-index lookup. For each non-reference path,
the algorithm retains every step between the minimum and maximum occurrence of
any queried reference node. Repeated anchors can therefore span most of a path
even for a 5 kb reference interval.

A compact local-haplotype mode would require a deliberate semantic choice, for
example clustering anchor occurrences or selecting a best collinear anchor
chain. Such a mode should be opt-in until its behavior for repeats, inversions,
and duplications is specified. Parallel path formatting is also possible after
that decision, using bounded per-path buffers and deterministic main-thread
output.

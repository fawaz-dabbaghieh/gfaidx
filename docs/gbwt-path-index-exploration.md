# Path index compression: GBWT/RLBWT exploration

Status: **concluded — GBWT-style indexing was investigated thoroughly and
not adopted.** See "Conclusion" at the end for the decision and its
reasoning; everything above it is the hands-on evidence that led there.
Numbers below are measured, not estimated, unless explicitly marked as
such. Nothing in this document was merged into gfaidx's build — see
`experiments/libgbwt_prototype/` for the standalone prototypes referenced
throughout.

## The problem

`.pdx`'s step table stores one 4-byte record per path-step, uncompressed
across paths. On real HPRC graphs this dominates the index:

| Graph | paths | nodes | steps | step table | `.pdx` total | step table share |
|---|---|---|---|---|---|---|
| `hprc_v2.0_chr22` | 6,401 | 4,147,250 | 768,342,596 | 2.862 GiB | 6.54 GiB | 43.8% |
| `hprc2_chr1` | 2,915 | 11,728,931 | 4,489,859,395 | 16.726 GiB | 35.94 GiB | 46.5% |

The redundancy is real and large: many paths traverse identical stretches of
the graph (haplotypes agree almost everywhere except at variant sites), but
today's step table stores each path's traversal independently with zero
cross-path deduplication. The node→paths direction (posting table, the other
big piece of `.pdx`) already gets varint/delta compression; the step table
gets none.

## Attempt 1: custom block/breakpoint compression (rejected — too little payoff)

Built three prototype iterations (`v1`, `v2`, `v3` — see git history / prior
session scratch work) trying to partition each path's step sequence into
"blocks" — maximal runs where a stable set of paths agrees — replacing the
step table with a block dictionary + per-path breakpoint table.

- **v1** (empirical unitig / degree==1 chaining): failed almost completely.
  Real pangenome graph builders (minigraph-cactus etc.) already compact any
  genuinely-universal unbranching stretch at construction time, so nothing
  resembling a simple degree-1 chain survives into the GFA. Median block
  length was 1 — essentially no compression.
- **v2** (topological-order group refinement): had a real implementation bug
  (path-start occurrences were each given a *globally unique* initial id
  instead of being grouped by starting node, so groups of size 1 could never
  show disagreement — nothing ever merged). Superseded by v3 rather than
  debugged further, since v3's design sidesteps the whole class of bug.
- **v3** (local edge-signature comparison, no topological sort needed): the
  correct, working version. Chains two adjacent edges into the same block
  iff their *occurrence sets* (which paths use that exact edge) are
  identical, checked via a randomized multiset-hash signature per distinct
  edge — fully vectorized, immune to graph cycles since it never traverses
  the graph, only compares two concrete edges.

**Real measured results (v3):**

| Graph | distinct edges | breakpoints / steps | step table: current → estimate | ratio |
|---|---|---|---|---|
| chr22 | 6,492,079 | 52% remain unmerged | 2.862 → 1.512 GiB | ~1.9× |
| chr1 | 17,691,563 | 63% remain unmerged | 16.726 → 10.582 GiB | ~1.58× |

Per-path breakpoint distribution on chr1 was sharply bimodal (median 5,
mean 968,377 out of ~1.5M avg path length) — most individual paths compress
almost perfectly, a minority of highly-divergent paths drag the average
down. Interesting, but the *blended* result (~1.6–1.9× on the step table,
translating to ~17–20% off total `.pdx` size) was judged not worth the
implementation complexity of a custom merge algorithm.

## Attempt 2: GBWT (node-native run-length BWT)

[GBWT](https://github.com/jltsiren/gbwt) (Sirén et al.) is a run-length
encoded BWT of paths threaded through a graph — the same core redundancy as
above, but exploited via implicit rank-based path membership (LF-mapping)
instead of an explicit per-block path list, which is why it compresses far
better than attempt 1.

### Measured compression (via `vg gbwt`, real HPRC data, not estimated)

| Graph | haplotypes | gfaidx step table | GBWT (paths only) | ratio |
|---|---|---|---|---|
| chr22 | 466 (6,401 path fragments) | 2.862 GiB | 102.06 MiB | **~28.7×** |
| chr1 | 466 (2,915 path fragments) | 16.726 GiB | 429.4 MiB | **~39.9×** |

Combined graph+paths (GBZ format) for chr22: 139.5 MiB vs. gfaidx's current
full index set (~6.69 GiB) — not a fully fair comparison since GBZ doesn't
replace gfaidx's community-chunked BFS extraction, but illustrative.

GBWT natively answers **both** of gfaidx's directions from one structure —
`extract()` (path→steps, replaces the step table) via forward LF-mapping,
and `locate()` (node→paths, replaces the posting table) via the same
forward LF-mapping walked until a stored checkpoint resolves identity. This
is why it beats attempt 1 so decisively: no duplication between the two
views, and membership is implicit (rank-based) rather than an explicit list.

### Problem A: construction cost (real measurement, not estimated)

Built `chr1_real.gbwt` from the real, fully-reconstructed chr1 GFA (2,915
paths, all P-lines re-extracted from gfaidx's own `.pdx` via `get_path`,
concatenated with the graph's S/L lines — gfaidx's own community-chunked
`.gz` output does **not** retain P/W lines, only H/S/L; confirmed via
`split_gfa_to_comms.cpp`, which dispatches only on those three line types).

```
vg gbwt -G chr1_full.gfa -o chr1_real.gbwt
  wall clock:  7h 51m
  peak RSS:    67.35 GiB
  CPU:         103% (i.e. essentially single-threaded)
```

`--num-jobs` defaults to 10 and is documented as applying to `-G` mode, but
provided no real parallelism here — GBWT's incremental path-insertion
algorithm has a fundamental sequential dependency (each path insertion
updates a shared structure the next insertion depends on), which is an
algorithmic property, not a missing flag.

For comparison: gfaidx's own current indexer builds its **entire HPRC v2
pangenome** (all chromosomes) in ~15–16 hours, peaking at 28 GB RAM. `vg
gbwt` took comparable wall-clock time to index *one* chromosome, at over 2×
the peak memory. Not viable to adopt as-is for construction.

### Problem B: query-time memory (real measurement, not estimated)

The standard `vg`/libgbwt load path reads the whole compressed structure
into RAM regardless of query size:

```
vg gbwt -c chr22.gbwt   (count paths — the smallest possible query)
  → RSS 125 MiB, file is 102 MiB

vg gbwt -c chr1_real.gbwt
  → RSS 454 MiB, file is 429 MiB
```

Confirmed at two independent scales: RSS ≈ file size regardless of query.
This conflicts directly with gfaidx's core design goal (disk-resident,
bounded-memory queries regardless of total index size).

**However** — this turned out to be a loader limitation, not a file-format
limitation. Confirmed via GBWT's actual source (not just CLI behavior):

- The `.gbwt` files `vg gbwt` produced are **format version 5** (the
  pre-zstd format — `GBWTHeader::DEFAULT_VERSION` is `TAGS_VERSION = 5`,
  below `ZSTD_VERSION = 6`; zstd wrapping is opt-in via
  `simple_sds_serialize_version()`, not the default).
- Version 5's `RecordArray` (`include/gbwt/support.h`) stores an `index`
  (`sdsl::sd_vector<>`, Elias-Fano, O(1)-ish `select`) giving each graph
  node's byte range within a flat `data` blob — `getRange(record)` is a
  public method that computes this directly. This is structurally the same
  idea as gfaidx's own posting table (per-key offset → byte range).
- `DASamples` (the `locate()` checkpoint structure) is similarly small and
  rank-addressable: a bitvector of "which nodes have samples", two sparse
  vectors, and a packed array of `(path, step)` payloads — proportional to
  `node_count` + `sample_count` (sample density set by `--id-interval`,
  default every 1024 steps), not to total data size.
- The only thing that isn't seek-friendly is that `RecordArray::data` is
  declared `std::vector<byte_type>` — the *class* assumes full residency
  once loaded, even though the *bytes on disk* don't require it.
- Empirically validated the underlying mechanism: 2,000 independent random
  `seek()+read(4KB)` calls across the full 429 MiB `chr1_real.gbwt` took
  **3.2ms total (1.6μs each)**, touching 8 MB, RSS ~9.6 MiB — versus 240ms /
  454 MiB for the standard full-load path. Seeking into this file really is
  cheap; the shipped loader just doesn't do it.

**locate() mechanism, confirmed via `GBWT::locate()` in `gbwt.cpp`:** it
calls `runLF()` — the *same* forward LF-mapping `extract()` uses — for each
BWT position, checking `da_samples.nextSample()` after every hop. No
backward search, no separate mechanism: walk forward until a checkpoint
resolves `(path, step)` identity, then adjust by the number of hops taken.
This only works unambiguously because the alphabet is node IDs, not DNA
content — a node ID can never accidentally match somewhere unrelated the
way a short DNA sequence can.

### Correction from initial review — even "small" per-node metadata must stay disk-resident

An earlier pass at this design assumed a per-node offset directory ("one
small integer per node") was cheap enough to hold fully in RAM. **Rejected
on review**: at 200M+ node scale this stops being small, and more
importantly, nothing that scales with graph size should be a hard RAM
requirement — that's the same principle `.ndx` already embodies (disk-based
lookup via sorted binary search over hashed node IDs, never a full in-memory
node table).

The fix is simpler than GBWT's own choice of Elias-Fano for this: since
`.ndx` already gives every node a **dense rank** (0..N-1), a gfaidx-native
offset directory doesn't need succinct encoding or binary search at all —
a plain **fixed-width array** on disk, where record `i`'s offset lives at
`directory_offset + i * WIDTH`. One `seek + read`, no different in kind from
`.ndx`. Same treatment applies to the sample/checkpoint table (GBWT's
`DASamples` equivalent): rank-aligned, fixed-width, disk-seekable — not
assumed to fit in memory just because it's "the small structure."

**Design principle going forward: nothing that scales with total graph or
path-collection size is a hard RAM requirement — not just bulk record data,
but offset directories and sample tables too.** Only genuinely
`O(path_count)` or `O(query size)` structures may be held resident.

### Node identity: string IDs, `.ndx`, and rank alignment

GFA segment (node) IDs are strings, not guaranteed to be small, dense, or
even numeric — this is the whole reason `.ndx` exists (sorted binary search
from string ID to `(community_id, rank)`, never a full in-memory node-name
table). Everything above operates in **rank-space** downstream of `.ndx`:
a user-facing query like "subgraph around node 28374273" resolves the string
`"28374273"` through the existing, unchanged `.ndx` lookup *first*, and only
the resulting dense rank is ever used to index into the new fixed-width
offset/sample directories. `.ndx` remains the single source of truth for
string↔rank; nothing in this design bypasses or duplicates it.

**Separate, real risk when using `vg gbwt` to build test/prototype data:**
`vg gbwt -G` defaults to `--max-node 1024`, which silently re-chops any
segment longer than 1024bp into multiple GBWT-internal nodes. This means
vg's internal node numbering is *not* guaranteed to correspond 1:1 with
gfaidx's own `.ndx`-ranked nodes, independent of the string/rank question
above. The `.gbwt` files built so far (`chr22.gbwt`, `chr1_real.gbwt`) did
**not** pass `--max-node 0`, so their internal node granularity may not
match gfaidx's original segments exactly — the size/construction-time/
query-RSS measurements above are still valid (they don't depend on node
granularity), but any correctness comparison against gfaidx's own
`get_subgraph` output needs 1:1 node correspondence to be meaningful. Fix
for future builds: pass `--max-node 0` (disables chopping) and capture
`--translation FILE` (segment-name ↔ vg-internal-id) at build time, so the
correspondence to `.ndx` is verified rather than assumed.

## Attempt 3: sequence-based BWT (ropebwt3 / grlBWT as DNA indexers) — rejected

Investigated whether a construction-efficient sequence-level BWT
(specifically [ropebwt3](https://github.com/lh3/ropebwt3), Heng Li) could
sidestep GBWT's construction cost by indexing each haplotype's actual DNA
sequence instead of its node-ID path.

**Why rejected:**

1. **Breaks gfaidx's graph-agnostic design goal.** gfaidx is meant to index
   any graph type (dBg, assembly graph, overlap graph, pangenome) uniformly.
   A sequence-based index requires reconstructing DNA sequence from paths
   and assumes haplotype/reference-like semantics — not meaningful for graph
   types where paths don't correspond to "a genome's sequence."
2. **Forward and reverse directions stop being symmetric.** `extract()`
   (haplotype→sequence) maps cleanly onto sequence-BWT `extract()`. But
   `locate()` (node→haplotypes) does not: "node X" isn't a symbol in a raw
   DNA alphabet, and searching for a node's sequence content is ambiguous —
   short nodes (a 1bp SNP) match everywhere; repetitive regions match
   spuriously. `vg` itself keeps GBWT *and* separate graph-native positional
   indexes for exactly this reason — even the reference implementation
   doesn't rely on sequence search for graph-position lookup.
3. **Net effect: the reverse direction would still need a separate,
   graph-native index** (essentially gfaidx's current posting table,
   unchanged) alongside the new sequence index. That gives up the
   "one structure serves both directions" property that made GBWT's
   compression so good in the first place. Reworked size estimate: step
   table shrinks to ropebwt3-scale, but the posting table (18.78 GiB of
   chr1's current 35.94 GiB) stays roughly where it is — realistic total
   improvement ~1.9×, not GBWT's ~40×, while adding real new complexity
   (sequence reconstruction, alignment-adjacent machinery for the reverse
   direction).

Conclusion: sequence-based indexing solves a different, narrower problem
than the one gfaidx actually has. Not pursued further.

**ropebwt3 benchmark numbers for reference** (from public documentation,
not independently reproduced): 320 whole human genomes in 65 hours, 170 GB
peak RAM; output format explicitly documented as memory-mappable. Notably
better construction efficiency than GBWT even at that scale, and the
mmap-friendly design was itself informative for how *any* construction
tool's output should be laid out — even though the tool itself isn't the
right fit here.

## grlBWT — noted, not yet tested

[grlBWT](https://github.com/ddiazdom/grlBWT) (Díaz-Domínguez, Navarro et
al.) — grammar-compression-based RLBWT construction, purpose-built for
collections of similar genomes.

- **Construction efficiency (from published benchmarks, not independently
  reproduced): 25 full human genomes (75 GB) in ~7.3 hours, 27 GB peak
  RAM** — for comparison, that's ~25× more genomic data than our GBWT chr1
  test, in comparable wall-clock time, at under half the peak memory.
  Notably: 27 GB peak is close to gfaidx's own 28 GB whole-genome benchmark.
- Operates on raw sequence (same node-ID↔sequence translation gap as
  ropebwt3 would have, if used the same way) — **but** grammar compression
  is alphabet-agnostic in principle, so it's worth checking whether it (or
  its underlying algorithm) could be adapted to run directly over integer
  node-ID sequences instead of DNA, avoiding the sequence-based rejection
  reasons above.
- Output is a custom run-length-pairs format with conversion utilities
  (`grl2plain`, `grl2rle`) but **no documented query/random-access API** —
  building a disk-resident reader against it would be at least as much new
  work as a from-scratch gfaidx-native format, possibly more, since GBWT at
  least has documented `extract()`/`locate()` semantics to reference.
- GPL-3.0, C++/CMake, depends on SDSL-lite (same dependency chain as GBWT).
  Fine to shell out to a separately-installed binary for construction only;
  would matter if any of its code were incorporated directly into
  MIT-licensed gfaidx.

Keep in mind as a construction-efficiency reference point / possible
algorithm-adaptation source. Not tested hands-on yet.

## Query correctness prototype — validated

Built a standalone C++ prototype (`experiments/` on this branch, not wired
into gfaidx's build) linking `libgbwt` directly — no `vg` CLI involved at
query time, just the library's `locate()`/`extract()` API. Build notes:

- `libgbwt` requires the **`vgteam/sdsl-lite`** fork specifically (has
  `sdsl/simple_sds.hpp`) — vanilla `simongog/sdsl-lite` fails to compile
  against it.
- Rebuilt `chr22.gbwt` with `--max-node 0 --translation FILE` from the
  properly-reconstructed complete GFA (see node-identity section above).
  Result was **byte-identical** to the original 107 MB file — chr22's
  segments are all already ≤1024bp, so chopping was never actually
  happening for this graph. `--translation` came back empty because there
  was nothing to translate: confirmed via the header (`offset`/
  `alphabet_size` fields are exactly 2× the known segment-id range) that
  vg's internal GBWT node id is simply `2 * raw_segment_id` (+1 for reverse
  strand) when segment names are numeric — deterministic arithmetic, not an
  opaque renumbering, for this graph specifically.

**Validation methodology:** ran gfaidx's real `get_subgraph` from seed node
`112000000`, `--max_nodes 500` — a genuine BFS neighborhood spanning 3
graph communities via shared edges (not a toy/contrived case), producing
111 real P/W ground-truth records. Extracted the exact 500-node set from
the output GFA's S-lines and fed it to the prototype (which only knows
node/path threading, not graph topology — it has no way to derive this
node set itself, by design, since that direction stays gfaidx's job).

**Result: exact match.** Same 74 distinct `(sample, haplotype, contig)`
keys, same 111 total subpath records, **zero mismatches** on first node,
last node, or run length across all 111 pairs — after accounting for one
known, documented difference: gfaidx's `find_subpaths_for_node_ids`
suppresses single-step runs ("Runs shorter than two consecutive steps are
suppressed"); the prototype doesn't do this yet, found exactly 12 extra
single-node runs, and excluding them landed exactly on gfaidx's 111.

Performance (recorded for reference, not the focus of this test — memory/
speed were explicitly out of scope): 0.37s wall, 113 MB RSS for the query,
using the standard eager loader (not the disk-resident design).

**Conclusion: the query logic is sound.** GBWT's `locate()`+`extract()`
genuinely reproduces gfaidx's own subgraph/subpath semantics exactly, once
fed the same node set. What's left is the disk-residency and construction-
efficiency work described below, not a doubt about correctness.

**Reproducing this validation:** full command sequence — building
`libgbwt`, reconstructing a complete GFA with real P/W lines, building the
`--max-node 0` GBWT, compiling the prototype, generating ground truth via
`gfaidx get_subgraph`, and the comparison script — is in
`experiments/libgbwt_prototype/README.md`, "Full reproduction" section.
Re-run and re-verified (0 diffs, 111 records, 74 keys) while writing that
section.

## A real bug the N=500 test didn't catch, found by scaling up

Curiosity-driven follow-up: benchmarked gfaidx / GBWT-in-memory /
GBWT-on-disk against the same start node at N = 500, 1,000, 10,000, 50,000,
100,000 (see "Benchmark: gfaidx vs. custom GBWT" below). Raw walk counts
didn't match between gfaidx and GBWT at any size, which was expected — the
N=500 validation already found that gfaidx suppresses single-step runs and
the prototype doesn't. But subtracting single-step runs from GBWT's count
didn't fully reconcile the two at N≥10,000 (off by 2, then 19, then 38,
**flipping sign** between sizes) — a sign flip rules out a simple constant
or proportional bias, so this was a second, distinct issue, not noise
around the known one.

Diffing actual `(sample, haplotype, contig, first_node, last_node,
run_length)` tuples between gfaidx's ground truth and GBWT's filtered
output at N=10,000 surfaced concrete missing rows, e.g. haplotype
`HG00408`'s 8-step run through contig `JBHDVL010000004.1`. All 8 segments
were confirmed present in the exact node list fed to both tools — the
input was never the problem. The run itself was written entirely with
reverse-orientation steps (`<111940778<111924933<...<111924924` in the
W-line); GBWT's raw output had **no row at all** for that haplotype/contig,
not a malformed one — a silent miss, not a scan error.

**Root cause:** every GFA segment maps to two distinct GBWT node ids —
`2*seg` (forward) and `2*seg+1` (reverse), the bidirectional-GBWT
convention used throughout this doc — and a haplotype can legitimately
traverse any segment in either orientation. Both prototypes built their
`target_nodes` set as forward-only (`node_type n = 2 * seg;`, called out
in a comment as a known shortcut, but never revisited). This broke two
things at once: `locate()` seeding never discovers a sequence that touches
a target segment *exclusively* via its reverse strand (not scanned
incorrectly — never added to `touching_sequences` in the first place), and
the `extract()`-time contiguous-run scan (`target_nodes.count(path[i])`)
fails to recognize a target segment's reverse-oriented GBWT id, truncating
or dropping runs even in sequences that *were* discovered via some other,
forward-touched node elsewhere in the same path.

This explains why N=500/1,000 passed cleanly (small, localized
neighborhoods happened not to hit an all-reverse run with no other
forward touch) while larger, more sprawling neighborhoods increasingly
did — and why the two effects (single-step suppression vs. this) looked
like one noisy, sign-flipping gap until separated by an actual diff
instead of assumed to be fully explained by the already-known cause.

**Fix:** insert both `2*seg` and `2*seg+1` into `target_nodes` (`for
(node_type n : {2 * seg, 2 * seg + 1})`), in both `query_subgraph.cpp` and
`query_subgraph_diskseek.cpp` — the output logic (`path[start] / 2` etc.)
already discards orientation correctly regardless of which id matched, so
no other change was needed. Rebuilt both; re-diffed against gfaidx's
ground truth (orientation-aware parser: `>`/`<` both split correctly,
fixed from an earlier version of the diff script that only handled `>`)
at every benchmarked size:

| N | gfaidx walks | GBWT walks (run>1) | diff lines |
|---|---|---|---|
| 500 | 111 | 111 | 0 |
| 1,000 | 590 | 590 | 0 |
| 10,000 | 1,265 | 1,265 | 0 |
| 50,000 | 2,762 | 2,762 | 0 |
| 100,000 | 1,660 | 1,660 | 0 |

Exact match at every size now, not just N=500. `query_subgraph` and
`query_subgraph_diskseek` still agree with each other after the fix
(re-checked at N=500 and N=100,000).

## Benchmark: gfaidx vs. custom GBWT (in-memory vs. disk-seek)

**Methodology.** Same start node (`112000000`, `hprc_v2.0_chr22`) at five
neighborhood sizes (`--max_nodes` 500 / 1,000 / 10,000 / 50,000 / 100,000).
`gfaidx get_subgraph` does its own BFS end-to-end (graph traversal *and*
subpath extraction) and is the full current baseline. The GBWT prototypes
don't do graph BFS at all — they only answer "which paths touch this node
set" — so they were fed the *exact* node set gfaidx's own BFS produced for
each size, isolating the path-lookup cost specifically (the part GBWT
would actually replace: `.pdx`, not `.gz`/`.ndx`).

**Results, after the orientation-bug fix above** (one run per cell, not
multi-rep — curiosity-driven, not a formal benchmark suite):

| N | gfaidx | GBWT (in-memory) | GBWT (disk-seek) |
|---|---|---|---|
| 500 | 0.04 s / 15.7 MB | 0.77 s / 110.7 MB | 3.30 s / 24.1 MB |
| 1,000 | 0.04 s / 26.5 MB | 1.74 s / 110.8 MB | 7.58 s / 24.3 MB |
| 10,000 | 0.11 s / 53.7 MB | 6.97 s / 117.8 MB | 29.76 s / 31.3 MB |
| 50,000 | 0.34 s / 94.3 MB | 30.54 s / 121.5 MB | 128.38 s / 35.0 MB |
| 100,000 | 0.62 s / 119.6 MB | 54.79 s / 126.1 MB | 216.72 s / 39.6 MB |

(time / peak RSS at each cell.) Walk counts matched gfaidx exactly at
every size once GBWT's raw output is filtered to exclude single-step runs
(gfaidx suppresses these by construction; the prototypes don't) —
confirmed via direct diff, not just count equality: 111/590/1,265/2,762/
1,660 on both sides, 0 mismatches.

**Headline pattern:** gfaidx remains dramatically faster at every size
(17–350×), in-memory GBWT stays RSS-flat at ~110–126 MB regardless of N
(dominated by loading the whole ~107 MB file every run, confirming the
"Problem B" finding above empirically, not just via `-c`), and disk-seek
GBWT stays RSS-bounded (~24–40 MB, nowhere near the 107 MB file) at a
real, consistent time cost — disk-seek is slower than in-memory by a
remarkably stable ~4.1–4.4× at *every* size, suggesting a fairly fixed
per-record overhead (`pread()` syscall vs. pointer dereference) rather
than something that gets worse at scale.

**A methodological note worth keeping for the record:** this same
benchmark is what surfaced the orientation bug documented above — the
first (buggy) pass gave *faster* numbers than the corrected one, because
the bug was silently skipping real work (sequences touching the target
set only via reverse-oriented steps). Re-run after the fix:

| N | GBWT (mem): buggy → fixed | GBWT (disk): buggy → fixed |
|---|---|---|
| 500 | 0.64 s → 0.77 s (1.2×) | 2.77 s → 3.30 s (1.2×) |
| 1,000 | 1.48 s → 1.74 s (1.2×) | 6.53 s → 7.58 s (1.2×) |
| 10,000 | 4.71 s → 6.97 s (1.5×) | 20.94 s → 29.76 s (1.4×) |
| 50,000 | 17.23 s → 30.54 s (1.8×) | 71.31 s → 128.38 s (1.8×) |
| 100,000 | 29.10 s → 54.79 s (1.9×) | 118.25 s → 216.72 s (1.8×) |

Worth citing as a general caution, not specific to GBWT: a small-scale
correctness test (here, N=500) can pass cleanly while a real bug hides in
an edge case (reverse-oriented-only runs) that only becomes statistically
likely to appear once the query is large enough — and because the bug
happened to make things *faster*, a performance benchmark alone wouldn't
have caught it either; it took diffing actual output rows at a size where
the effect was large enough to notice.

## Disk-resident reader — validated

The query-correctness prototype above used the standard eager loader —
proving the *logic* is right, but leaving the memory claim from Problem B
resting on source-reading plus a generic seek-cost microbenchmark, not a
real query. This closes that gap: `experiments/libgbwt_prototype/
query_subgraph_diskseek.cpp` is a second prototype that never materializes
the BWT's flat `data` blob (the one structure that scales with total
graph+path size) and answers the exact same validated query anyway.

**Design.** `CompressedRecord`'s constructor (`src/support.cpp`, confirmed
by reading it, not assumed) only reads bytes within `[start, limit)` of
whatever buffer it's given — it has no dependency on anything outside a
single record's own bytes. That means GBWT's own record type can be reused
unmodified against a small on-demand buffer instead of the full eager
blob. `SeekableGBWT` (in the new file) is a close port of `GBWT::load()` /
`GBWT::locate(SearchState)` / `gbwt::extract()` (verified line-by-line
against `src/gbwt.cpp` and `include/gbwt/algorithms.h`) with exactly one
change: every place the real code does
`CompressedRecord(this->bwt.data, start, limit)` (indexing into the fully
loaded blob), this version does one `pread()` of `[start, limit)` into a
small caller-owned buffer first. Concretely, at load time:

- Header, tags, the BWT's `index` (the Elias-Fano `sd_vector` giving each
  node's byte *range*, not its bytes), `da_samples`, `metadata`, and the
  cached endmarker record are all loaded eagerly, exactly like the real
  loader — all genuinely `O(node_count)` or `O(path_count)`, matching the
  design principle from earlier in this doc.
- The BWT's `data` blob is never read into a `std::vector`. Its length
  prefix is read (to know how many bytes to skip and where they start),
  the file offset of byte 0 is recorded, and the stream seeks past it.
- Every `LF()` call — the one primitive both `locate()` and `extract()`
  are built from — fetches its record with one `pread()` into a small
  buffer sized to just that record, decodes it with GBWT's own
  `CompressedRecord`, and discards the buffer once done.

**Result: byte-identical output**, not just equivalent. Same 500-node
node-list file, same `chr22_nochop.gbwt`: the disk-seeking version's 123
output rows are byte-for-byte identical to the eager loader's (`diff`
reports zero differences), and — transitively, through the already-proven
eager-loader-vs-gfaidx match — identical to gfaidx's own real
`get_subgraph` ground truth (111/111 after excluding the same 12
single-step runs gfaidx suppresses).

**Result: memory genuinely stays bounded**, measured, not inferred:

| Reader | File size | RSS | Wall time |
|---|---|---|---|
| Eager loader (`query_subgraph`) | 107 MB | 113 MB | 0.37 s |
| Disk-seeking (`query_subgraph_diskseek`) | 107 MB | **24.6 MB** | 2.80 s |

24.6 MB is dominated by fixed costs (`da_samples`, `metadata`, one
extracted path buffer) — not by file size. Spot-checked against
`chr1_real.gbwt` (450 MB, ~4× bigger) with 8 arbitrary sample nodes: load
and `locate()` completed cleanly (470 touching sequences found across both
strands), with RSS fluctuating in a 75–101 MB band while running — well
under the 450 MB file, and clearly not tracking it upward. Stopped partway
through the `extract()` phase rather than waiting it out, once the pattern
below made it clear why it would take a while: chr1's haplotypes are much
longer than chr22's, so full-path extraction (see next paragraph) means
proportionally more `pread()` calls. This was a scale/memory spot-check,
not a full correctness re-validation — chr1_real.gbwt was also built with
default node-chopping (no `--max-node 0`), so its internal node ids
aren't guaranteed 1:1 with gfaidx's own segment ids the way chr22's
rebuild is (see "Node identity" above); chr22 remains the graph with the
complete, exact-match proof.

**A genuinely useful finding, not just a clean result: record-fetch count
was surprisingly high** — 9.9 million `pread()` calls for a 500-node
query, fetching 121% of the (uncompressed-equivalent) data blob's total
byte count in aggregate, despite only touching a few hundred distinct
nodes directly. Root cause, confirmed by re-reading the algorithm rather
than guessed: `extract(sequence)` walks a path via `LF()` from its start
all the way to the endmarker — i.e., it extracts the *entire* haplotype,
not just the local neighborhood around the query. Both prototypes do this
identically (it's `gbwt::extract()`'s real behavior, `include/gbwt/
algorithms.h`), and it's essentially free with the eager loader
(in-memory array indexing) — so it was invisible until every hop became a
`pread()`. Wall-clock still stayed low here (2.8s) because chr22's 107 MB
file was fully page-cache-warm after the first pass; it would not stay
low on a cold cache or a much longer chromosome.

**Design implication for the real (non-GBWT-derived) format:** a
production disk-resident reader should not lean on unconditional
`extract()` for local subgraph/subpath queries. It should walk outward
from each `locate()` hit only as far as the query actually needs (stop
once the walk leaves the target node set, or after a bounded number of
steps) — closer in spirit to how `.pcx`'s existing checkpoint-driven
incremental extraction already works than to GBWT's convenience API. This
is a concrete, evidence-based input to open item 3 below, not a flaw in
what was just validated: correctness and memory-boundedness were the
questions this step was built to answer, and both are now settled.

Reproduction commands are in `experiments/libgbwt_prototype/README.md`
alongside the eager-loader ones.

## Attempt 4: bounded/local extraction — tested, does not deliver

The disk-resident reader test above identified unconditional `extract()`
(walking a haplotype from its true start to its true end) as the likely
dominant cost. This was the fourth and — per explicit request — intended
to be the *definitive* experiment: implement bounded/local extraction for
real, measure it honestly across the same five sizes, and settle whether
it gets GBWT-based querying close to gfaidx's own speed before committing
any further engineering effort in that direction.

### Why this seemed promising: a worked example

GBWT stores, per node, a compact run-length-encoded record of "how many
sequences pass through here, and what does each group do next" — record
size depends on the number of *distinct* next-steps, not on how many
sequences take them. A node shared identically by 4 haplotypes or 4,000
has the same tiny record. Concretely, for a toy graph

```
A → B → C → D → E
        ↓       ↑
        └── X ──┘
```

with four haplotypes `H1=H2=H3: A B C D E` and `H4: A B X D E` (diverges
at B, rejoins at D), the per-node records are:

```
A: [(B, 4)]              4 sequences pass through, all → B
B: [(C, 3), (X, 1)]      3 → C, 1 → X
C: [(D, 3)]
X: [(D, 1)]
D: [(E, 4)]                (3 arrived via C, 1 via X -- doesn't matter here)
E: [(end, 4)]
```

`LF(node, offset)` — the only primitive `extract()` has — answers "the
sequence at this exact offset in this node's column: where does it go
next, and at what offset?" Each call moves *one* sequence *one* step.
Reconstructing H1 (offset 0 at A):

```
LF(A, 0) → (B, 0)     A has one run (→B); offset 0 stays offset 0
LF(B, 0) → (C, 0)     offset 0 falls in the "→C" run (offsets 0,1,2), rank 0
LF(C, 0) → (D, 0)     C only goes to D
LF(D, 0) → (E, 0)     D only goes to E
LF(E, 0) → end
```

5 hops for a 5-node path. Reconstructing H2 and H3 costs 5 hops *each* too
— same edges, but each is a separate walk, because "offset 0 at B" and
"offset 1 at B" are different tickets each needing their own `LF()` call
at every step. H4 (offset 3 at A, diverging through X) also costs 5 hops
— divergence and rejoining cost nothing extra; it's one hop per edge in
*its own* path, same as the others.

**The insight this makes concrete:** reconstruction cost is `(number of
sequences) × (path length)`, completely disconnected from how compactly
the shared structure is stored. A conserved block shared by 300
haplotypes over 5,000 nodes costs 300 × 5,000 = 1.5 million hops to
reconstruct individually, even though that block's entire on-disk
footprint is a few kilobytes — because each hop answers "where does
*this specific* sequence go next," and that question must be asked once
per sequence per step no matter how many other sequences are doing the
identical thing at the same moment. This is exactly why `extract()`
looked so expensive in the disk-resident reader test: large neighborhoods
pull in widely-shared conserved stretches, and reconstructing every
touching haplotype's local run through one, individually, is unavoidably
`(sequences touching it) × (local run length)`.

By contrast, `locate()` asks a strictly weaker question — "which
sequences pass through here," not "what is each one's path" — which
*can* be answered by batched rank/count operations over the same compact
record (see `GBWT::locate(SearchState)`'s round-based batching, "locate()
mechanism" above) without ever tracing any individual sequence's identity
forward. That asymmetry — locate() batchable, extract() inherently
per-sequence — is the reason locate() stayed cheap throughout this whole
investigation while extract() did not.

### The experiment

Design: for every occurrence (target node, offset) — i.e. every position
a target node appears in the BWT, both orientations — walk forward via
`LF()` only as long as the next node is also in the target set, instead
of walking to the sequence's true end. A run of length L, entered from L
different starting occurrences, produces L overlapping suffixes of
itself; deduplicated by `(sequence_id, exit_position)` (all suffixes of
the same true run share both), keeping the longest candidate per key —
exact, not approximate, since the longest suffix is exactly the one that
started at the run's true beginning. A `visited` set of BWT positions
avoids restarting a walk from a position some earlier walk already
covered (first version didn't have this and was *slower* than unbounded
at N≥10,000 — O(L²) redundant re-walking of the same run from every
starting occurrence within it).

Implementation: `experiments/libgbwt_prototype/query_subgraph_bounded.cpp`.

**Results — correctness was exact at every size** (0 diff lines against
gfaidx's ground truth, same methodology as above, all five sizes):

| N | gfaidx | GBWT unbounded | GBWT bounded | bounded vs. unbounded | bounded vs. gfaidx |
|---|---|---|---|---|---|
| 500 | 0.04 s | 0.77 s | 0.24 s | 3.2× faster | 6× slower |
| 1,000 | 0.04 s | 1.74 s | 0.44 s | 4.0× faster | 11× slower |
| 10,000 | 0.11 s | 6.97 s | 4.13 s | 1.7× faster | 37× slower |
| 50,000 | 0.34 s | 30.54 s | 60.91 s | **2.0× slower** | 179× slower |
| 100,000 | 0.62 s | 54.79 s | 355.91 s | **6.5× slower** | 574× slower |

RSS also grew with N for bounded extraction (112 MB → 294 MB) — unlike
the disk-seek reader, this version does *not* keep memory flat, since the
`visited` set and dedup map scale with total occurrences processed, not
just query node count.

### Verdict

**Does not validate the hoped-for outcome.** Bounded extraction wins
clearly at small-to-medium neighborhoods (2–4× faster than unbounded at
N≤10,000) but the benefit erodes and then *reverses* at exactly the sizes
that matter most for real subgraph queries — by N=100,000 it is over 6×
worse than the naive full-path approach it was meant to replace. And even
at its best case (N=1,000), it remains 11× slower than gfaidx — nowhere
near "similar performance," which was the bar for treating this as a
green light.

**Why the crossover happens:** the premise was that local runs are short.
True for small neighborhoods; false once N grows large enough that the
BFS neighborhood itself starts encompassing genuinely long conserved
stretches — hundreds of haplotypes agreeing for thousands of consecutive
nodes, which is a normal property of pangenome graphs (large "core
genome" regions are the rule, not the exception), not a graph pathology.
At N=100,000: 2.36 billion LF hops for ~2.1 million non-redundant walks —
average walk length roughly doubled from N=50,000 to N=100,000,
compounding with the doubled occurrence count.

**One honest caveat, for completeness:** the implementation has a known
remaining inefficiency — the `visited` check only happens before
*starting* a new walk, not *during* one, so a walk that reaches an
already-covered stretch mid-flight re-walks it instead of doing an O(1)
lookup. A properly memoized version would likely improve the large-N
numbers by some constant factor. It would not be expected to change the
qualitative conclusion: the worked example above shows the fundamental
cost is `(sequences touching a region) × (local overlap length)`, and
that quantity genuinely grows toward `(sequences) × (full path length)`
as the query neighborhood grows into conserved territory — no walking-
based cleverness avoids paying for that, because gfaidx's precomputed
posting table doesn't walk at all; it already has the answer stored. A
different lever worth naming for future reference (not built, not
verified): a properly round-based *batched* walk — advance all sequences
currently at the same node together, one round at a time, sharing the
record fetch across all of them (the same trick `GBWT::locate(SearchState)`
already uses) — would cut the number of record fetches, which matters a
lot for disk-seek, but would not reduce the total number of logical
(sequence, step) facts that must be computed, which is the more
fundamental limit.

This confirms, rather than overturns, the assessment from before this
experiment was run: GBWT's compression is real and substantial (Attempt
2's ~30–40× step-table reduction), but it fundamentally trades that
compression against reconstruction cost at query time, and gfaidx's own
posting table is close to the theoretical floor for the specific query
shape gfaidx needs (given a node set, list which paths touch it and
where) — a precomputed, materialized structure will not be beaten on
speed by a general-purpose compressed index answering the same question
through on-the-fly reconstruction, however that reconstruction is
engineered.

## Conclusion: GBWT-style indexing was not adopted

**Decision: do not pursue a GBWT-style (run-length BWT / FM-index) path
index for gfaidx.** This was a thorough, hands-on investigation, not a
desk-rejection — real construction, real query prototypes (eager and
disk-seeking), a real correctness proof, a real benchmark across five
orders of magnitude of neighborhood size, and a real attempt at the most
promising optimization (bounded/local extraction) before concluding.
Every piece of that evidence is above. Summary, for citation:

**What worked.** Compression is real and substantial: ~29–40× smaller
than gfaidx's current step table on real HPRC chromosomes (Attempt 2).
Query *correctness* is fully proven, not assumed: a standalone prototype
linking `libgbwt` reproduces gfaidx's own subgraph/subpath output exactly
(0 mismatches) across five neighborhood sizes spanning 500 to 100,000
nodes. Genuine disk-residency is also proven, not just theorized: a
second prototype that never loads GBWT's bulk data into RAM answers the
same queries with byte-identical output at 24–40 MB RSS regardless of
query size, confirming the on-disk format itself is not the obstacle.

**What did not work, and why it's a real obstacle rather than an
engineering gap:** query *speed*. gfaidx's current posting table stores
node→(path, step) occurrences directly — a query is a lookup. GBWT stores
paths as an implicitly-compressed run-length BWT and must *reconstruct*
that same occurrence information via LF-mapping at query time — a query
is a walk. Reconstructing many individual haplotypes' paths through a
shared region costs `(sequences touching it) × (local path length)` hops,
regardless of how compactly that region is stored (worked example in
"Attempt 4" above) — compression shrinks the records, not the number of
hops needed to trace many individual paths through them. Measured
end-to-end: GBWT is 17–350× slower than gfaidx at the same query, even
after the best-tried optimization (bounded/local extraction, which
*reverses* — gets slower, not faster — at exactly the large-neighborhood
sizes that matter most for real subgraph queries, because "local" stops
meaning "short" once a query spans genuinely conserved regions — a normal
property of pangenome graphs, not an edge case). This is a property of
the reconstruct-on-query design, not of any specific implementation
tried here.

**Why this settles it, rather than leaving room for "a better
implementation would fix it":** three independent angles were tried —
the reference eager loader, a from-scratch disk-seeking reader, and a
from-scratch bounded/local-extraction reader — and all three hit the same
wall, because all three are ultimately walking the same LF-mapping
primitive. A properly *batched* walk (advancing many sequences at the
same node together, sharing record fetches — named but not built, end of
"Attempt 4") could plausibly improve the disk-seek constant factor, but
would not change the fundamental `(sequences) × (path length)` hop count,
which is the deeper limit. Given gfaidx's own design priority — fast,
disk-resident queries over index size — this trade-off runs in the wrong
direction: a smaller index bought at a 17–350× query-speed cost is not a
good trade for this tool, however the reconstruction is engineered.

**What would still be true if compression were the only goal.** For a
tool whose primary objective is minimizing on-disk size and query speed
is secondary, GBWT (or a similar RLBWT) remains a reasonable choice — the
compression numbers are real, and both the correctness and
disk-residency obstacles that motivated most of this investigation were
fully resolved. That is not gfaidx's priority, which is the actual
reason this path was not adopted, not any flaw in GBWT itself.

**Open, if revisited later** (not pursued further given the conclusion
above): a gfaidx-native construction algorithm (GBWT's own incremental
path-insertion construction is separately too slow/heavy — Attempt 2,
Problem A — an independent problem from the query-speed one); grlBWT's
grammar-based construction adapted to integer node-ID sequences, as a
possible faster-construction alternative if the query-speed conclusion
were ever revisited; the batched-walk idea named above.

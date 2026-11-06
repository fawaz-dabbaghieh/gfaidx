# Locality node IDs and the compressed `.pdx` v5: results

This report covers the `.ndx` v2 / `.pdx` v5 change on branch
`pdx-compression`: what was built, what it saves, what it costs at query and
build time, and which alternatives were measured and rejected. The design
brief was `pdx_compression_plan.md`; deviations from it are called out.

All numbers come from one machine (20-core ARM64, 121 GB RAM, NVMe), Release
builds, comparing the branch point (`72f6adf`, "base") with this branch
("new"). Both binaries index the same input GFA.

## Summary (HPRC v2 PGGB chr1, 11.7 M nodes, 4.49 B steps, 2,915 paths)

| | base | new | change |
|---|---:|---:|---:|
| `.pdx` total | 35.94 GiB | 9.71 GiB | 3.7× smaller |
| step table | 16.73 GiB (4.00 B/step) | 4.99 GiB (1.19 B/step) | 3.4× smaller |
| postings | 18.78 GiB (4.49 B/posting) | 4.29 GiB (1.03 B/posting) | 4.4× smaller |
| node table + strings | 0.43 GiB | 0.43 GiB | unchanged |
| `index_gfa` wall time | 35:03 | 31:02 | 0.89× |
| `index_gfa` peak RSS | 3,093,844 KB | 3,093,972 KB | +0.004% |

Query latency on chr1 (234 queries, all outputs byte-identical to base):

| | p50 over query types, new / base |
|---|---|
| warm | 0.50–0.95× (every one of 26 types faster) |
| cold page cache | 0.21–0.80× for 25 types; `get_subgraph --max_nodes 2000` 1.05× |
| heaviest query (1q21 segdup, 1 Mb, `--mode all --with_coords`, 25.8 GB output) | 174–204 s → 59–64 s |

The brief's targets were ≤ 5.0 GiB of steps (met: 4.99 GiB), ≥ 15% smaller
postings (77% smaller), warm latency within 1.2× (every query type is
faster at p50; the worst chr1 p95 is 1.13×, and on MC chr22 one 4-query
type's p95, i.e. its slowest query, went from 23 to 29 ms), faster cold
full-path reads (0.38×), build time within 1.2× (0.89×) and peak RSS within
10% (+0.004%).

Build times are from the two builds running side by side on the same
machine, so both saw the same contention. A rebuild of the baseline produced
a byte-identical `.pdx`.

## What changed

1. **Node IDs** (`index_gfa --node_order auto`, the default). Nodes are
   sorted by Louvain community, then by a locality key: the integer segment
   name if every name is an integer (or one shared prefix plus digits, e.g.
   minigraph's `s123`), otherwise first appearance in the `L` lines. Both
   candidate orders are scored on the sorted edge list (estimated delta-varint
   bytes per edge) and the cheaper one wins. This guards against integer names
   that carry no locality (see the shuffled graphs below). The ID is the
   position in that order, so IDs are dense and each community owns a
   contiguous range.
2. **`.ndx` v2**: 64-byte header, entries `{hash64, hash32, node_id}` sorted
   by hash as before, a community start array, and a fingerprint of the
   name → ID mapping. v2 files are 8 mod 16 bytes long, so legacy readers
   reject them and a legacy file is never parsed as v2.
3. **`.pdx` v5 step table**: `LEB128((zigzag(id - prev) << 1) | reverse)`,
   with `prev` reset every 128 global steps; a superblock table (u64 + 32 × u16
   per 4096 steps, 0.018 B/step) locates any block.
4. **`.pdx` v5 postings** (`path_slots`): blocks of 32 consecutive node IDs.
   Each path keeps "slots" (its ranks at the last node of the block it
   visited); an occurrence `(p, r)` is predicted when `(p, r - 1)` is a slot,
   and only the predicted subset (bitmap, index list or complement list,
   whichever is smallest) is stored. The remaining occurrences are explicit,
   relative to the path's last rank when it has one. Loops predict too,
   because every occurrence has its own slot.
5. **Reader I/O**: `PathIndexReader` reads with `pread` (one syscall per read,
   no shared stream position), caches recently decoded step blocks, and
   prefetches all step runs of a query in one batch. `PostingCursor` keeps a
   per-thread posting block cache. An mmap reader was tried first and
   rejected (see below).
6. **Safety**: `.pdx` stores the `.ndx` fingerprint and `.cdx` a 32-bit tag of
   it (in its formerly unused `reserved` field). Every command that pairs them
   checks it, so a stale `.pdx` or `.cdx` next to a rebuilt index is rejected
   instead of returning wrong nodes. `--node_order legacy_hash` writes the
   previous `.ndx` and v4 `.pdx` byte for byte.

## Graphs

| graph | source | nodes | steps | paths |
|---|---|---:|---:|---:|
| DRB1-3123 | odgi test graph (PGGB) | 4,955 | 35,059 | 12 |
| LPA | odgi test graph (PGGB), KIV-2 copy-number loops | 3,751 | 202,806 | 13 |
| sim | `sim_pangenome.py 400000 40` (MC-like, contig W lines) | 624,433 | 17.2 M | 904 |
| sim_shuf / lpa_shuf | integer names replaced by a random permutation | same | same | same |
| sim_nonnum / lpa_nonnum | names replaced by non-numeric strings | same | same | same |
| MC chr22 | HPRC v2.0 Minigraph-Cactus CHM13 chr22 | 4.15 M | 768 M | 6,401 |
| PGGB chr1 | HPRC v2 PGGB chr1 (the target) | 11.73 M | 4.49 B | 2,915 |

The large graphs' original GFAs were not on disk, so their inputs were
rebuilt from the existing indexes: S/L lines from the chunked `.gz`, P/W lines
dumped from the `.pdx` (byte-identical P/W records). Base and new were built
from these same files.

## Size

### `.pdx` per section

| graph | base `.pdx` | new `.pdx` | ratio | steps B/step (base → new) | postings B/posting (base → new) |
|---|---:|---:|---:|---|---|
| DRB1 | 447 KiB | 235 KiB | 1.9× | 4.00 → 1.03 | 3.96 → 0.72 |
| LPA | 1.36 MiB | 0.52 MiB | 2.6× | 4.00 → 1.03 | 2.34 → 0.98 |
| lpa_shuf | 1.36 MiB | 0.53 MiB | 2.5× | 4.00 → 1.03 | 2.34 → 1.06 |
| lpa_nonnum | 1.38 MiB | 0.56 MiB | 2.5× | 4.00 → 1.03 | 2.34 → 1.06 |
| sim | 160.1 MiB | 45.8 MiB | 3.5× | 4.00 → 1.04 | 4.36 → 0.37 |
| sim_shuf | 160.1 MiB | 49.5 MiB | 3.2× | 4.00 → 1.06 | 4.36 → 0.58 |
| sim_nonnum | 163.6 MiB | 53.1 MiB | 3.1× | 4.00 → 1.06 | 4.36 → 0.58 |
| MC chr22 | 6.54 GiB | 1.17 GiB | 5.6× | 4.00 → 1.04 | 4.91 → 0.37 |
| **PGGB chr1** | **35.94 GiB** | **9.71 GiB** | **3.7×** | **4.00 → 1.19** | **4.49 → 1.03** |

B/step includes the block index; the node table (32 B/node) and strings are
unchanged. On graphs with few steps per node the node table now dominates
the file (42% of sim's new `.pdx`, 11% of MC chr22's); shrinking it is
listed under follow-ups.

`.ndx` grows by its header and community array (64 B + 4 B per community,
e.g. 66,356,000 → 66,363,640 B on MC chr22). `.lnx` and `.pcx` sizes are
unchanged.

### Node order

`index_gfa` logs the estimated step bytes per edge for both candidate orders
and keeps the cheaper one:

| graph | integer-name order | first-appearance order | chosen |
|---|---:|---:|---|
| DRB1 | 1.002 | 1.003 | integer name |
| LPA | 1.001 | 1.001 | integer name |
| lpa_shuf | 1.343 | 1.001 | first appearance |
| sim | 1.018 | 1.029 | integer name |
| sim_shuf | 1.933 | 1.029 | first appearance |
| MC chr22 | 1.004 | 1.007 | integer name |
| PGGB chr1 | 1.044 | 1.795 | integer name |

Without the scoring step, shuffled integer names would have cost 1.96 B/step
on sim_shuf (measured) instead of 1.06: community grouping alone does not
rescue integer names that carry no locality.

The brief's estimator (`scripts/pdx_step_delta_estimate.py`) on a
locality-ordered packed32 index now reports the old `comm+name` row as
`current` (1.08 B/step on sim with its 8-byte-per-block index charge).
Ordering communities by their smallest name instead of Louvain order made
no measurable difference (sim, LPA, DRB1: identical to two decimals;
MC chr22 below), so Louvain order is kept.

Estimator on the MC chr22 baseline (B/step, 8 bytes charged per 128-step
block, so about 0.045 more than the shipped block index):

| ordering | B/step |
|---|---:|
| hash rank (base ids) | 1.26 |
| first visit | 1.32 |
| community, first visit | 1.17 |
| integer name | 1.09 |
| community, integer name (shipped) | 1.09 |
| communities by smallest name, then integer name | 1.09 |

### Posting encodings tried

Bytes per posting. The candidate rows come from re-encoding every node's
postings of the new-ID `.pdx` with a size model (`posting_estimate.cpp`); the
shipped row is measured on the built files:

| encoding | DRB1 | LPA | sim | MC chr22 | PGGB chr1 |
|---|---:|---:|---:|---:|---:|
| v4 path groups (base) | 3.96 | 2.34 | 4.36 | 4.91 | 4.49 |
| P1: fold `count > 1` into the path delta | 2.96 | 2.23 | 3.47 | 3.94 | 3.86 |
| P1 + P3: block-granular first rank (rank / 128) | 2.00 | 2.08 | 2.52 | 2.95 | 3.17 |
| P1 + P4: zigzag delta of first rank across path groups | 2.62 | 2.17 | 3.69 | 3.45 | 3.46 |
| P2, one state per path, K = 32 | 0.71 | **2.75** | 0.61 | 0.39 | 1.34 |
| **P2 slots (shipped), K = 32, measured** | **0.72** | **0.98** | **0.37** | **0.37** | **1.03** |

- P1 saves 5–25% but is far behind P2. The brief's bar was 15% of posting
  bytes on chr1; the shipped encoding saves 77%.
- P3 would need one step-block decode per posting at query time, and P4 is
  worse than P1 on contig-fragmented paths (sim). Both are superseded.
- P2 with a single "last rank" per path breaks on loops: LPA's KIV-2
  copy-number region becomes 2.75 B/posting, worse than v4. Giving every
  occurrence its own slot fixes that (0.98 measured) and also helps
  elsewhere (sim 0.61 → 0.37).

Block size K (MC chr22, exact sizes, and the average number of postings a
cold single-node lookup decodes; the node's own postings average 185):

| K | B/posting | postings `.pdx` share | decoded per isolated lookup |
|---:|---:|---:|---:|
| 8 | 0.90 | 692 MB | 834 |
| 16 | 0.55 | 422 MB | 1,575 |
| **32** | **0.37** | **287 MB** | 3,056 |
| 64 | 0.28 | 219 MB | 6,021 |
| 128 | 0.24 | 185 MB | 11,950 |

Same on PGGB chr1 (383 postings per node on average):

| K | B/posting | postings | decoded per isolated lookup |
|---:|---:|---:|---:|
| 16 | 1.18 | 4.94 GiB | 3,263 |
| **32** | **1.03** | **4.29 GiB** | 6,303 |
| 64 | 0.95 | 3.97 GiB | 12,369 |
| 128 | 0.91 | 3.81 GiB | 24,777 |

K = 32 is the default. On MC chr22, query latency at K = 16, 32 and 64 was
the same within noise for every query type (see Latency), because real
queries touch runs of nearby node IDs. K only matters for isolated
single-node lookups, which the CLI and library never issue on their own.
K = 64 would save another 0.34 GiB on chr1 (3% of the file) and remains
available as `--posting_block_nodes 64`.

Why chr1 postings compress less than MC chr22 (1.03 vs 0.37 B/posting): only
67% of chr1 occurrences are predicted (MC chr22: 89%). For another 29%, the
path has a slot but its next step lands elsewhere: PGGB paths often move
backwards in node-ID order or skip to a node outside the block. Those cost
0.84 of the 1.03 B/posting, so a better predictor for them is the obvious
next step (see follow-ups).

## Reader I/O: mmap tried and rejected

The first v5 reader mapped the whole `.pdx` and decoded in place. It was fast,
but every page it touches counts toward the process's resident set. During
`index_gfa`, the `.pcx` builder scans the full step table, and MC chr22's peak
RSS went from 1.12 GB to 1.60 GB (all of it reclaimable page cache, but
`time -v`, cgroups and the gbz-base comparison benchmark all report it).
Queries would show the same inflation. The shipped reader uses `pread`
instead: one syscall per read (the v4 reader used `seekg` + `read`), no
shared stream state, and RSS back to baseline (MC chr22 build: 1,118,792 KB
base vs 1,119,020 KB new).

## Latency

Method: `scripts/pdx_ab_benchmark.py` runs each query against both binary +
index pairs. Per query and side it does one cold run (the graph and all
sidecars evicted from the page cache with `posix_fadvise(DONTNEED)` first),
one untimed warm-up and several timed warm runs, alternating sides. Every
run's output is hashed and compared across sides. Tables give, per query
type, the p50 and p95 over queries of each query's median warm time, the p50
of cold times, and the p50 of peak RSS. The machine was otherwise idle.

Query types: `path_full` is `get_path --path_id` (the reference path plus
random ones); `path_nodes_N` is `get_path --nodes` with N consecutive nodes of
a random path window; `subgraph_2000` is `get_subgraph --max_nodes 2000`;
`region_*` and `locus_*` are `get_region` in `--mode all`, `bfs`
(`--context 10000`) and `reference` (`--context 10000`), with and without
`--with_coords`, at random positions or at the ten chr1 loci of the existing
gbz-base benchmark (centromere HOR, HSat2, 1q21 segmental duplication, AMY1,
RHD/RHCE, GBA/GBAP1, FCGR, CFH/CFHR, two random loci) at 1 kb–1 Mb.

### MC chr22 (124 queries, 3 warm repeats; all outputs identical)

| query type | n | warm p50 base → new | ratio | warm p95 base → new | cold p50 base → new | ratio | RSS p50 MB base → new |
|---|---:|---|---:|---|---|---:|---|
| path_full | 4 | 1.03 s → 383 ms | 0.37 | 2.59 s → 961 ms | 1.81 s → 481 ms | 0.27 | 52 → 52 |
| path_nodes_20 | 40 | 15 ms → 9 ms | 0.59 | 24 ms → 13 ms | 49 ms → 14 ms | 0.29 | 20 → 20 |
| path_nodes_500 | 20 | 30 ms → 20 ms | 0.65 | 43 ms → 24 ms | 82 ms → 35 ms | 0.43 | 20 → 20 |
| region_all_100kb | 2 | 54 ms → 40 ms | 0.74 | 63 ms → 41 ms | 196 ms → 47 ms | 0.24 | 20 → 20 |
| region_all_10kb | 4 | 23 ms → 16 ms | 0.70 | 25 ms → 19 ms | 74 ms → 23 ms | 0.31 | 20 → 20 |
| region_all_1kb | 4 | 28 ms → 18 ms | 0.65 | 77 ms → 46 ms | 55 ms → 23 ms | 0.42 | 20 → 20 |
| region_all_coords_100kb | 2 | 66 ms → 53 ms | 0.80 | 104 ms → 68 ms | 263 ms → 75 ms | 0.28 | 20 → 20 |
| region_all_coords_10kb | 4 | 28 ms → 18 ms | 0.63 | 30 ms → 24 ms | 50 ms → 24 ms | 0.47 | 20 → 20 |
| region_all_coords_1kb | 4 | 23 ms → 17 ms | 0.75 | 23 ms → 29 ms | 94 ms → 39 ms | 0.41 | 20 → 20 |
| region_bfs_ctx10k_100kb | 2 | 152 ms → 84 ms | 0.55 | 955 ms → 632 ms | 708 ms → 185 ms | 0.26 | 62 → 61 |
| region_bfs_ctx10k_10kb | 4 | 36 ms → 22 ms | 0.62 | 47 ms → 30 ms | 111 ms → 49 ms | 0.44 | 20 → 20 |
| region_bfs_ctx10k_1kb | 4 | 34 ms → 22 ms | 0.65 | 36 ms → 24 ms | 110 ms → 42 ms | 0.38 | 20 → 20 |
| region_bfs_ctx10k_coords_100kb | 2 | 23 ms → 15 ms | 0.64 | 156 ms → 99 ms | 46 ms → 29 ms | 0.63 | 20 → 20 |
| region_bfs_ctx10k_coords_10kb | 4 | 45 ms → 34 ms | 0.74 | 60 ms → 38 ms | 184 ms → 95 ms | 0.52 | 22 → 20 |
| region_bfs_ctx10k_coords_1kb | 4 | 35 ms → 25 ms | 0.72 | 35 ms → 32 ms | 139 ms → 72 ms | 0.52 | 20 → 20 |
| region_ref_ctx10k_coords_100kb | 2 | 32 ms → 22 ms | 0.71 | 37 ms → 27 ms | 108 ms → 74 ms | 0.69 | 30 → 27 |
| region_ref_ctx10k_coords_10kb | 4 | 22 ms → 15 ms | 0.69 | 61 ms → 55 ms | 63 ms → 45 ms | 0.72 | 20 → 20 |
| region_ref_ctx10k_coords_1kb | 4 | 23 ms → 16 ms | 0.71 | 31 ms → 20 ms | 45 ms → 31 ms | 0.67 | 20 → 20 |
| subgraph_2000 | 10 | 43 ms → 24 ms | 0.56 | 55 ms → 36 ms | 158 ms → 67 ms | 0.43 | 20 → 20 |

### Reader changes that the latency depends on

- **Step-block cache.** Node-set queries in repeats produce millions of short
  runs (one chr1 query: 20 nodes, 9.26 M runs). Reading each run separately
  from delta-coded blocks made that query 1.22× slower than v4 (13.6 s vs
  11.1 s). The reader now caches the last superblock record and 8 decoded
  128-step blocks, which brings it to 7.4 s (0.67×).
- **Prefetching.** Node-set and haplotype-span queries know all their runs
  before reading any. Without help, a cold run costs one dependent index read
  plus one data read, which made small cold queries 1.1–1.25× slower than v4
  (+10 ms). `prefetch_runs` issues `posix_fadvise(WILLNEED)` for every needed
  index record and then every data range, so the disk serves them in
  parallel. Small cold queries became 2–4× faster than v4. It also works on v4
  files.
- **Block size.** MC chr22 latency at K = 16, 32 and 64 (before prefetching)
  was within noise of each other for every query type.

### PGGB chr1 (234 queries, 2 warm repeats; all outputs identical)

| query type | n | warm p50 base → new | ratio | warm p95 base → new | cold p50 base → new | ratio | RSS p50 MB base → new |
|---|---:|---|---:|---|---|---:|---|
| locus_all_coords_100kb | 10 | 145 ms → 130 ms | 0.90 | 35.72 s → 22.14 s | 649 ms → 178 ms | 0.27 | 55 → 20 |
| locus_all_coords_10kb | 10 | 119 ms → 96 ms | 0.81 | 34.64 s → 21.84 s | 404 ms → 119 ms | 0.29 | 40 → 20 |
| locus_all_coords_1Mb | 10 | 596 ms → 480 ms | 0.80 | 201.67 s → 63.32 s | 2.58 s → 537 ms | 0.21 | 128 → 24 |
| locus_all_coords_1kb | 10 | 100 ms → 78 ms | 0.78 | 35.42 s → 21.29 s | 373 ms → 119 ms | 0.32 | 37 → 20 |
| locus_bfs_ctx10k_100kb | 10 | 359 ms → 286 ms | 0.80 | 1.63 s → 1.39 s | 1.19 s → 437 ms | 0.37 | 114 → 114 |
| locus_bfs_ctx10k_10kb | 10 | 158 ms → 130 ms | 0.82 | 1.27 s → 1.26 s | 543 ms → 218 ms | 0.40 | 70 → 70 |
| locus_bfs_ctx10k_1Mb | 10 | 1.24 s → 927 ms | 0.75 | 54.12 s → 47.88 s | 3.86 s → 1.19 s | 0.31 | 251 → 254 |
| locus_bfs_ctx10k_1kb | 10 | 106 ms → 85 ms | 0.80 | 1.26 s → 1.27 s | 380 ms → 213 ms | 0.56 | 66 → 66 |
| locus_ref_ctx10k_coords_100kb | 10 | 57 ms → 47 ms | 0.84 | 1.22 s → 1.24 s | 339 ms → 245 ms | 0.72 | 117 → 107 |
| locus_ref_ctx10k_coords_10kb | 10 | 31 ms → 28 ms | 0.89 | 1.27 s → 1.30 s | 179 ms → 121 ms | 0.67 | 57 → 50 |
| locus_ref_ctx10k_coords_1Mb | 10 | 230 ms → 188 ms | 0.82 | 13.66 s → 13.56 s | 1.17 s → 567 ms | 0.48 | 218 → 201 |
| locus_ref_ctx10k_coords_1kb | 10 | 30 ms → 25 ms | 0.83 | 1.24 s → 1.24 s | 123 ms → 98 ms | 0.80 | 49 → 44 |
| path_full | 4 | 10 ms → 5 ms | 0.50 | 11.18 s → 3.28 s | 27 ms → 10 ms | 0.38 | 20 → 20 |
| path_nodes_20 | 40 | 10 ms → 7 ms | 0.71 | 2.80 s → 2.36 s | 68 ms → 17 ms | 0.25 | 20 → 20 |
| path_nodes_500 | 20 | 96 ms → 71 ms | 0.74 | 331 ms → 365 ms | 334 ms → 160 ms | 0.48 | 26 → 31 |
| region_all_10kb | 4 | 27 ms → 25 ms | 0.94 | 52 ms → 39 ms | 127 ms → 34 ms | 0.27 | 21 → 21 |
| region_all_1kb | 4 | 15 ms → 14 ms | 0.93 | 1.06 s → 1.06 s | 87 ms → 21 ms | 0.24 | 21 → 21 |
| region_all_coords_10kb | 4 | 24 ms → 22 ms | 0.90 | 34 ms → 25 ms | 166 ms → 69 ms | 0.42 | 23 → 21 |
| region_all_coords_1kb | 4 | 21 ms → 18 ms | 0.86 | 29.41 s → 19.86 s | 256 ms → 75 ms | 0.29 | 35 → 21 |
| region_bfs_ctx10k_10kb | 4 | 39 ms → 28 ms | 0.72 | 49 ms → 36 ms | 280 ms → 145 ms | 0.52 | 41 → 41 |
| region_bfs_ctx10k_1kb | 4 | 137 ms → 110 ms | 0.80 | 1.19 s → 1.26 s | 597 ms → 205 ms | 0.34 | 46 → 46 |
| region_bfs_ctx10k_coords_10kb | 4 | 64 ms → 54 ms | 0.84 | 87 ms → 65 ms | 504 ms → 296 ms | 0.59 | 133 → 95 |
| region_bfs_ctx10k_coords_1kb | 4 | 49 ms → 38 ms | 0.77 | 285 ms → 243 ms | 443 ms → 288 ms | 0.65 | 97 → 77 |
| region_ref_ctx10k_coords_10kb | 4 | 28 ms → 23 ms | 0.82 | 32 ms → 31 ms | 185 ms → 138 ms | 0.75 | 53 → 41 |
| region_ref_ctx10k_coords_1kb | 4 | 23 ms → 18 ms | 0.80 | 28 ms → 23 ms | 278 ms → 219 ms | 0.79 | 102 → 76 |
| subgraph_2000 | 10 | 732 ms → 694 ms | 0.95 | 6.59 s → 7.44 s | 689 ms → 721 ms | 1.05 | 625 → 625 |

The p95 columns are dominated by the 1q21 segmental duplication and FCGR
loci (outputs up to 25.8 GB). The three types whose p95 is higher than base
(`subgraph_2000` 1.13×, `path_nodes_500` 1.10×, `region_bfs_ctx10k_1kb`
1.06×) contain node sets in repeat-dense regions, where nodes have millions
of postings and decoding a posting block costs more than v4's per-node
blocks. Peak RSS was lower than base in 75 queries and higher in 14. The
large increases (+0.2–0.4 GB) are repeat-dense node sets, where the
decoder holds the slot state of each block's largest node (see Follow-ups).

### Simulated graph (124 queries, 3 warm repeats; all outputs identical)

| query type | n | warm p50 base → new | ratio | warm p95 base → new | cold p50 base → new | ratio | RSS p50 MB base → new |
|---|---:|---|---:|---|---|---:|---|
| path_full | 4 | 36 ms → 16 ms | 0.45 | 484 ms → 194 ms | 141 ms → 19 ms | 0.13 | 20 → 20 |
| path_nodes_20 | 40 | 3 ms → 2 ms | 0.65 | 4 ms → 3 ms | 8 ms → 5 ms | 0.56 | 20 → 20 |
| path_nodes_500 | 20 | 10 ms → 5 ms | 0.53 | 10 ms → 6 ms | 28 ms → 12 ms | 0.43 | 20 → 20 |
| region_all_100kb | 2 | 66 ms → 33 ms | 0.50 | 66 ms → 34 ms | 156 ms → 39 ms | 0.25 | 20 → 20 |
| region_all_10kb | 4 | 17 ms → 10 ms | 0.60 | 17 ms → 10 ms | 35 ms → 10 ms | 0.27 | 20 → 20 |
| region_all_1kb | 4 | 7 ms → 5 ms | 0.72 | 8 ms → 5 ms | 16 ms → 9 ms | 0.55 | 20 → 20 |
| region_all_coords_100kb | 2 | 66 ms → 34 ms | 0.51 | 67 ms → 35 ms | 168 ms → 40 ms | 0.24 | 20 → 20 |
| region_all_coords_10kb | 4 | 17 ms → 11 ms | 0.61 | 19 ms → 12 ms | 46 ms → 16 ms | 0.36 | 20 → 20 |
| region_all_coords_1kb | 4 | 9 ms → 6 ms | 0.72 | 9 ms → 7 ms | 24 ms → 12 ms | 0.49 | 20 → 20 |
| region_bfs_ctx10k_100kb | 2 | 255 ms → 141 ms | 0.56 | 302 ms → 169 ms | 518 ms → 194 ms | 0.38 | 35 → 35 |
| region_bfs_ctx10k_10kb | 4 | 194 ms → 108 ms | 0.56 | 196 ms → 121 ms | 473 ms → 138 ms | 0.29 | 30 → 30 |
| region_bfs_ctx10k_1kb | 4 | 199 ms → 111 ms | 0.56 | 203 ms → 114 ms | 464 ms → 139 ms | 0.30 | 29 → 29 |
| region_bfs_ctx10k_coords_100kb | 2 | 295 ms → 165 ms | 0.56 | 300 ms → 170 ms | 618 ms → 203 ms | 0.33 | 39 → 37 |
| region_bfs_ctx10k_coords_10kb | 4 | 167 ms → 95 ms | 0.57 | 188 ms → 108 ms | 467 ms → 128 ms | 0.27 | 30 → 28 |
| region_bfs_ctx10k_coords_1kb | 4 | 174 ms → 102 ms | 0.58 | 210 ms → 112 ms | 401 ms → 178 ms | 0.44 | 31 → 28 |
| region_ref_ctx10k_coords_100kb | 2 | 108 ms → 98 ms | 0.91 | 115 ms → 105 ms | 196 ms → 135 ms | 0.69 | 34 → 34 |
| region_ref_ctx10k_coords_10kb | 4 | 73 ms → 71 ms | 0.98 | 86 ms → 86 ms | 115 ms → 99 ms | 0.86 | 31 → 29 |
| region_ref_ctx10k_coords_1kb | 4 | 63 ms → 62 ms | 0.98 | 84 ms → 83 ms | 115 ms → 123 ms | 1.07 | 29 → 28 |
| subgraph_2000 | 10 | 21 ms → 14 ms | 0.64 | 23 ms → 15 ms | 58 ms → 27 ms | 0.46 | 20 → 20 |

## Build cost

| graph | base wall | new wall | base peak RSS | new peak RSS |
|---|---:|---:|---:|---:|
| sim | 8.7 s | 7.5 s | 237,076 KB | 237,224 KB |
| MC chr22 | 5:36 | 4:54 | 1,118,792 KB | 1,119,020 KB |
| PGGB chr1 | 35:03 | 31:02 | 3,093,844 KB | 3,093,972 KB |

The new build does slightly more work (scoring two node orders on the edge
list, sorting IDs, encoding) but writes far less, so it is faster overall.
On chr1, assigning IDs and writing `.ndx` v2 took 3.6 s (base `.ndx`: 1.7 s).
Peak RSS is unchanged; the community array is freed before path indexing.

## Correctness

- `ctest`: 14 tests pass, including two new ones.
  `tests/test_pdx_codec.cpp` round-trips the step codec (all varint lengths,
  block and superblock edges, random ranges), the slot posting codec
  (loops, backward jumps, empty nodes, block sizes 1/3/32/64, truncation),
  node ID assignment (integer, prefixed, mixed and overflowing names) and
  `.ndx` v2 (lookups, communities, padding, damaged files, pairing).
  `tests/test_node_order_equivalence.sh` indexes six graphs (simulated W
  lines, the same as P lines, non-numeric and shuffled-integer renamings, and
  two fixtures with loops) with `legacy_hash`, `auto`, and `auto` with 3-node
  posting blocks. It requires byte-identical `get_path`, `get_subgraph` and
  `get_region` output (all modes, with and without coordinates, with and
  without `.cdx`), and checks that mixed `.pdx`/`.cdx` numberings are
  rejected.
- `--node_order legacy_hash` reproduces every base output file
  (`.gz`, `.idx`, `.ndx`, `.lnx`, `.pdx`, `.pcx`) byte for byte on DRB1, LPA,
  sim and lpa_nonnum.
- Round trip on every graph: all paths dumped from base and new `.pdx` as GFA
  text are byte-identical, and every node's `(path, rank)` multiset is
  identical (order-independent digest per node name). This covers all
  4.49 B chr1 steps and postings.
- Query equivalence: 1,000-query sweeps (random node sets, subgraphs and
  regions in all modes) on MC chr22 and chr1, plus every latency query on
  sim, MC chr22 and chr1, gave 0 output differences and 0 failures.
- The Python bindings build and return identical results on base and new
  indexes.

## Compatibility

- New binaries read v4 `.pdx` and headerless `.ndx` (with `pread` and the
  prefetch, existing v4 indexes also query faster). Old binaries reject
  `.ndx` v2 (size check) and `.pdx` v5 (version check) with an error.
- `.pdx` v5 stores the `.ndx` fingerprint and `.cdx` a 32-bit tag of it;
  `get_path`, `get_subgraph`, `get_region`, `index_coordinates` and the
  library check them and name the file to rebuild. `.lnx` has no spare header
  field; `index_gfa` always rebuilds it together with `.ndx`, and the README
  says so.
- `.pcx` stores cumulative sequence lengths, which do not depend on node IDs
  or step encoding, so no `.pcx` change was needed (`docs/todo.md` notes
  this next to the existing fingerprinting item).
- `index_paths` on an existing legacy index writes a v5 `.pdx` that keeps the
  hash-rank IDs (the default encodings work with any IDs, but compress far
  less without the new numbering); `--step_encoding packed32
  --posting_encoding groups` writes a v4 file.

## Rejected or deferred

| option | result | decision |
|---|---|---|
| integer name as the only key | 1.96 B/step on shuffled names | replaced by edge-scored choice |
| communities ordered by smallest name | no change on any graph | Louvain order kept |
| P1, P3, P4 posting tweaks | 5–49% smaller postings | superseded by slots (58–92%) |
| P2 with one state per path | 2.75 B/posting on LPA (worse than v4) | per-occurrence slots |
| mmap reader | +480 MB peak RSS on MC chr22 build | `pread` |
| K = 64 posting blocks | 0.34 GiB smaller on chr1 | available via flag; default 32 |
| smaller node table (32 → 16–24 B/node) | ~0.18 GiB on chr1, 42% of sim's file | deferred (optional in the brief) |
| `.pcx` step-table fingerprint | not needed for this change | left in `docs/todo.md` |

## Follow-ups

- **Better prediction for PGGB postings.** On chr1, 29% of occurrences are
  explicit even though their path has a slot, and they cost 0.84 of the
  1.03 B/posting. Coding them relative to the nearest slot, or predicting
  `rank - 1` for paths that run backwards in ID order, should shrink chr1
  postings well below 1 B.
- **Node table.** 32 B/node of which 16 are offsets and lengths that could
  be derived. Matters most for graphs with few steps per node.
- **Upgrading existing indexes without the source GFA.** The large HPRC
  indexes in `~/gfaidx_benchmark` were built from GFAs that are no longer on
  disk. Their `.ndx` already holds the communities and their `.pdx` the
  paths, so a converter could renumber nodes and rewrite `.ndx`, `.lnx`,
  `.pdx` and `.cdx` without rerunning Louvain or reading a GFA.
- **Memory on repeat nodes.** Decoding a posting block holds the slot state of
  its largest node (16–24 bytes per occurrence). On repeat-dense chr1 node-set
  queries, peak RSS was 1.3–2.2× baseline (e.g. 0.83 → 1.26 GB). Those
  queries were also faster. RSS was lower than baseline in most other chr1
  queries.

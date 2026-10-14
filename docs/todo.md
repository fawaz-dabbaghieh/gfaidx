# TODO

This file tracks concrete work that has been intentionally deferred. Broader
design notes remain in `ideas.md`.

## Correctness

### Bind `.pcx` To Its Source `.pdx` And `.lnx`

**Status:** deferred

The current `.pcx` validation checks counts and path metadata, but it does not
verify the packed path-step values or the rank-aligned node lengths that
produced its cumulative coordinates. A stale `.pcx` built from different,
same-shaped source indexes can therefore pass validation and silently emit
incorrect P/W coordinates.

Required work:

- fingerprint the ordered packed step table in `.pdx`
- fingerprint the rank-ordered `uint32` length array in `.lnx`
- store both expected fingerprints in a versioned `.pcx` header
- compare the stored source fingerprints in constant time when loading `.pcx`
- reject incompatible or legacy checkpoint indexes and use the existing
  path-prefix fallback with a warning
- add regression tests using same-sized indexes with different steps and
  different node lengths

Migration requirement:

- provide a way to fingerprint existing large `.pdx` and `.lnx` files without
  rebuilding their full indexes; a small versioned footer or an upgrade command
  should be considered

Performance constraint:

- do not hash the complete `.pdx` at every query, because that would remove the
  main performance benefit of `.pcx`

### Overlap-Aware Coordinates For De Bruijn Graphs

**Status:** deferred

Walk and path coordinates are computed by summing segment lengths from
`SeqStart`, which assumes consecutive segments do not overlap. In a de Bruijn
graph, consecutive k-mer nodes overlap by `k - 1` bases (`30M` links for
`k = 31`), so the summed length overshoots `SeqEnd`. gfaidx then warns
`W-line '...' has segment lengths beyond SeqEnd` and writes the subwalk
without coordinates. Example from `test_graphs/dbg_ecoli` (Bifrost, `KL:Z:31`),
walk `562.99687_AMP-R_CIP-S|0|562.99687.con.0002|136000|156000`:

- `SeqEnd - SeqStart`: 20,000 bp
- summed lengths of its 2,549 segments: 96,450 bp
- summed lengths minus 30 bp per link: 20,010 bp

Proposed approach: the user supplies one constant overlap for the whole graph,
for example `--overlap 30`, which covers de Bruijn graphs without per-link
CIGAR handling. Because the overlap is constant, the correction is linear in
the step rank: the start of step `i` is `prefix(i) - i * overlap`, and a
subwalk of `n` steps covers `sum(lengths) - (n - 1) * overlap` bases. Existing
`.pcx` checkpoints therefore stay valid; the correction is applied when they
are read, so no reindexing is needed.

Required work:

- add the overlap option to `get_path`, `get_subgraph`, `get_region`,
  `index_coordinates`, and the library/Python options
- apply the correction in both coordinate builders in `src/paths/walk_coords.cpp`
  (the full-slice path and the `.pcx` fast path)
- build `.cdx` tracks with the same correction, so region lookup by
  coordinate works on overlap graphs
- accept P lines whose overlaps all equal the given value, instead of
  refusing every P line with overlaps
- optionally check the given overlap against the `L` CIGARs, or default it from
  a `KL:Z:<k>` header tag as `k - 1`
- explain the 10 bp left over in the example above (trimmed walk ends, or a
  different coordinate convention in the tool that wrote the walks) before
  treating `SeqEnd` mismatches as errors
- add a small de Bruijn fixture with known coordinates to the tests

## Performance

### Index Shared Cross-Community Links Per Community

**Status:** deferred

The last `.idx` span is one gzip member holding every link between
communities. Since the region-context work, `bfs` and `--context` queries read
it once per query instead of once per visited community, but they still
decompress and scan the whole member. On large graphs this member can dominate
the cost of small queries.

Proposed approach: group the shared links by community, write each group as
its own gzip member, and index the members per community. A query would then
read only the shared links of the communities it visits.

### Reference-Mode Memory Above The Pre-Context Implementation

**Status:** deferred, not profiled

`--mode reference --context 0` writes output byte-identical to the earlier
reference mode, but peak RSS rose from about 14.5 MB to 36 MB on a 1 Mb chr22
query. The likely sources are the decompressed gzip members kept for
materialization and the in-memory link graph, but this has not been measured.

### Parallelize Path-Supported Subgraph And Region Extraction

**Status:** completed in version 1.9.4 for P/W formatting and coordinates

Version 1.9.4 parallelizes the dominant P/W phase for `get_subgraph` and
`get_region` while retaining deterministic main-thread output.

Implemented:

- `--threads` defaults to one and accepts 1 through 256 workers
- workers own independent `PathIndexReader` streams and share a frozen,
  immutable selected-node-name cache
- short records use bounded batches; chromosome-scale records remain separate
- a bounded ring preserves record order and limits in-flight output memory
- CMake `Threads::Threads` supplies portable Linux/macOS thread linkage

BFS, shared-edge construction, `.cdx` lookup, graph materialization, and the
final output writer intentionally remain serial.

Posting decoding and all-haplotype selected-step scanning were already small in
the measured workload. Parallelizing them remains deferred until new profiling
shows that either phase is a material bottleneck.

See `extraction_optimizations.md` for the design, correctness checks, and full
1/2/4/8-thread resource tables.

## Compatibility

### Allow `get_region --print_path_names` With A `.cdx` But No `.pdx`

**Status:** completed

Graphs indexed with `index_gfa --no_paths` do not have a `.pdx`, but
`index_coordinates` can still build a useful `.cdx` from rGFA `S` records.
The listing path now opens `.pdx` and `.cdx` independently, so these
`.cdx`-only coordinate tracks remain visible.

Implemented behavior:

- when both `.pdx` and `.cdx` exist, list all P/W records from `.pdx`, annotate
  tracks accelerated by `.cdx`, and retain `.cdx`-only tracks
- when only `.pdx` exists, list its P/W records and report whether on-the-fly
  coordinate lookup is available
- when only `.cdx` exists, list its coordinate tracks without requiring `.pdx`
- when neither exists, report that no path or coordinate index is available

Regression coverage:

- build a graph with `index_gfa --no_paths`
- build an rGFA S-derived `.cdx`
- verify that `get_region --print_path_names` lists the `.cdx` tracks

Implemented with the clearer `--list_coordinates` alias while retaining
`--print_path_names` for compatibility.

### Wire Up `get_region --debug_trace`

**Status:** deferred

`get_region` accepts `--debug_trace` but never reads it, so the flag has no
effect. Tracing currently works only through the `GFAIDX_DEBUG_SUBGRAPH=1`
environment variable. `get_subgraph` already passes its flag through.

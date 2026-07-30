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

## Performance

### Parallelize Path-Supported Subgraph And Region Extraction

**Status:** deferred

Large `get_subgraph` and `get_region --all_haplotypes` queries currently
perform posting decoding, selected path-step retrieval, coordinate calculation,
and P/W formatting serially. Queries that touch tens of millions of postings
and path steps can therefore spend substantial time after the initial graph or
coordinate selection has completed.

Initial profiling work:

- add separate timers around posting decoding, selected path-step retrieval,
  node-rank sorting and deduplication, coordinate calculation, and P/W output
- benchmark local SSD and network storage separately, because parallel random
  reads may not improve both environments
- record peak memory as thread count increases

Recommended implementation:

- add a `--threads` option with a default of one to preserve current behavior
- divide reference-node posting blocks among workers and merge thread-local
  minimum/maximum path-step bounds
- divide selected path intervals among workers and fill disjoint portions of a
  preallocated node-rank array
- group subpath runs by path and calculate coordinates and formatted P/W
  records in parallel
- buffer formatted records in bounded batches and write them in deterministic
  path order from the main thread
- reuse the posting-parallelization machinery for generic `get_subgraph`
  subpath discovery if profiling shows that stage remains expensive

Current implementation constraints:

- `PathIndexReader` owns one seek-based `std::ifstream` and mutable node
  metadata/name caches, so one shared reader is not currently thread-safe
- the least invasive first implementation can give each worker its own
  `PathIndexReader`; a later offset-based `pread` reader could avoid duplicated
  path metadata and caches
- `.lnx` and `.pcx` are read-only memory mappings and can be shared by workers
- direct writes from workers to one output stream must be avoided because they
  would either corrupt output or require a lock that serializes the work

Keep serial initially:

- BFS expansion, because parallel frontier processing can change which nodes
  are admitted at `--max_nodes`
- shared-edge adjacency construction, because its maps and vectors are mutable
- `.cdx` binary search, because it is normally small relative to posting and
  step processing

Compatibility requirements:

- do not change `.pdx`, `.lnx`, `.pcx`, or `.cdx` formats
- do not require index regeneration
- preserve deterministic node, edge, path, and walk output ordering
- verify that `--threads 1` and `--threads N` produce byte-identical GFA output

Suggested first benchmark:

- compare 1, 2, 4, and 8 threads on the large chromosome query previously used
  to exercise tens of millions of postings and selected path steps
- report posting time, step-reading time, coordinate/output time, total wall
  time, CPU utilization, peak memory, and bytes read

## Compatibility

### Allow `get_region --print_path_names` With A `.cdx` But No `.pdx`

**Status:** deferred

Graphs indexed with `index_gfa --no_paths` do not have a `.pdx`, but
`index_coordinates` can still build a useful `.cdx` from rGFA `S` records.
Currently, `get_region --print_path_names` rejects the request when `.pdx` is
missing, so these `.cdx`-only coordinate tracks cannot be listed.

Required behavior:

- when both `.pdx` and `.cdx` exist, list all P/W records from `.pdx`, annotate
  tracks accelerated by `.cdx`, and retain `.cdx`-only tracks
- when only `.pdx` exists, list its P/W records and report whether on-the-fly
  coordinate lookup is available
- when only `.cdx` exists, list its coordinate tracks without requiring `.pdx`
- when neither exists, report that no path or coordinate index is available

Required tests:

- build a graph with `index_gfa --no_paths`
- build an rGFA S-derived `.cdx`
- verify that `get_region --print_path_names` lists the `.cdx` tracks

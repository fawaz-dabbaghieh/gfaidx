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

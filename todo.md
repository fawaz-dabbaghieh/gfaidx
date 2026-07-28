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

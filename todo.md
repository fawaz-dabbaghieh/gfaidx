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

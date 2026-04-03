# GFAIDX Local Changes

This directory is not an untouched vendor drop. `gfaidx` carries local changes
here where correctness or memory behavior matters for indexing.

## Community Contraction Copy Reduction

In `Community::partition2graph_binary()`, the original code built a temporary
`communities` structure and then copied it into `BGraph::nodes` via
`BGraph(vector<vector<int>>& c_nodes)`.

Local change:

- added `BGraph(vector<vector<int>>&& c_nodes)` to take ownership of the
  temporary nested vector by move
- switched `partition2graph_binary()` to construct `g2` with
  `std::move(communities)`

Why:

- removes one full deep copy of community membership during each contraction
  step
- reduces temporary memory overlap inside the Louvain method

## Binary Reader Fix

The binary graph reader was also corrected locally to read `links` using
4-byte entries, matching both the documented format and the current
`direct_binary_writer` output.

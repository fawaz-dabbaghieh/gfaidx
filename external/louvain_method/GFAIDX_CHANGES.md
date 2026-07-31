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

## Self-Loop Scan Reuse

The original local-moving loop scanned each node's adjacency list in
`neigh_comm()`, then scanned it again in both `remove()` and `insert()` to find
the same self-loop weight.

Local change:

- return the self-loop weight found during the required `neigh_comm()` scan
- pass that value to `remove()` and `insert()` instead of calling
  `nb_selfloops()` again

Why:

- removes two redundant adjacency scans for every processed node and pass
- preserves the existing node order, modularity calculation, and treatment of
  weighted and unweighted self-loops

## Binary Reader Fix

The binary graph reader was also corrected locally to read `links` using
4-byte entries, matching both the documented format and the current
`direct_binary_writer` output.

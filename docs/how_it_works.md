# How gfaidx works

This page explains what `gfaidx index_gfa` builds, what each index file
contains, and how a query uses them. For commands and options, see the
[command reference](cli_reference.md).

## Contents

- [Indexing, step by step](#indexing-step-by-step)
- [The index files](#the-index-files)
- [How a query reads the index](#how-a-query-reads-the-index)
- [Node IDs](#node-ids)
- [The path index](#the-path-index)
- [Compatibility between files and versions](#compatibility-between-files-and-versions)
- [Resources and limits](#resources-and-limits)

## Indexing, step by step

`gfaidx index_gfa graph.gfa graph.gfa.gz` runs these steps:

1. **Check the input and collect the links.** The GFA, plain or gzipped, must
   be GFA 1 (1.0, 1.1 or 1.2) and have at least one `L` line. GFA 2 is
   rejected. Every `L` line is written to an edge list, and `P` and `W` lines
   are counted.
2. **Find communities.** The edge list is sorted with the system `sort` and
   Louvain community detection splits the graph into communities: groups of
   nodes with many links inside the group and few to the rest of the graph.
   Nodes without any link go into one extra community.
3. **Adjust community sizes (optional).** `--max_chunk_nodes N` runs Louvain
   again inside every community of at least `N` nodes to split it into
   smaller pieces, stopping before any piece would grow above `N` nodes.
   `--min_chunk_nodes N` merges
   communities smaller than `N` nodes into the neighboring community they
   share the most links with. Both are off by default.
4. **Write the graph in blocks.** Each community's `S` lines and the `L` lines
   inside it become one gzip block (a separate gzip member) of the output
   `.gz`. Links between two communities go into one final shared block. The
   `.idx` records where each block starts and how long it is.
5. **Number the nodes (`.ndx`).** Nodes get integer IDs community by
   community, so each community owns one continuous range of IDs. Inside a
   community, nodes are ordered so that nodes that follow each other on paths
   get nearby IDs (see [Node IDs](#node-ids)). The `.ndx` maps every segment
   name to its ID and community.
6. **Store node lengths (`.lnx`).** One length per node ID.
7. **Index the paths (`.pdx`).** Every `P` and `W` line is stored as a list of
   node IDs, together with the reverse lookup: for every node, which paths
   pass through it and at which step (see [The path index](#the-path-index)).
   Skipped with `--no_paths`.
8. **Store path-length checkpoints (`.pcx`).** For every path, the sequence
   length up to every 4,096th step, so a position on a long path is found
   without adding up every node before it.
9. **Write a summary (`.params`).** A JSON file with the gfaidx version, the
   options used, graph counts (segments, links, total sequence length, paths,
   walks, path steps), the number of communities and the size of each file.

All files are written inside a `gfaidx_tmp_*` directory, under `--tmp_dir` or
beside the input, and only moved to their final names once every step has
finished. The `.gz` is moved last. A failed or interrupted run leaves no
partial index beside the output.

The `.gz` is itself a valid gzip file: `zcat graph.gfa.gz` prints the `H`, `S`
and `L` lines, grouped by community. `P` and `W` lines are stored only in the
`.pdx`.

## The index files

| File | Contents | Built by |
|---|---|---|
| `graph.gfa.gz` | the graph's `S` and `L` lines as one gzip block per community, plus a final block of links between communities | `index_gfa` |
| `.idx` | the offset and size of each block in the `.gz` (text) | `index_gfa` |
| `.ndx` | a hash table from segment name to node ID and community | `index_gfa` |
| `.lnx` | the sequence length of each node, by node ID | `index_gfa` |
| `.pdx` | every `P` and `W` path as node IDs, and for every node the paths and steps that visit it | `index_gfa` (or `index_paths`) unless `--no_paths` |
| `.pcx` | cumulative path lengths every 4,096 steps | `index_gfa` (or `index_path_checkpoints`) unless `--no_paths` |
| `.params` | a JSON summary of the run; queries do not read it | `index_gfa` |
| `.cdx` | positions of selected reference paths or rGFA segments, for coordinate queries | `index_coordinates` (optional) |

Every file is named after the `.gz`, for example `graph.gfa.gz.ndx`, so the
query commands and libraries find them without extra options.

## How a query reads the index

A query only decompresses the blocks that contain the nodes it needs:

1. **Find the nodes.** `get_subgraph` starts from a node name. `get_region`
   turns `sequence:start-end` into the nodes on that stretch of the reference,
   using the `.cdx`, or the `.pdx` and `.lnx` when there is no `.cdx`.
2. **Find their blocks.** The `.ndx` gives each node's ID and community, and
   the `.idx` gives the block of each community.
3. **Read the blocks.** Only those blocks are decompressed. The `S` lines of
   the selected nodes and the `L` lines between them are written out. Links
   that cross communities come from the shared block.
4. **Add the paths.** The `.pdx` gives every `P` and `W` path through the
   selected nodes, cut to the parts that stay inside the selection. With
   `--with_coords`, the `.lnx` and `.pcx` give each part its coordinates.

`get_subgraph`, `get_region --mode bfs` and `get_region --mode reference` grow
the selection outwards through the links, up to a node count or a base-pair
distance. `get_region --mode all`
instead follows the paths: it takes every path that passes through the
reference nodes of the interval and keeps each path's stretch between its
first and last such node. See [How `--mode all` preserves
paths](cli_reference.md#how---mode-all-preserves-paths).

## Node IDs

The `.ndx` sorts nodes by community and, inside a community, by a locality
key. The key is the integer segment name when the graph builder numbered nodes
along the paths (PGGB after `odgi sort`, Minigraph-Cactus, minigraph's `s123`
style names), otherwise the order in which nodes first appear in the `L`
lines. `index_gfa` estimates how well each candidate order would compress the
path index, from the sorted edge list, and keeps the better one, so graphs
whose integer names carry no order fall back automatically. The node ID is the
position in that order. Segment names stay the identity of a node; the integer
is only a sort key.

`--node_order legacy_hash` keeps the earlier hash-based IDs and writes the
older `.ndx` and `.pdx` formats, which gfaidx 2.1 and older can read.

## The path index

The `.pdx` stores two views of the same paths:

- **Path first**: each path points to its range of steps, so a whole path or
  any slice of it is read directly.
- **Node first**: each node points to its postings, the `(path, step)` pairs
  where a path visits it, so the paths through a set of nodes are found
  without reading whole paths.

**Step table.** Each step is stored as the difference to the previous node ID
on the path, plus its orientation, in a variable number of bytes. Because
neighboring nodes on a path have nearby IDs, most differences are small and
take one byte. The table restarts every 128 steps, so any slice can be decoded
on its own, and a small table of offsets finds the block of any step.

**Postings.** Postings are stored in blocks of 32 consecutive node IDs. When a
path visits node `n` at step `r`, it usually visited a nearby node of the same
block at step `r - 1`. Such visits are predicted and only marked in a small
bitmap or list; the remaining visits are stored explicitly. Loops are
predicted too, because each visit is tracked separately.

On the HPRC v2 PGGB chromosome 1 graph (11.7 million nodes, 4.49 billion path
steps) this makes the `.pdx` 9.7 GiB instead of 35.9 GiB with the earlier
format, and queries are faster. See
[pdx_compression_results.md](pdx_compression_results.md) for the full
measurements and the alternatives that were tried.

While querying, gfaidx reads the `.pdx` with positioned reads, caches recently
decoded blocks, and asks the operating system to prefetch all the blocks a
query will read. Path metadata is loaded when the index is opened; node names
are looked up through the `.ndx` instead of being held in memory.

## Compatibility between files and versions

- The `.pdx`, `.lnx` and `.cdx` refer to nodes by the IDs assigned in the
  `.ndx`, so they must come from the same `index_gfa` run. The `.pdx` stores a
  fingerprint of the `.ndx` and the `.cdx` a tag of it, and every query
  refuses files that do not match. Rebuild the `.pdx` and `.cdx` whenever the
  `.ndx` is rebuilt.
- The `.pcx` stores sequence lengths, which do not depend on node IDs.
- gfaidx 2.3 reads indexes built by older versions. gfaidx 2.1 and older
  cannot read indexes built with the default options of 2.3; build those with
  `--node_order legacy_hash`.

## Resources and limits

- `index_gfa` runs the system `sort`. On Linux this is GNU coreutils, which is
  installed by default on most systems; on Alpine Linux, install it with
  `apk add coreutils`.
- Temporary files go to `--tmp_dir`, or beside the input GFA. Path indexing
  needs about 12 bytes of temporary space per path step, and more with
  `--threads`; see the `--threads` option of
  [`index_gfa`](cli_reference.md#gfaidx-index_gfa).
- As an example, the HPRC v2 PGGB chromosome 1 graph indexed in about 31
  minutes with a peak of 3.1 GB of memory on one thread.
- Only GFA 1 is supported, and the graph needs at least one `L` line.
- Coordinates cannot be computed for `P` lines with overlaps (a CIGAR other
  than `*` in the overlap field), because node lengths alone do not give their
  positions.
- The `.ndx` identifies names by two hash values (64-bit and 32-bit FNV-1a)
  and does not store the names themselves, so a lookup could in principle
  match a different name with the same two hashes.
- gfaidx runs on Linux and macOS only. Parts of the index reading code use
  Unix system calls (`open`, `pread`, `mmap`).

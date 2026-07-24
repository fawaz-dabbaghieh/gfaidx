# gfaidx

`gfaidx` is a command-line tool for indexing genome graphs stored in GFA.
It writes a multi-member gzip version of the graph and several sidecar indexes.
The goal is to give random and fast access to GFA graphs, including large
graphs, without keeping the full graph in RAM. The main indexes live on disk and
are read only for the part of the graph or path being queried.

Main tasks:

- split a graph into community-based gzip chunks
- look up a node's community and stream one chunk
- extract a BFS subgraph around a node or coordinate interval
- index `P` and `W` lines for full-path and subpath queries
- index reference coordinates, or derive coordinates from indexed paths when possible

## Contents

- [What the indexes are](#what-the-indexes-are)
  - [Chunk index](#chunk-index)
  - [Path index](#path-index)
- [Build and install](#build-and-install)
  - [Supported platforms](#supported-platforms)
  - [C++ CLI](#c-cli)
  - [Bioconda](#bioconda)
  - [Python helper install](#python-helper-install)
- [CLI](#cli)
  - [`gfaidx index_gfa`](#gfaidx-index_gfa)
  - [`gfaidx get_subgraph`](#gfaidx-get_subgraph)
  - [`gfaidx index_coordinates`](#gfaidx-index_coordinates)
  - [`gfaidx get_region`](#gfaidx-get_region)
  - [`gfaidx get_chunk`](#gfaidx-get_chunk)
  - [`gfaidx index_paths`](#gfaidx-index_paths)
  - [`gfaidx index_path_checkpoints`](#gfaidx-index_path_checkpoints)
  - [`gfaidx get_path`](#gfaidx-get_path)
  - [Build `.lnx` for existing indexes](#build-lnx-for-existing-indexes)
- [How the path index works](#how-the-path-index-works)
- [Utility scripts](#utility-scripts)
- [Python helpers](#python-helpers)
- [Notes](#notes)
- [TODO](#todo)

## What the indexes are

### Chunk index

`index_gfa` writes the query-ready graph plus sidecar indexes by default:

- `<graph>.gz`
  the multi-member gzip file containing one member per community plus a final shared-edge member
- `<graph>.gz.idx`
  a community-to-gzip-offset index
- `<graph>.gz.ndx`
  a sorted binary hash table mapping node string IDs to community IDs
- `<graph>.gz.lnx`
  a rank-aligned `uint32` node-length table used for coordinate-bearing W subwalk output
- `<graph>.gz.pdx`
  a binary path index for `P` and `W` lines
- `<graph>.gz.pcx`
  a small path-coordinate checkpoint index used to avoid long `.pdx` prefix scans
- `<graph>.gz.cdx`
  an optional standalone reference-coordinate index built by `index_coordinates`

`get_subgraph` uses `.idx`, `.ndx`, and optionally `.pdx` to extract a BFS
neighborhood across communities.

`get_region` additionally uses `.cdx` to resolve a 0-based reference interval
to `.ndx`/`.pdx` node ranks before running the same graph extraction pipeline.
When `--with_coords` is requested, `.lnx` supplies node lengths without
re-scanning all `S` lines and `.pcx` supplies cumulative path-length
checkpoints. The `.cdx`, `.lnx`, and `.pcx` files are separate sidecars, so
existing graph and path indexes remain compatible.

`get_chunk` remains available as the compatibility command for streaming a
single community member back out without graph expansion.

### Path index

`index_paths` writes:

- `<paths>.pdx`
  a binary index for `P` and `W` lines

The `.pdx` stores:

- path metadata
- node metadata
- a packed step table
- compressed per-node postings
- a shared string blob

Important: `.pdx` node IDs are aligned to the sorted entry rank in the `.ndx` file used during `index_paths`. That lets `get_path` resolve node names through `.ndx` without loading a giant global node-name map into memory.

## Build and install

### Supported platforms

`gfaidx` targets Unix-like systems only:

- Linux
- macOS

It is not supported on Windows. Parts of the index I/O layer use Unix system
calls such as `open`, `fstat`, `mmap`, and `close`.

### C++ CLI

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j 4
```

To install the executable into a prefix:

```bash
cmake --install build --prefix /usr/local
```

For a user-writable install, replace `/usr/local` with another prefix, for
example `$HOME/.local`.

### Bioconda

A Bioconda package is planned. Once the recipe is merged, installation will be:

```bash
conda install -c bioconda gfaidx
```

### Python helper install

```bash
pip install -e .
```

## CLI

### `gfaidx index_gfa`

Build the chunked gzip graph plus `.idx`, `.ndx`, `.lnx`, and by default `.pdx`
and `.pcx`.

```bash
gfaidx index_gfa <in_gfa> <out_gfa.gz> [options]
```

Arguments:

- `in_gfa`
  input GFA graph
- `out_gfa.gz`
  output multi-member gzip graph

Options:

- `--keep_tmp`
  keep temporary files instead of deleting them at the end
- `--tmp_dir <dir>`
  base directory for temporary files
- `--progress_every <N>`
  progress logging interval while reading the GFA
- `--gzip_level <1..9>`
  gzip compression level for the final chunked output
- `--gzip_mem_level <1..9>`
  zlib memory level for gzip compression
- `--max_chunk_nodes <N>`
  re-run Louvain inside communities containing at least `N` nodes and prevent
  small-community merges from producing a chunk larger than `N`; `0` disables
  refinement and leaves merging without an upper bound
- `--min_chunk_nodes <N>`
  merge communities smaller than `N` nodes into the neighboring community with
  the most connecting edges; `0` disables small-community merging
- `--no_paths`
  skip building `<out_gfa.gz>.pdx` and `.pcx`; still write `.gz`, `.idx`,
  `.ndx`, and `.lnx`

Outputs:

- `<out_gfa.gz>`
- `<out_gfa.gz>.idx`
- `<out_gfa.gz>.ndx`
- `<out_gfa.gz>.lnx`
- `<out_gfa.gz>.pdx` unless `--no_paths` is used
- `<out_gfa.gz>.pcx` unless `--no_paths` is used

Example:

```bash
gfaidx index_gfa graph.gfa graph.indexed.gfa.gz --tmp_dir /scratch/tmp
```

### `gfaidx get_subgraph`

Extract a BFS neighborhood subgraph from the indexed graph, optionally followed
by the matching `P/W` subpaths if a companion `.pdx` is available.

```bash
gfaidx get_subgraph <in_indexed_gz> <start_node> <out_gfa> [options]
```

Arguments:

- `in_gz`
  input indexed gzip graph produced by `index_gfa`
- `start_node`
  node ID to start the BFS neighborhood from
- `out_gfa`
  output file for the extracted GFA subgraph

Index options:

- `--idx <path>`
  override the companion `.idx`; defaults to `<in_gz>.idx`
- `--ndx <path>`
  override the companion `.ndx`; defaults to `<in_gz>.ndx`
- `--pdx <path>`
  override the companion `.pdx`; defaults to `<in_gz>.pdx`

- `--max_nodes <N>`
  BFS node cap; defaults to `100`

Behavior:

- explicit `--idx`, `--ndx`, and `--pdx` take priority over inferred companion files
- if `.pdx` is present, `get_subgraph` appends the matching `P/W` subpaths for the extracted node set
- if `.pdx` is missing and was not explicitly requested, `get_subgraph` warns and continues with `S/L` output only
- BFS discovery uses a simple node-level adjacency, but final output re-streams the original chunk lines so emitted `S/L` records preserve the original GFA edge orientations

Examples:

```bash
gfaidx get_subgraph graph.indexed.gfa.gz s12345 neighborhood.gfa
gfaidx get_subgraph graph.indexed.gfa.gz s12345 neighborhood.gfa --max_nodes 2000
```

### `gfaidx index_coordinates`

Build a standalone `.cdx` aligned to an existing `.ndx`. Reference W records
are selected from the header `RS:Z` sample list. If the supplied GFA is an
indexed gzip that no longer contains W records, `index_coordinates` can read the
reference W metadata and steps from a companion `.pdx`. If no eligible reference
W record exists, `SR:i:0` segments with `SN` and `SO` tags are indexed instead.
Alternatively, provide a filtered `get_path --print_path_names` output file to
index explicit P paths and W walks from the `.pdx`.

```bash
gfaidx index_coordinates <in_gfa> <out.cdx> [options]
```

Options:

- `--ndx <path>`
  node hash index; defaults to `<in_gfa>.ndx` when present
- `--pdx <path>`
  optional path index used as the source of reference W records when they are
  absent from the supplied GFA; defaults to `<in_gfa>.pdx` when present
- `--reference <sample>`
  index only this sample from the header `RS:Z` list; by default all listed
  reference samples are indexed
- `--path_names_file <path>`
  tab-separated file in the same format emitted by
  `gfaidx get_path graph.gfa.gz --print_path_names`; selected `W` rows keep their
  W coordinates, while selected `P` rows are indexed as path-local coordinates
  starting at 0 and advancing by segment length. P paths with non-`*` overlaps
  are rejected because their coordinate lengths would be ambiguous.
- `--progress_every <N>`
  report input progress every `N` lines; `0` disables progress logging

Example:

```bash
gfaidx index_coordinates chr22.gfa chr22.gfa.gz.cdx \
  --ndx chr22.gfa.gz.ndx --reference CHM13

gfaidx index_coordinates chr22.indexed.gfa.gz chr22.gfa.gz.cdx --reference CHM13

# print_path_names prints all available paths in the indexed graph
gfaidx get_path graph.indexed.gfa.gz --print_path_names > path_names.tsv
# edit or filter path_names.tsv, then index exactly those P/W records
gfaidx index_coordinates graph.indexed.gfa.gz graph.gfa.gz.cdx --path_names_file path_names.tsv
```

### `gfaidx get_region`

Resolve a 0-based, half-open reference interval through `.cdx`, translate its
node ranks through `.pdx`, and either use all overlapping reference nodes as BFS
seeds or select the P/W path spans supported by those nodes. If `.cdx` is absent
or does not contain the requested coordinate track, `get_region` can fall back
to the resolved `.pdx` and `.lnx` to compute coordinates on the fly for any
indexed `P` path or concrete-coordinate `W` walk.

```bash
gfaidx get_region <in_gz> <sequence:start-end> <out_gfa> [options]
```

Important options:

- `--reference <sample>`
  select the coordinate namespace when multiple reference samples contain the
  requested sequence
- `--cdx`, `--idx`, `--ndx`, `--pdx`, `--lnx`, `--pcx`
  override companion indexes; each defaults to `<in_gz>.<suffix>`
- `--max_nodes <N>`
  cap the total seed plus BFS node count; it must be at least the seed count.
  This limit is not used with `--all_haplotypes`
- `--all_haplotypes`
  avoid BFS and use the `.pdx` posting table to find every indexed P/W record
  containing a reference interval node. For each matching record, include every
  step between its minimum and maximum matching step, then materialize the exact
  union of those nodes and edges whose endpoints are both in the union. This
  mode assumes the graph nodes of interest are covered by indexed P/W records;
  graph-only nodes are not discovered. A path with only one matching reference
  node contributes that node only
- `--no_paths`
  omit P/W output; `.pdx` remains required for rank-to-node-name conversion
- `--with_coords`
  emit returned `W` subwalks with concrete `SeqStart`/`SeqEnd` coordinates and
  returned no-overlap `P` subpaths with path-local coordinate names such as
  `CHM13#0#chr1:1830045-1840123`. The command uses the resolved `.pdx` for path
  metadata, `.lnx` for node lengths, and `.pcx` to start near the requested
  path step instead of scanning from step zero. If `.pcx` is absent, it uses
  the previous bounded path-prefix scan. If `.lnx` is absent, it falls back to
  scanning indexed GFA `S` lines; if validation fails, it falls back to ordinary
  subpath output and logs a warning.
- On-the-fly `.pdx` coordinate lookup
  handles `P` paths as path-local coordinates starting at 0 and handles `W`
  walks from their stored `SeqStart`. This fallback requires `.lnx`, because the
  path steps in `.pdx` need rank-aligned node lengths to derive cumulative
  coordinates.
- `--print_path_names`
  print the coordinate tracks available in the resolved `.cdx`, then exit. The
  output is TSV with columns `source`, `reference`, `haplotype`, `sequence`,
  `start`, `end`, and `entries`. `source` is `W` for walks, `P` for paths with
  path-local coordinates, or `S` for rGFA segment-derived tracks.
- `--no_header`
  omit the TSV header when used with `--print_path_names`

Example:

```bash
gfaidx get_region chr22.gfa.gz chr22:1500000-2000000 region.gfa \
  --reference CHM13 --max_nodes 100000

gfaidx get_region chr22.gfa.gz chr22:1500000-2000000 haplotypes.gfa \
  --reference CHM13 --all_haplotypes

gfaidx get_region chr22.gfa.gz --print_path_names
```

### `gfaidx get_chunk`

Stream one community member from the indexed gzip graph.

```bash
gfaidx get_chunk <in_gz> [--community_id <id> | --node_id <node>] [options]
```

Arguments:

- `in_gz`
  input indexed gzip graph produced by `index_gfa`

Options:

- `--idx <path>`
  path to the `.idx` file; defaults to `<in_gz>.idx`
- `--ndx <path>`
  path to the `.ndx` file; defaults to `<in_gz>.ndx`
- `--community_id <id>`
  stream this community directly
- `--node_id <node>`
  resolve the node through `.ndx` and stream its community

Notes:

- if `--node_id` is provided, it takes precedence over `--community_id`
- the legacy `--index` and `--node_index` spellings are still accepted
- shared edges are stored in a final extra member so streamed communities remain self-contained

Example:

```bash
gfaidx get_chunk graph.gfa.gz --node_id s12345
```

### `gfaidx index_paths`

Build or rebuild a standalone `.pdx` index for `P` and `W` lines.

```bash
gfaidx index_paths <in_gfa> <out_index.pdx> [options]
```

Arguments:

- `in_gfa`
  input GFA graph
- `out_index.pdx`
  output binary path index

Options:

- `--ndx <path>`
  node hash index produced by `index_gfa`; defaults to `<in_gfa>.ndx` when
  present. Provide this explicitly when the node index was renamed or stored
  elsewhere.
- `--tmp_dir <dir>`
  base directory for temporary files used by the external posting sort; defaults to the output directory
- `--progress_every <N>`
  progress logging interval while reading

Notes:

- `index_gfa` already builds `<out_gfa.gz>.pdx` by default
- `index_paths` is mainly useful when you want to rebuild only the path index after path-index code changes

What gets indexed:

- `P` lines
- `W` lines
- node names from `S` lines

Current behavior:

- `P` and `W` are both stored as ordered walks
- steps are stored in a packed 4-byte format
- per-node postings are compressed with delta + varint encoding
- large posting tables are built through disk-backed sorted runs to reduce peak RAM

Example:

```bash
gfaidx index_paths graph.indexed.gfa.gz graph.indexed.gfa.gz.pdx
```

### `gfaidx index_path_checkpoints`

Build the small `.pcx` acceleration sidecar for an existing `.pdx` and `.lnx`
without rebuilding the graph or path index.

```bash
gfaidx index_path_checkpoints <indexed_gfa> [out_index.pcx] [options]
```

The command infers `<indexed_gfa>.pdx`, `<indexed_gfa>.lnx`, and by default
writes `<indexed_gfa>.pcx`. Use `--pdx`, `--lnx`, or an explicit output path
when the files were renamed.

`--checkpoint_steps <N>` controls the checkpoint interval and defaults to
4096. Each checkpoint is one `uint64` cumulative path length, so the default
sidecar is normally much smaller than `.pdx`. Queries scan at most 4095 path
steps before the requested subpath instead of starting at path step zero.

Example:

```bash
gfaidx index_path_checkpoints graph.indexed.gfa.gz
```

### `gfaidx get_path`

Query an indexed graph's companion `.pdx` index in one of four modes:

1. list indexed `P` path names and `W` walk coordinate identifiers
2. full path lookup by canonical path ID
3. full `W` lookup by structured fields
4. node-set or subgraph lookup returning subpaths/subwalks

```bash
gfaidx get_path <in_gfa> [query mode options]
```

Arguments:

- `in_gfa`
  input indexed GFA graph; `get_path` looks for `<in_gfa>.pdx`

General options:

- `--ndx <path>`
  optional override for the node hash index; for node-set queries, `get_path`
  first tries `<in_gfa>.ndx`
- `--pdx <path>`
  optional override for the path index if the companion `.pdx` was renamed or
  stored elsewhere
- `--lnx <path>`
  optional override for the node-length index used by `--with_walk_coords`;
  defaults to `<in_gfa>.lnx` when present

#### Mode 1: print path names

```bash
gfaidx get_path graph.indexed.gfa.gz --print_path_names
```

Output is tab-separated:

- `P <path_name>` for original `P` records
- `W <sample> <hap_index> <seq_id> <seq_start|*> <seq_end|*>` for original
  `W` records

This output can be filtered and supplied to
`gfaidx index_coordinates --path_names_file` to coordinate-index selected paths
or walks.

#### Mode 2: exact path ID lookup

```bash
gfaidx get_path graph.indexed.gfa.gz --path_id <id>
```

Options:

- `--path_id <id>`
  canonical path ID

Notes:

- for `P` lines, this is the path name from the GFA
- for `W` lines, the canonical key is `sample|hap|seq_id|start|end`

Example:

```bash
gfaidx get_path graph.indexed.gfa.gz --path_id loopbackP
```

#### Mode 3: structured `W` lookup

```bash
gfaidx get_path graph.indexed.gfa.gz --sample <sample> --hap_index <hap> --seq_id <seq> [--seq_start <n|*>] [--seq_end <n|*>]
```

Options:

- `--sample <sample>`
  `W` sample ID
- `--hap_index <hap>`
  `W` haplotype index
- `--seq_id <seq>`
  `W` sequence ID
- `--seq_start <n|*>`
  optional exact start coordinate filter
- `--seq_end <n|*>`
  optional exact end coordinate filter

If multiple `W` lines match and start/end are omitted, the lookup is rejected as ambiguous.

Example:

```bash
gfaidx get_path graph.indexed.gfa.gz --sample HG002 --hap_index 1 --seq_id chr22
```

#### Mode 4: node-set or subgraph lookup

Return all `P`/`W` runs that remain contiguous inside the requested node set.

```bash
gfaidx get_path graph.indexed.gfa.gz --nodes 1,2,3
gfaidx get_path graph.indexed.gfa.gz --nodes_file nodes.txt
gfaidx get_path graph.indexed.gfa.gz --subgraph_gfa subgraph.gfa
```

Node query options:

- `--nodes <csv>`
  comma-separated node IDs
- `--nodes_file <path>`
  file with node IDs, one per line or comma-separated per line
- `--subgraph_gfa <path>`
  GFA whose `S` lines define the node set

Coordinate options for path output:

- `--with_walk_coords`
  try to emit exact `SeqStart`/`SeqEnd` for returned `W` subwalks and
  coordinate-bearing names for no-overlap `P` subpaths
- `--lnx <path>`
  node-length sidecar used to avoid scanning all `S` lines; defaults to
  `<in_gfa>.lnx` when present
- `--pcx <path>`
  optional path-coordinate checkpoints used to avoid scanning P/W paths from
  step zero; defaults to `<in_gfa>.pcx` when present
- `--source_gfa <path>`
  original source GFA used as a fallback length source when `.lnx` is absent

Coordinate rules:

- only applies to node-set queries
- affects `W` output coordinates and `P` output names
- `.lnx` stores rank-aligned `uint32` segment lengths; fallback scanning takes
  length from the sequence field first, then `LN:i:`
- if anything is missing or inconsistent, `get_path` falls back to ordinary
  subpath output and logs a warning

Example:

```bash
gfaidx get_path graph.indexed.gfa.gz \
  --subgraph_gfa extracted_subgraph.gfa \
  --with_walk_coords \
  --source_gfa graph.gfa
```

If the companion `.ndx` file was renamed or moved, provide it explicitly with `--ndx`.

### Build `.lnx` for existing indexes

Existing indexed graphs do not need to be fully re-indexed to get node lengths.
Build the sidecar directly from the indexed graph and matching `.ndx`:

```bash
python3 scripts/build_lnx.py graph.indexed.gfa.gz
```

Defaults:

- input graph: `graph.gfa.gz`
- node index: `graph.gfa.gz.ndx`
- output: `graph.gfa.gz.lnx`

Use `--ndx`, `--out`, or `--force` if the files were renamed or the output
should be replaced.

## How the path index works

`.pdx` stores two linked views of the same path data:

- path-first view
  each path points to its contiguous step range
- node-first view
  each node points to a compressed posting block containing all `(path_id, step_rank)` occurrences

This makes two workloads fast:

- full path reconstruction
- finding all subpaths that overlap a node set or extracted subgraph

Current query-time memory behavior:

- path metadata is loaded eagerly
- node metadata and node names are loaded lazily on demand
- node-name resolution for node-set queries goes through `.ndx`, not a giant in-memory node-name hash map

## Utility scripts

### `scripts/pdx_size_breakdown.py`

Print a size breakdown for a `.pdx` file.

```bash
python3 scripts/pdx_size_breakdown.py graph.pdx
```

### `scripts/w_to_p.awk`

Convert `W` lines to lossy `P` lines for testing and visualization. Non-`W` lines are passed through unchanged.

```bash
awk -f scripts/w_to_p.awk input.gfa > output.gfa
```

## Python helpers

Install with:

```bash
pip install -e .
```

The helper CLI is:

```bash
pygfaidx-bfs <graph.gfa.gz> <node_id> [size] [outgfa] [--no-shared-cache]
```

It loads community chunks on demand through `.idx` and `.ndx`.

The Python API exposes `ChGraph`:

```python
from pygfaidx.chgraph import ChGraph
```

## Notes

- `index_gfa` and `index_paths` are separate on purpose
- node-based `get_path` queries now depend on `.ndx`
- full-path `get_path --path_id ...` does not need `.ndx`
- `gfaidx` is Unix-only and is not intended to build or run on Windows
- current `.ndx` lookup relies on a 64-bit FNV-1a hash plus a 32-bit FNV-1a hash; collision handling is still probabilistic rather than string-verified

## TODO

- allow user-provided graph partitioning for `index_gfa`
- improve temporary-file behavior in chunk splitting
- add heavier tests on larger graphs
- consider a collision-proof node-name side table for very large graphs
- add unit tests
- add Rust interface
- add a conda package

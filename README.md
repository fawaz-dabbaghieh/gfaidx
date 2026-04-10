# gfaidx

`gfaidx` is a CLI toolkit for two related GFA indexing tasks:

- chunking a graph into community-based gzip members for fast subgraph streaming
- indexing `P` and `W` lines into a binary path index for fast full-path and subpath retrieval

The main CLI subcommands are:

- `index_gfa`
- `get_chunk`
- `index_paths`
- `get_path`

## What The Indexes Are

### Chunk index

`index_gfa` writes three artifacts:

- `<graph>.gz`
  the multi-member gzip file containing one member per community plus a final shared-edge member
- `<graph>.gz.idx`
  a community-to-gzip-offset index
- `<graph>.gz.ndx`
  a sorted binary hash table mapping node string IDs to community IDs

`get_chunk` uses `.idx` and `.ndx` to stream one community back out.

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

## Build

## Supported platforms

`gfaidx` targets Unix-like systems only:

- Linux
- macOS

It is not supported on Windows. Parts of the index I/O layer use Unix system
calls such as `open`, `fstat`, `mmap`, and `close`.

### C++ CLI

```bash
mkdir build
cd build
cmake ..
make -j 4
```

### Python helpers

```bash
pip install -e .
```

## CLI

### `gfaidx index_gfa`

Build the chunked gzip graph plus `.idx` and `.ndx`.

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

Outputs:

- `<out_gfa.gz>`
- `<out_gfa.gz>.idx`
- `<out_gfa.gz>.ndx`

Example:

```bash
gfaidx index_gfa graph.gfa graph.gfa.gz --tmp_dir /scratch/tmp
```

### `gfaidx get_chunk`

Stream one community from the indexed gzip graph.

```bash
gfaidx get_chunk <in_gz> [--community_id <id> | --node_id <node>] [options]
```

Arguments:

- `in_gz`
  input indexed gzip graph produced by `index_gfa`

Options:

- `--index <path>`
  path to the `.idx` file; defaults to `<in_gz>.idx`
- `--node_index <path>`
  path to the `.ndx` file; defaults to `<in_gz>.ndx`
- `--community_id <id>`
  stream this community directly
- `--node_id <node>`
  resolve the node through `.ndx` and stream its community

Notes:

- if `--node_id` is provided, it takes precedence over `--community_id`
- shared edges are stored in a final extra member so streamed communities remain self-contained

Example:

```bash
gfaidx get_chunk graph.gfa.gz --node_id s12345
```

### `gfaidx index_paths`

Build a `.pdx` index for `P` and `W` lines.

```bash
gfaidx index_paths <in_gfa> <out_index.pdx> --ndx <graph.ndx> [options]
```

Arguments:

- `in_gfa`
  input GFA graph
- `out_index.pdx`
  output binary path index

Options:

- `--ndx <path>`
  node hash index produced by `index_gfa`; required so `.pdx` node IDs match `.ndx` ranks
- `--progress_every <N>`
  progress logging interval while reading

What gets indexed:

- `P` lines
- `W` lines
- node names from `S` lines

Current behavior:

- `P` and `W` are both stored as ordered walks
- steps are stored in a packed 4-byte format
- per-node postings are compressed with delta + varint encoding

Example:

```bash
gfaidx index_paths graph.gfa graph.paths.pdx --ndx graph.gfa.gz.ndx
```

### `gfaidx get_path`

Query a `.pdx` index in one of three modes:

1. full path lookup by canonical path ID
2. full `W` lookup by structured fields
3. node-set or subgraph lookup returning subpaths/subwalks

```bash
gfaidx get_path <in_index.pdx> [query mode options]
```

Arguments:

- `in_index.pdx`
  input path index

General options:

- `--ndx <path>`
  required for node-set queries and exact `W` subwalk coordinates

#### Mode 1: exact path ID lookup

```bash
gfaidx get_path graph.pdx --path_id <id>
```

Options:

- `--path_id <id>`
  canonical path ID

Notes:

- for `P` lines, this is the path name from the GFA
- for `W` lines, the canonical key is `sample|hap|seq_id|start|end`

Example:

```bash
gfaidx get_path graph.pdx --path_id loopbackP
```

#### Mode 2: structured `W` lookup

```bash
gfaidx get_path graph.pdx --sample <sample> --hap_index <hap> --seq_id <seq> [--seq_start <n|*>] [--seq_end <n|*>]
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
gfaidx get_path graph.pdx --sample HG002 --hap_index 1 --seq_id chr22
```

#### Mode 3: node-set or subgraph lookup

Return all `P`/`W` runs that remain contiguous inside the requested node set.

```bash
gfaidx get_path graph.pdx --ndx graph.gfa.gz.ndx --nodes 1,2,3
gfaidx get_path graph.pdx --ndx graph.gfa.gz.ndx --nodes_file nodes.txt
gfaidx get_path graph.pdx --ndx graph.gfa.gz.ndx --subgraph_gfa subgraph.gfa
```

Node query options:

- `--nodes <csv>`
  comma-separated node IDs
- `--nodes_file <path>`
  file with node IDs, one per line or comma-separated per line
- `--subgraph_gfa <path>`
  GFA whose `S` lines define the node set

Coordinate options for `W` output:

- `--with_walk_coords`
  try to emit exact `SeqStart`/`SeqEnd` for returned `W` subwalks
- `--source_gfa <path>`
  original source GFA used to derive segment lengths

Coordinate rules:

- only applies to node-set queries
- only affects `W` output
- segment length is taken from the sequence field first, then `LN:i:`
- if anything is missing or inconsistent, `get_path` falls back to `* *` coordinates and logs a warning

Example:

```bash
gfaidx get_path graph.pdx \
  --ndx graph.gfa.gz.ndx \
  --subgraph_gfa extracted_subgraph.gfa \
  --with_walk_coords \
  --source_gfa graph.gfa
```

## How The Path Index Works

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

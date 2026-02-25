# gfaidx
`gfaidx` is a CLI toolkit for indexing GFA graphs into community-based chunks and
streaming those chunks efficiently. It builds a gzipped, community-partitioned
graph plus two indexes:
- `.idx` maps community IDs to gzip member offsets/sizes.
- `.ndx` maps node IDs to community IDs using a binary hash table.

The project includes a Python helper package (`pygfaidx`) and API for BFS traversal and
chunk loading using the same `.idx`/`.ndx` artifacts.

## Installation

### C++ CLI
```bash
mkdir -p build
cmake -S . -B build
cmake --build build
```

### Python helpers
```bash
pip install -e .
```

## CLI usage

### `gfaidx index_gfa`
```
gfaidx index_gfa <in.gfa> <out.gfa.gz> [options]
```
Options:
- `--tmp_dir <dir>`: base directory for temp files (default: input GFA directory)
- `--keep_tmp`: keep temp files and `latest` symlink that points to the latest temp directory
- `--progress_every <N>`: progress log interval (default: 1000000, 0 disables)
- `--gzip_level <1..9>`: gzip compression level (default: 6)
- `--gzip_mem_level <1..9>`: gzip mem level (default: 8)

Outputs:
- `<out.gfa.gz>`: chunked gzip file (one gzip member per community)
- `<out.gfa.gz>.idx`: community offsets index
- `<out.gfa.gz>.ndx`: node ID hash index

### `gfaidx get_chunk`
```
gfaidx get_chunk <out.gfa.gz> [--community_id <id> | --node_id <node>] [--index <path>] [--node_index <path>]
```
- `--community_id`: stream a specific community
- `--node_id`: resolve the node’s community via `.ndx` (takes precedence to `--community_id`)
- `--index`: path to `.idx` (default: `<out.gfa.gz>.idx`)
- `--node_index`: path to `.ndx` (default: `<out.gfa.gz>.ndx`)

Note: Edges that are shared between two communities live in their own chunk (the last one).
Therefore, chunks are self contained without any stray edges.

## Python helpers (`pygfaidx`)

Install with `pip install -e .` and use:

```bash
pygfaidx-bfs <graph.gfa.gz> <node_id> [size] [outgfa] [--no-shared-cache]
```

- Loads chunks on demand via `.idx`/`.ndx`
- Optionally disables shared-edge cache with `--no-shared-cache`
- Shared edges are stored in a final “shared” gzip member; by default
  `pygfaidx` loads them into memory once and uses them during traversal.

### Python API
The python package exposes a `ChGraph` class that can be used to load the indexed graph and traverse it.
This class can be imported as such `from pygfaidx.chgraph import ChGraph`, and the loading and unloading of chunks
while traversing the graph happens in the background, i.e., transparent from the user. Hence, the user
can traverse the graph as usual, and the chunks will be loaded on demand when needed in the background.


# TODO
- [x] Add fast buffer-based GFA reader, inspired by `strangepg`
- [x] Generate edge lists from a GFA
    - [x] Integrate `strangepg` file reading for faster GFA loading
- [x] On disk binary search for node IDs to their community ID
    - [x] It works but needs to be implemented in the code to store the community IDs.
- [x] separate the GFA file based on the communities produced.
    - [x] Change the map to a string: <int, int> and the second Int is the community ID, or keep a vector of node length
          and add to it <string, int> with node id and community ID. Need to test memory for both.
- [x] Generate the community ID to file offset index (int: <int, int>, community ID: <start, end>)
    - [x] Need to look if I can then gzip the chunks separately and how I will change the offsets.
- [x] Separate the edges that belong to different communities to their own chunk.
- [x] Parallelize the GFA chunking/gzipping. Not necessary for now, it's faster now with compression level 6 instead of 9.
- [x] Make my own binary graph generator based on the sorted edge list to give to Bgraph.
- [x] Add the header to the first chunk.
- [ ] Allow users to provide their own partitioning to index the GFA
- [ ] Add a way for the user to rezip the GFA in a multimember way after unzipping (I think adding the raw line numbers can solve this)
- [ ] Maybe make the community index a binary file that gets loaded into memory completely for faster access.
- [ ] Index the Paths and other lines, these will be line by line indexed, should be easy
- [ ] Recursive chunking, I think I should further chunk the communities that are too large. Do it on the separated file.
- [x] Add command line interface
- [ ] Add unit tests
- [ ] Add Rust interface
- [ ] Add conda package

# TODO Python
- [x] Add command line interface
- [x] Edit old ChGraph to work with the new indexes

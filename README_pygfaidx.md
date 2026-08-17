# pygfaidx

`pygfaidx` is the compiled Python interface to the gfaidx C++ query engine.
It opens existing gfaidx indexes, performs node, topology, path, and coordinate
queries, and returns ordinary mutable in-memory graph objects.

The API has the same two-part model as the C++ library:

- `pygfaidx.IndexedGraph` is an immutable, disk-backed indexed graph.
- `pygfaidx.Graph` is an independent, owned, mutable result.

Editing a returned `Graph` never changes the indexed graph or its sidecar
files. The package name remains `pygfaidx`, but this API replaces the earlier
pure-Python `ChGraph` proof of concept.

This guide covers [installation and a complete starter script](#install),
[index inspection](#open-and-inspect-an-index),
[node and subgraph queries](#extract-a-mutable-neighborhood),
[mutable graph operations](#inspect-and-mutate-a-graph),
[paths and regions](#work-with-indexed-paths), and
[streaming](#stream-large-results).

## Prepare an indexed graph

Use the gfaidx command-line tool to build the query-ready graph and sidecars:

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j 4

./build/gfaidx index_gfa graph.gfa graph.indexed.gfa.gz
```

This normally creates files such as:

```text
graph.indexed.gfa.gz
graph.indexed.gfa.gz.idx
graph.indexed.gfa.gz.ndx
graph.indexed.gfa.gz.lnx
graph.indexed.gfa.gz.pdx
graph.indexed.gfa.gz.pcx
```

A `.cdx` coordinate index is optional. See [README.md](README.md) for
coordinate indexing and all CLI options.

## Install

From a source checkout:

```bash
python -m pip install .
```

For an editable developer install:

```bash
python -m pip install -e .
```

Verify that the extension imports:

```bash
python -c "import pygfaidx; print(pygfaidx.IndexedGraph)"
```

The build uses scikit-build-core and pybind11. It requires Python 3.9 or newer,
a C++17 compiler, CMake, zlib, and pthread support. Linux and macOS are
supported.

## Five-minute example

This complete script extracts a neighborhood from an indexed graph:

```python
#!/usr/bin/env python3

import argparse
import sys

import pygfaidx


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Extract a mutable GFA neighborhood around one node"
    )
    parser.add_argument("graph", help="gfaidx-produced .gfa.gz")
    parser.add_argument("seed", help="node id at which traversal begins")
    parser.add_argument("output", help="ordinary GFA output path")
    parser.add_argument("--max-nodes", type=int, default=10_000)
    args = parser.parse_args()

    # Opening an IndexedGraph is read-only. Normal adjacent sidecars are found
    # automatically from the graph filename.
    index = pygfaidx.IndexedGraph(args.graph)

    if not index.node_exists(args.seed):
        print(f"node does not exist: {args.seed}", file=sys.stderr)
        return 1

    # The query returns an owned Graph, not a view into the disk index.
    result = index.get_subgraph(
        seeds=[args.seed],
        max_nodes=args.max_nodes,
        include_paths=index.capabilities.paths,
        include_coordinates=False,
        threads=2,
    )

    # Validation is non-mutating. Depending on the source GFA, a path can
    # legally contain consecutive steps without a corresponding L record.
    for issue in result.validate():
        print(f"{issue.code}: {issue.message}", file=sys.stderr)

    result.write_gfa(args.output)
    print(
        f"wrote {result.node_count} nodes and {result.edge_count} edges",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (RuntimeError, ValueError, IndexError) as error:
        print(f"gfaidx error: {error}", file=sys.stderr)
        raise SystemExit(1) from error
```

Run it against the index:

```bash
python extract_neighborhood.py \
    graph.indexed.gfa.gz \
    s123 \
    neighborhood.gfa \
    --max-nodes 5000
```

## Open and inspect an index

```python
import pygfaidx

index = pygfaidx.IndexedGraph("graph.indexed.gfa.gz")

print("graph:", index.graph_path)
print("nodes:", index.node_count)
print("communities:", index.community_count)

capabilities = index.capabilities
print("paths:", capabilities.paths)
print("node lengths:", capabilities.node_lengths)
print("path checkpoints:", capabilities.path_checkpoints)
print("coordinate index:", capabilities.coordinates)
```

The required `.idx` and `.ndx` sidecars support topology queries. Optional
capabilities are:

| Property | Sidecar | Used for |
| --- | --- | --- |
| `paths` | `.pdx` | P/W metadata, steps, and path-bearing extraction |
| `node_lengths` | `.lnx` | Fast path coordinates and haplotype gap limits |
| `path_checkpoints` | `.pcx` | Faster coordinate lookup along long paths |
| `coordinates` | `.cdx` | Indexed reference-coordinate tracks |

Check these flags before presenting optional path or coordinate functionality
in an application.

### Override sidecar locations

Adjacent conventional names are discovered automatically. Use `IndexPaths`
only when files were renamed or moved; fields left empty retain discovery:

```python
paths = pygfaidx.IndexPaths()
paths.idx = "/indexes/assembly.graph.idx"
paths.ndx = "/indexes/assembly.nodes.ndx"
paths.pdx = "/indexes/assembly.paths.pdx"
paths.cdx = "/indexes/assembly.coordinates.cdx"

index = pygfaidx.IndexedGraph(
    "/graphs/assembly.indexed.gfa.gz",
    index_paths=paths,
)

# index_paths contains the resolved explicit and automatically found paths.
print(index.index_paths.lnx)
```

The graph and all supplied sidecars must come from the same indexing bundle.
gfaidx rejects detectable node-count or rank-alignment mismatches.

## Look up nodes, links, and communities

```python
node_id = "s123"

if index.node_exists(node_id):
    node = index.get_node(node_id)
    print(node.id, node.sequence, node.tags)

    # neighbors() ignores endpoint orientation for convenient exploration.
    for neighbor_id in index.neighbors(node_id):
        print("neighbor:", neighbor_id)

    # incident_edges() retains orientations and parallel links.
    for edge in index.incident_edges(node_id):
        print(
            edge.id,
            edge.from_endpoint.node_id,
            "-" if edge.from_endpoint.reverse else "+",
            edge.to_endpoint.node_id,
            "-" if edge.to_endpoint.reverse else "+",
            edge.overlap,
        )

    # Communities are materialized as independent mutable Graph objects.
    community_id = index.community_id(node_id)
    community = index.get_community(community_id)
    print("community nodes:", community.node_count)
```

Use `incident_edges()` for orientation-aware algorithms or when distinct
parallel links matter. Use `node_exists()` before `get_node()` when a
missing id is an expected condition.

## Extract a mutable neighborhood

`get_subgraph()` performs an orientation-agnostic breadth-first traversal
from one or more seeds:

```python
graph = index.get_subgraph(
    # Multiple seeds are deduplicated and form one traversal frontier.
    seeds=["s123", "s456", "s123"],
    max_nodes=5_000,
    include_paths=True,
    include_coordinates=True,
    threads=4,
)

print(graph.node_count, graph.edge_count, graph.path_count)
for node in graph.nodes:
    print(node.id, node.sequence)
```

The options have the following meaning:

| Argument | Meaning |
| --- | --- |
| `max_nodes` | Maximum selected nodes. BFS stops at the limit; zero is invalid. |
| `include_paths` | Add matching P/W subpaths when a `.pdx` is available. |
| `include_coordinates` | Preserve coordinate-bearing W/P output when possible. Requires `include_paths=True`. |
| `threads` | Number of native path-formatting workers, from 1 through 256. |

If the unique seed count already exceeds `max_nodes`, the query raises
`RuntimeError`. Without a `.pdx`, an ordinary neighborhood query can still
return S/L topology when paths were requested.

## Inspect and mutate a Graph

The result owns its records and can be changed freely:

```python
# Add a node. Tags retain their ordinary GFA TAG:TYPE:VALUE spelling.
graph.add_node(
    "new_node",
    sequence="ACGT",
    tags=["LN:i:4", "XX:Z:example"],
)

# Construct a bidirected link. reverse=True corresponds to '-' in GFA.
edge = pygfaidx.Edge()
edge.from_endpoint = pygfaidx.Endpoint("s123", reverse=False)
edge.to_endpoint = pygfaidx.Endpoint("new_node", reverse=True)
edge.overlap = "0M"
edge.tags = ["ID:Z:new-link"]
edge_id = graph.add_edge(edge)

# Construct a P path. Every referenced node must already exist.
path = pygfaidx.Path()
path.record_type = "P"
path.name = "analysis_path"
path.steps = [
    pygfaidx.PathStep("s123", reverse=False),
    pygfaidx.PathStep("new_node", reverse=True),
]
path.overlaps = "*"
path_id = graph.add_path(path)

print("edge id:", edge_id, "path id:", path_id)
graph.write_gfa("edited.gfa")
```

### Update existing records

Objects returned by `nodes`, `edges`, `paths`, `get_node()`,
`get_edge()`, and `get_path()` are Python snapshots. Changing a snapshot
does not update the graph until it is passed back to the matching method.
Vector-valued fields such as `tags` and `steps` are lists returned by value;
assign a changed list back to its snapshot as shown below:

```python
node = graph.get_node("s123")
node.sequence = "AACCGG"
# Assign the changed list back; appending to node.tags alone changes a
# temporary Python list rather than the native Node.
node.tags = [*node.tags, "XX:Z:curated"]
graph.update_node(node)

edge = graph.get_edge(edge_id)
edge.overlap = "1M"
graph.update_edge(edge)

path = graph.get_path("analysis_path")
path.tags = [*path.tags, "DS:Z:derived path"]
graph.update_path(path)

# This convenience method checks all replacement node ids.
graph.replace_path_steps(
    "analysis_path",
    [
        pygfaidx.PathStep("new_node", reverse=False),
        pygfaidx.PathStep("s123", reverse=True),
    ],
)
```

This explicit update pattern lets the native graph keep its lookup indexes
consistent. A node is identified by its id. Edge and path snapshots carry
stable numeric ids used by their update methods.

### Remove and validate records

Removing a node automatically removes its incident edges, but fails while a P
or W path still references it:

```python
graph.remove_path("analysis_path")
graph.remove_node("new_node")

issues = graph.validate()
for issue in issues:
    print(issue.code, issue.message)
```

`validate()` reports missing edge endpoints, missing path nodes, and
consecutive path steps without a corresponding link. It does not modify the
graph.

## Read or construct an ordinary GFA

The mutable class can be used without `IndexedGraph`:

```python
# Read H/S/L/P/W records from an ordinary GFA.
graph = pygfaidx.Graph.read_gfa("input.gfa")

# Or build a small graph from scratch.
created = pygfaidx.Graph()
created.add_header("H\tVN:Z:1.0")
created.add_node("a", sequence="AC")
created.add_node("b", sequence="GT")

edge = pygfaidx.Edge()
edge.from_endpoint = pygfaidx.Endpoint("a")
edge.to_endpoint = pygfaidx.Endpoint("b")
edge.overlap = "0M"
created.add_edge(edge)

created.write_gfa("created.gfa")
```

Blank lines and comments are ignored while reading. Unsupported GFA record
types are rejected. Optional GFA fields are retained verbatim in each record's
`tags` list.

## Work with indexed paths

```python
for descriptor in index.paths():
    print(
        descriptor.record_type,
        descriptor.key,
        descriptor.step_count,
    )

descriptor = index.get_path("reference_path")
print(descriptor.name, descriptor.tags)

# Slice by zero-based path-step offset, not base-pair coordinate.
steps = index.path_steps(
    "reference_path",
    start_step=100,
    max_steps=25,
)
for step in steps:
    print(step.node_id, step.reverse)

# Materialize the nodes and internal links for the same step slice.
path_graph = index.get_path_subgraph(
    "reference_path",
    start_step=100,
    max_steps=25,
    max_nodes=10_000,
    include_coordinates=True,
)
path_graph.write_gfa("path-slice.gfa")
```

P paths use their GFA name as the key. W walks use
`sample|haplotype|sequence|start|end`. A `PathDescriptor` also exposes these
W fields individually, so application code does not need to parse the key.

## Query coordinate regions

List accelerated `.cdx` coordinate tracks:

```python
for track in index.coordinate_tracks():
    print(
        track.source_type,
        track.reference,
        track.sequence,
        track.haplotype,
        track.begin,
        track.end,
        track.entry_count,
    )
```

A region can also be resolved from indexed P/W coordinates when a matching
`.cdx` track is unavailable and the necessary `.pdx`/`.lnx` information is
present.

```python
region = index.get_region(
    reference="GRCh38",
    sequence="chr1",
    begin=1_000_000,
    end=1_100_000,
    mode=pygfaidx.RegionMode.ALL_HAPLOTYPES,
    haplotype_gap=5_000,
    max_nodes=100_000,
    include_paths=True,
    include_coordinates=True,
    threads=4,
)

region.write_gfa("region.gfa")
```

Coordinates are 0-based and half-open: `[begin, end)`. Region queries require
a `.pdx` to translate selected node ranks back to node names.

- `RegionMode.BFS` uses reference-overlapping nodes as traversal seeds and
  expands until `max_nodes`.
- `RegionMode.ALL_HAPLOTYPES` selects matching P/W runs containing the
  reference interval nodes instead of expanding by BFS.
- `haplotype_gap` limits how many unshared bases may bridge matches in
  all-haplotype mode and requires a `.lnx`.

### A reusable region-extraction function

```python
from pathlib import Path
from typing import Union

import pygfaidx


def extract_region(
    graph_path: Union[str, Path],
    reference: str,
    sequence: str,
    begin: int,
    end: int,
) -> pygfaidx.Graph:
    """Return an editable graph for one reference-coordinate interval."""
    index = pygfaidx.IndexedGraph(str(graph_path))

    if not index.capabilities.paths:
        raise RuntimeError("this index has no .pdx required for regions")

    return index.get_region(
        reference=reference,
        sequence=sequence,
        begin=begin,
        end=end,
        mode=pygfaidx.RegionMode.BFS,
        max_nodes=50_000,
        include_paths=True,
    )
```

## Stream large results

Streaming writes GFA records without constructing a mutable `Graph`. The
destination can be a text file or any object with a `write(str)` method:

```python
with open("neighborhood.gfa", "w", encoding="utf-8") as output:
    index.stream_subgraph(
        seeds=["s123"],
        destination=output,
        max_nodes=100_000,
        include_paths=True,
        include_coordinates=False,
        threads=4,
    )

with open("region.gfa", "w", encoding="utf-8") as output:
    index.stream_region(
        reference="GRCh38",
        sequence="chr1",
        begin=1_000_000,
        end=1_100_000,
        destination=output,
        mode=pygfaidx.RegionMode.BFS,
        max_nodes=100_000,
        include_paths=True,
    )
```

`io.StringIO` is useful for a small serialized result that another Python
library expects as text:

```python
import io

buffer = io.StringIO()
index.stream_subgraph(
    ["s123"],
    destination=buffer,
    max_nodes=100,
)
gfa_text = buffer.getvalue()
```

Native lookup, decompression, traversal, and formatting release the Python
GIL. Streaming reacquires it only while calling `destination.write()` for each
record. The Python streaming API always consumes the complete query unless the
destination raises an exception.

## Error handling

C++ exceptions are translated to ordinary Python exceptions:

- `ValueError` indicates invalid options or an inconsistent graph mutation.
- `IndexError` indicates a missing node, edge, path, community, step, or
  coordinate interval.
- `RuntimeError` indicates I/O failure, a missing required sidecar, an
  incompatible index, or a result exceeding a query limit.

```python
try:
    graph = index.get_path_subgraph(
        "missing_path",
        start_step=0,
        max_steps=100,
    )
except IndexError as error:
    print(f"path was not found: {error}")
```

At a command-line or service boundary, catching all three exception types and
reporting their message gives users the native gfaidx diagnostic.

## Ownership and concurrency

- Keep an `IndexedGraph` referenced while its query is running.
- Each materialized `Graph` owns its records and can outlive the index that
  produced it.
- Snapshot objects are independent Python values; commit edits with
  `update_node()`, `update_edge()`, or `update_path()`.
- Native query work releases the GIL. Const queries are safe, though some
  stream-backed work on one index is synchronized internally.
- Do not mutate one `Graph` simultaneously from multiple threads without
  application-level locking.
- Set `max_nodes` for user-controlled queries. Prefer streaming when only GFA
  output is needed.

## API map

| Goal | Python API |
| --- | --- |
| Open an index | `IndexedGraph(graph_path, index_paths=...)` |
| Test or fetch one indexed node | `node_exists`, `get_node` |
| Explore topology | `neighbors`, `incident_edges`, `get_community` |
| Extract a mutable BFS result | `get_subgraph` |
| List or slice paths | `paths`, `get_path`, `path_steps` |
| Extract a path interval | `get_path_subgraph` |
| List coordinate tracks | `coordinate_tracks` |
| Extract a reference interval | `get_region` |
| Stream serialized output | `stream_subgraph`, `stream_region` |
| Read or create a working graph | `Graph.read_gfa`, `Graph()` |
| Edit and check a working graph | `add_*`, `update_*`, `remove_*`, `validate` |
| Write a working graph | `Graph.write_gfa` |

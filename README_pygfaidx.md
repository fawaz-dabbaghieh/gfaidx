# pygfaidx

`pygfaidx` is the compiled Python interface to the gfaidx C++ query engine.
It opens existing gfaidx indexes, performs node/path/coordinate queries, and
returns ordinary mutable in-memory graph objects.

The package name remains `pygfaidx`, but this is a new API. The earlier
pure-Python `ChGraph` prototype is not retained.

## Install

From a source checkout:

```bash
python -m pip install .
```

For an editable developer install:

```bash
python -m pip install -e .
```

The build uses scikit-build-core and pybind11. A C++17 compiler, CMake, zlib,
and pthread support are required. Linux and macOS are supported.

## Open and inspect an index

```python
import pygfaidx

index = pygfaidx.IndexedGraph("graph.indexed.gfa.gz")

print(index.node_count)
print(index.community_count)
print(index.capabilities.paths)
print(index.capabilities.coordinates)

if index.node_exists("s123"):
    node = index.get_node("s123")
    print(node.id, node.sequence, node.tags)

    for neighbor_id in index.neighbors("s123"):
        print("neighbor:", neighbor_id)

    for edge in index.incident_edges("s123"):
        print(
            edge.from_endpoint.node_id,
            edge.from_endpoint.reverse,
            edge.to_endpoint.node_id,
            edge.to_endpoint.reverse,
            edge.overlap,
        )
```

`neighbors()` ignores orientation for convenient discovery.
`incident_edges()` provides complete oriented endpoints and preserves
parallel links.

## Extract, inspect, edit, and write

```python
graph = index.get_subgraph(
    seeds=["s123"],
    max_nodes=5_000,
    include_paths=True,
    include_coordinates=True,
)

print(graph.node_count, graph.edge_count, graph.path_count)
for node in graph.nodes:
    print(node.id, node.sequence)

# The extracted Graph is independent from the on-disk IndexedGraph.
graph.add_node("new_node", sequence="ACGT", tags=["XX:Z:example"])

edge = pygfaidx.Edge()
edge.from_endpoint = pygfaidx.Endpoint("s123", reverse=False)
edge.to_endpoint = pygfaidx.Endpoint("new_node", reverse=False)
edge.overlap = "0M"
edge_id = graph.add_edge(edge)

graph.write_gfa("edited.gfa")
```

Use `pygfaidx.Graph.read_gfa("input.gfa")` to load an ordinary GFA directly
into the same mutable representation.

Objects returned from `nodes`, `edges`, `paths`, and `get_node()` are
snapshots. Use `update_node()`, `update_edge()`, and `update_path()` to
commit changes so the graph can preserve its internal indexes.

Removing a node removes incident edges but fails while a path references it:

```python
graph.remove_path("unwanted_path")
graph.remove_node("s456")

for issue in graph.validate():
    print(issue.code, issue.message)
```

## Paths

```python
for descriptor in index.paths():
    print(descriptor.record_type, descriptor.key, descriptor.step_count)

descriptor = index.get_path("reference_path")
steps = index.path_steps("reference_path", start_step=100, max_steps=25)
for step in steps:
    print(step.node_id, step.reverse)

path_graph = index.get_path_subgraph(
    "reference_path",
    start_step=100,
    max_steps=25,
    max_nodes=10_000,
)
```

P paths use their GFA name as the key. W walks use the same composite identity
stored by gfaidx: `sample|haplotype|sequence|start|end`.

## Coordinate regions

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
)

region.write_gfa("region.gfa")
```

Coordinates are 0-based and half-open. Use `RegionMode.BFS` for graph
expansion around reference nodes, or `ALL_HAPLOTYPES` to preserve matching
indexed path intervals.

## Explicit streaming

Streaming writes records without constructing a mutable `Graph`:

```python
with open("neighborhood.gfa", "w") as output:
    index.stream_subgraph(
        ["s123"],
        destination=output,
        max_nodes=100_000,
        include_paths=True,
    )

with open("region.gfa", "w") as output:
    index.stream_region(
        reference="GRCh38",
        sequence="chr1",
        begin=1_000_000,
        end=1_100_000,
        destination=output,
        max_nodes=100_000,
    )
```

Native lookup, decompression, traversal, and formatting release the Python GIL.
The GIL is reacquired only while writing each record to the Python destination.

# gfaidx C++ library

The gfaidx library lets a C++ application query an indexed GFA without loading
the complete source graph into memory. An `IndexedGraph` is an immutable,
disk-backed index. Queries return an independent, owned `Graph` that can be
inspected, edited, validated, and written as ordinary GFA.

## Build and install

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j 4
cmake --install build --prefix /your/install/prefix
```

In a downstream CMake project:

```cmake
find_package(gfaidx CONFIG REQUIRED)

add_executable(my_graph_tool main.cpp)
target_link_libraries(my_graph_tool PRIVATE gfaidx::gfaidx)
```

The installed query library requires C++17, zlib, and threads. It currently
targets Linux and macOS. Louvain and the index-building implementation are not
part of the public query target.

## Open and inspect an indexed graph

```cpp
#include <gfaidx/gfaidx.hpp>

#include <fstream>
#include <iostream>

int main() {
    gfaidx::IndexedGraph index("graph.indexed.gfa.gz");

    std::cout << index.node_count() << " indexed nodes\n";
    if (index.node_exists("s123")) {
        const auto node = index.get_node("s123");
        std::cout << node.id << "\t" << node.sequence << "\n";

        for (const auto& neighbor : index.neighbors(node.id)) {
            std::cout << "neighbor: " << neighbor << "\n";
        }
        for (const auto& edge : index.incident_edges(node.id)) {
            std::cout << edge.from.node_id
                      << (edge.from.reverse ? '-' : '+') << " -> "
                      << edge.to.node_id
                      << (edge.to.reverse ? '-' : '+') << "\n";
        }
    }
}
```

`neighbors()` is orientation-agnostic. Use `incident_edges()` whenever an
algorithm needs the complete bidirected endpoint orientations or needs to
distinguish parallel links.

## Extract a mutable subgraph

```cpp
gfaidx::ExtractionOptions options;
options.max_nodes = 5000;
options.include_paths = true;
options.include_coordinates = true;

gfaidx::Graph graph = index.get_subgraph({"s123"}, options);

for (const auto& node : graph.nodes()) {
    std::cout << node.id << "\n";
}

// Mutations affect this owned extraction only, never the indexed graph.
graph.add_node({"new_node", "ACGT", {}});
graph.add_edge({
    0,
    {"s123", false},
    {"new_node", false},
    "0M",
    {},
});

graph.write_gfa("edited.gfa");
```

An existing ordinary GFA can also be loaded into the owned representation with
`gfaidx::Graph::from_gfa("input.gfa")`.

Node deletion automatically removes incident links, but it is rejected while a
P or W path still references that node. Remove or edit those paths first:

```cpp
graph.remove_path("path_name");
graph.remove_node("s456");

for (const auto& issue : graph.validate()) {
    std::cerr << issue.code << ": " << issue.message << "\n";
}
```

## Paths and coordinates

```cpp
for (const auto& path : index.paths()) {
    std::cout << path.key << "\t" << path.step_count << "\n";
}

const auto descriptor = index.get_path("reference_path");
const auto steps = index.path_steps("reference_path", 100, 25);
auto path_graph = index.get_path_subgraph("reference_path", 100, 25);

gfaidx::RegionOptions region_options;
region_options.max_nodes = 100000;
region_options.mode = gfaidx::RegionMode::all_haplotypes;
region_options.haplotype_gap = 5000;

auto region = index.get_region(
    "GRCh38", "chr1", 1'000'000, 1'100'000, region_options);
```

Coordinates are 0-based and half-open. `get_region()` uses a `.cdx` when
available and otherwise falls back to the indexed P/W paths and `.lnx`.

## Explicit streaming

Use streaming when the output should not become a mutable in-memory graph:

```cpp
std::ofstream output("region.gfa");
index.stream_region(
    "GRCh38", "chr1", 1'000'000, 1'100'000,
    [&](std::string_view line) {
        output << line << '\n';
        return static_cast<bool>(output);
    },
    region_options);
```

Returning `false` stops output. Selection state still scales with the selected
node count, but streamed records are not retained. A `QueryCallbacks` object
can additionally receive warnings and provide cooperative cancellation.

## Ownership and thread safety

- `IndexedGraph` never changes the graph or its sidecars.
- `Graph` owns every node, link, and path in an extraction.
- Accessor values remain owned by their graph. Mutations go through `Graph`
  methods so lookup indexes remain consistent.
- Concurrent const queries are safe. Stream-backed reader access is currently
  synchronized internally; mmap-only node lookups can proceed independently.
- Do not mutate the same `Graph` concurrently without external locking.

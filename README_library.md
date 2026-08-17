# gfaidx C++ library

The gfaidx C++ API lets an application query an indexed GFA without loading the
entire source graph into memory. It deliberately separates two kinds of graph:

- `gfaidx::IndexedGraph` is an immutable, disk-backed view of a gfaidx index.
  Use it for fast node, neighborhood, path, and coordinate queries.
- `gfaidx::Graph` is an independent, owned, mutable graph. Queries can return
  one, or an application can create or read one directly. It can be inspected,
  edited, validated, and written as ordinary GFA.

Changing a `Graph` never changes its source `IndexedGraph` or any on-disk
index. This makes the indexed graph a reusable data source while each analysis
owns its working result.

This guide covers [building and downstream CMake integration](#build-and-install-the-library),
[index inspection](#open-an-index-and-inspect-its-capabilities),
[node and subgraph queries](#extract-neighborhoods),
[mutable graph operations](#inspect-and-mutate-an-owned-graph),
[paths and regions](#work-with-indexed-paths), and
[streaming](#stream-large-results).

## Prepare an indexed graph

The library opens indexes created by the gfaidx CLI. From this repository:

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j 4

# This creates graph.indexed.gfa.gz and its normal companion sidecars.
./build/gfaidx index_gfa graph.gfa graph.indexed.gfa.gz
```

The required `.idx` and `.ndx` files and optional path/coordinate sidecars
normally sit beside the indexed graph and use its full name as their prefix,
for example `graph.indexed.gfa.gz.pdx`. `IndexedGraph` discovers these names
automatically.

See [README.md](README.md) for all indexing options and for building a
standalone coordinate index.

## Build and install the library

Install gfaidx to a prefix that the downstream project can find:

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j 4
cmake --install build --prefix /absolute/path/to/gfaidx-install
```

The default build follows CMake's `BUILD_SHARED_LIBS` setting. To request a
shared library explicitly:

```bash
cmake -S . -B build-shared \
    -DCMAKE_BUILD_TYPE=Release \
    -DBUILD_SHARED_LIBS=ON
cmake --build build-shared -j 4
cmake --install build-shared --prefix /absolute/path/to/gfaidx-install
```

The public library requires C++17, zlib, and threads and currently targets
Linux and macOS. The installed CMake package resolves zlib and threads for its
consumers. Louvain and the index-building implementation belong to the CLI and
are not part of the public query target.

## Integrate gfaidx into a new C++ project

A minimal application can use this layout:

```text
my-graph-tool/
├── CMakeLists.txt
└── src/
    └── main.cpp
```

Use the exported target `gfaidx::gfaidx` instead of adding include or library
paths manually:

```cmake
cmake_minimum_required(VERSION 3.20)
project(my_graph_tool VERSION 0.1.0 LANGUAGES CXX)

add_executable(my_graph_tool src/main.cpp)
target_compile_features(my_graph_tool PRIVATE cxx_std_17)

# The package provides headers, linker flags, zlib, and thread dependencies.
find_package(gfaidx CONFIG REQUIRED)
target_link_libraries(my_graph_tool PRIVATE gfaidx::gfaidx)
```

Here is a complete program that extracts a neighborhood around one node,
checks the mutable result, and writes it as GFA:

```cpp
#include <gfaidx/gfaidx.hpp>

#include <exception>
#include <iostream>
#include <string>

int main(int argc, char** argv) {
    if (argc != 4) {
        std::cerr
            << "usage: my_graph_tool <indexed.gfa.gz> <seed-node> <out.gfa>\n";
        return 2;
    }

    try {
        // Opening the index is read-only. Adjacent sidecars are discovered.
        gfaidx::IndexedGraph index(argv[1]);
        const std::string seed = argv[2];

        if (!index.node_exists(seed)) {
            std::cerr << "node does not exist: " << seed << '\n';
            return 1;
        }

        const auto capabilities = index.capabilities();
        std::cerr << "indexed nodes: " << index.node_count() << '\n'
                  << "path index: "
                  << (capabilities.paths ? "available" : "unavailable")
                  << '\n';

        gfaidx::ExtractionOptions options;
        options.max_nodes = 10'000;
        options.include_paths = capabilities.paths;
        options.include_coordinates = false;
        options.threads = 2;

        // The returned object owns its records and is safe to edit.
        gfaidx::Graph result = index.get_subgraph({seed}, options);

        // Validation reports structural issues without changing the graph.
        for (const auto& issue : result.validate()) {
            std::cerr << issue.code << ": " << issue.message << '\n';
        }

        result.write_gfa(argv[3]);
        std::cerr << "wrote " << result.node_count() << " nodes and "
                  << result.edge_count() << " edges\n";
    } catch (const std::exception& error) {
        // Invalid arguments, missing records, incompatible indexes, and I/O
        // failures are surfaced as standard C++ exceptions.
        std::cerr << "gfaidx error: " << error.what() << '\n';
        return 1;
    }

    return 0;
}
```

Configure the new project with the prefix used during installation:

```bash
cmake -S . -B build \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_PREFIX_PATH=/absolute/path/to/gfaidx-install
cmake --build build -j 4

./build/my_graph_tool \
    /data/graph.indexed.gfa.gz \
    s123 \
    neighborhood.gfa
```

If the prefix contains several packages or gfaidx is in a nonstandard library
directory, point directly to the directory containing
`gfaidxConfig.cmake`:

```bash
cmake -S . -B build \
    -Dgfaidx_DIR=/absolute/path/to/gfaidx-install/lib/cmake/gfaidx
```

## Open an index and inspect its capabilities

```cpp
#include <gfaidx/gfaidx.hpp>

#include <iostream>

gfaidx::IndexedGraph index("graph.indexed.gfa.gz");
const auto capabilities = index.capabilities();

std::cout << "nodes: " << index.node_count() << '\n';
std::cout << "communities: " << index.community_count() << '\n';
std::cout << "paths: " << capabilities.paths << '\n';
std::cout << "node lengths: " << capabilities.node_lengths << '\n';
std::cout << "path checkpoints: " << capabilities.path_checkpoints << '\n';
std::cout << "coordinate index: " << capabilities.coordinates << '\n';
```

The capabilities correspond to optional sidecars:

| Capability | Sidecar | Used for |
| --- | --- | --- |
| `paths` | `.pdx` | P/W metadata, steps, and path-bearing extraction |
| `node_lengths` | `.lnx` | Fast path coordinates and haplotype gap limits |
| `path_checkpoints` | `.pcx` | Faster coordinate lookup along long paths |
| `coordinates` | `.cdx` | Indexed reference-coordinate tracks |

The graph can still answer node and topology queries without the optional
sidecars. Check capabilities before exposing optional features in a service or
command-line tool.

### Override sidecar locations

Use `IndexPaths` when a sidecar was renamed or stored elsewhere. Empty fields
keep automatic discovery, so only override the exceptional files:

```cpp
gfaidx::IndexPaths paths;
paths.idx = "/indexes/assembly.graph.idx";
paths.ndx = "/indexes/assembly.nodes.ndx";
paths.pdx = "/indexes/assembly.paths.pdx";
paths.cdx = "/indexes/assembly.coordinates.cdx";

gfaidx::IndexedGraph index(
    "/graphs/assembly.indexed.gfa.gz",
    paths);

// These are the resolved paths, including automatically discovered fields.
std::cout << index.index_paths().lnx << '\n';
```

The graph and all supplied sidecars must originate from the same indexing
bundle. gfaidx rejects detectable count or alignment mismatches.

## Look up nodes, links, and communities

```cpp
const std::string node_id = "s123";

if (index.node_exists(node_id)) {
    // IndexedGraph returns this Node by value.
    const gfaidx::Node node = index.get_node(node_id);
    std::cout << node.id << '\t' << node.sequence << '\n';

    // This convenience view ignores endpoint orientation.
    for (const auto& neighbor_id : index.neighbors(node_id)) {
        std::cout << "neighbor: " << neighbor_id << '\n';
    }

    // Use incident_edges when orientation or parallel links matter.
    for (const auto& edge : index.incident_edges(node_id)) {
        std::cout << edge.from.node_id
                  << (edge.from.reverse ? '-' : '+') << " -> "
                  << edge.to.node_id
                  << (edge.to.reverse ? '-' : '+')
                  << " overlap=" << edge.overlap << '\n';
    }

    // A community is returned as another independent mutable Graph.
    const std::uint32_t community = index.community_id(node_id);
    gfaidx::Graph member = index.get_community(community);
    std::cout << "community nodes: " << member.node_count() << '\n';
}
```

Lookup methods throw `std::out_of_range` for missing nodes, paths, communities,
or coordinate intervals. Use `node_exists()` when absence is expected.

## Extract neighborhoods

`get_subgraph()` performs an orientation-agnostic breadth-first traversal
from one or more seed nodes:

```cpp
gfaidx::ExtractionOptions options;
options.max_nodes = 5'000;
options.include_paths = true;
options.include_coordinates = true;
options.threads = 4;

// Multiple seeds are deduplicated and form one traversal frontier.
gfaidx::Graph graph =
    index.get_subgraph({"sample_A", "sample_B", "sample_A"}, options);

for (const auto& node : graph.nodes()) {
    std::cout << node.id << '\n';
}
```

The extraction options have the following meaning:

| Option | Meaning |
| --- | --- |
| `max_nodes` | Maximum selected nodes. BFS stops at the limit; zero is invalid. |
| `include_paths` | Add matching P/W subpaths when a `.pdx` is available. |
| `include_coordinates` | Preserve coordinate-bearing W/P output when possible. Requires `include_paths`. |
| `threads` | Number of path-formatting workers, from 1 through 256. |

If the number of unique seeds already exceeds `max_nodes`, the query throws.
A missing optional path index produces topology-only S/L output and can be
reported through the warning callback described below.

## Inspect and mutate an owned Graph

All mutations are checked so that internal node, edge, and path indexes remain
consistent:

```cpp
// Copy an accessor value before changing it.
auto node = graph.get_node("s123");
node.sequence = "ACGT";
node.tags.push_back("XX:Z:curated");
graph.update_node(node);

// A zero edge id asks Graph to allocate a stable id.
gfaidx::Edge edge;
edge.from = {"s123", false};
edge.to = {"new_node", true};
edge.overlap = "0M";

graph.add_node({"new_node", "TGCA", {"LN:i:4"}});
const std::uint64_t edge_id = graph.add_edge(edge);

// Updates use the stable edge id returned by add_edge().
auto changed_edge = graph.get_edge(edge_id);
changed_edge.overlap = "1M";
graph.update_edge(changed_edge);

// Create a normal GFA P path through existing nodes.
gfaidx::Path path;
path.record_type = 'P';
path.name = "analysis_path";
path.steps = {{"s123", false}, {"new_node", true}};
path.overlaps = "*";
const std::uint64_t path_id = graph.add_path(path);

graph.replace_path_steps(
    "analysis_path",
    {{"new_node", false}, {"s123", true}});

std::cout << "new edge id: " << edge_id
          << ", new path id: " << path_id << '\n';
```

`nodes()`, `edges()`, and `paths()` return const references to the
container in C++. A later add/remove operation can invalidate references and
iterators, so copy an element before mutating the graph. `update_node()` finds
the node by its id; `update_edge()` and `update_path()` preserve their stable
numeric ids.

Removing a node automatically removes its incident links, but it is rejected
while a P or W path still references that node. Edit or remove those paths
first:

```cpp
graph.remove_path("analysis_path");
graph.remove_node("new_node");

for (const auto& issue : graph.validate()) {
    std::cerr << issue.code << ": " << issue.message << '\n';
}
```

`validate()` currently reports missing edge endpoints, missing path nodes, and
consecutive path steps without a corresponding link.

## Work with indexed paths

```cpp
for (const auto& descriptor : index.paths()) {
    std::cout << descriptor.record_type << '\t'
              << descriptor.key << '\t'
              << descriptor.step_count << '\n';
}

const auto descriptor = index.get_path("reference_path");

// Read 25 steps beginning at zero-based step 100.
const auto steps = index.path_steps("reference_path", 100, 25);
for (const auto& step : steps) {
    std::cout << step.node_id << (step.reverse ? '-' : '+') << '\n';
}

// Materialize just those path steps and links among their nodes.
gfaidx::ExtractionOptions path_options;
path_options.max_nodes = 10'000;
path_options.include_coordinates = true;
gfaidx::Graph path_graph =
    index.get_path_subgraph("reference_path", 100, 25, path_options);
```

P records use their GFA path name as the key. W records use the deterministic
composite key
`sample|haplotype|sequence|start|end`. The fields on `PathDescriptor` let an
application display this identity without parsing the key.

## Query coordinate regions

List available accelerated coordinate tracks before choosing a reference and
sequence:

```cpp
for (const auto& track : index.coordinate_tracks()) {
    std::cout << track.source_type << '\t'
              << track.reference << '\t'
              << track.sequence << '\t'
              << track.begin << '\t'
              << track.end << '\t'
              << track.entry_count << '\n';
}
```

`coordinate_tracks()` reports `.cdx` tracks. A coordinate query can also
fall back to indexed P/W paths when `.pdx` and the necessary length data are
available.

```cpp
gfaidx::RegionOptions options;
options.max_nodes = 100'000;
options.mode = gfaidx::RegionMode::all_haplotypes;
options.haplotype_gap = 5'000;
options.include_paths = true;
options.include_coordinates = true;
options.threads = 4;

gfaidx::Graph region = index.get_region(
    "GRCh38",
    "chr1",
    1'000'000,
    1'100'000,
    options);
```

Coordinates are 0-based and half-open: `[begin, end)`. Region queries require
a `.pdx` because node ranks must be translated back to names.

- `RegionMode::bfs` uses all reference-overlapping nodes as BFS seeds and
  expands until `max_nodes`.
- `RegionMode::all_haplotypes` avoids BFS and selects matching P/W runs that
  contain the reference interval nodes.
- `haplotype_gap` limits how many unshared bases may bridge matches in
  all-haplotype mode. It requires a `.lnx`.

## Stream large results

Use streaming when the result should be written or processed as GFA records
without retaining an owned `Graph`:

```cpp
#include <fstream>
#include <stdexcept>
#include <string_view>

std::ofstream output("region.gfa");
if (!output) {
    throw std::runtime_error("could not open region.gfa");
}

index.stream_region(
    "GRCh38",
    "chr1",
    1'000'000,
    1'100'000,
    [&](std::string_view line) {
        output << line << '\n';
        // Returning false stops record delivery without an exception.
        return static_cast<bool>(output);
    },
    options);
```

The `std::string_view` passed to a visitor is valid only for that callback.
Copy it if it must outlive the call. Selection state still scales with the
selected node count, but emitted records are not retained.

`stream_subgraph()` and `stream_path_subgraph()` use the same visitor model:

```cpp
std::size_t lines_seen = 0;

index.stream_subgraph(
    {"s123"},
    [&](std::string_view line) {
        consume_gfa_record(line);
        return ++lines_seen < 1'000;  // Stop after 1,000 records.
    },
    options);
```

### Warnings and cooperative cancellation

Library code does not print fallback warnings. Supply callbacks to connect them
to a logger, GUI, or job cancellation token:

```cpp
#include <atomic>
#include <iostream>

std::atomic_bool cancelled{false};

gfaidx::QueryCallbacks callbacks;
callbacks.warning = [](std::string_view message) {
    std::cerr << "gfaidx warning: " << message << '\n';
};
callbacks.keep_going = [&cancelled] {
    return !cancelled.load();
};

// Returning false from keep_going causes the query to throw a cancellation
// runtime_error. The caller decides how that maps to its job system.
gfaidx::Graph result =
    index.get_subgraph({"s123"}, options, callbacks);
```

The warning callback is useful when a query falls back from `.cdx` to a path
scan, cannot produce coordinate-bearing output, or omits paths because a
`.pdx` is unavailable.

## Read, construct, and write ordinary GFA

The mutable type is also usable independently of an index:

```cpp
#include <sstream>

// File overload.
gfaidx::Graph graph = gfaidx::Graph::from_gfa("input.gfa");

// Stream overload, useful for generated or in-memory GFA.
std::istringstream input(
    "H\tVN:Z:1.0\n"
    "S\ta\tAC\n"
    "S\tb\tGT\n"
    "L\ta\t+\tb\t+\t0M\n");
gfaidx::Graph generated = gfaidx::Graph::from_gfa(input);

generated.add_header("H\tTS:Z:processed");
generated.write_gfa(std::cout);
```

Blank lines and comment lines are ignored while reading. H, S, L, P, and W
records are supported; unsupported record types are rejected. Optional GFA
fields are retained verbatim in `Tags`.

For record-by-record output from an already materialized graph:

```cpp
graph.visit_gfa_lines([](std::string_view line) {
    send_to_pipeline(line);
    return true;
});
```

## Error handling

The API uses standard exceptions:

- `std::invalid_argument` for invalid options or inconsistent mutations.
- `std::out_of_range` for a requested node, edge, path, community, step, or
  region that does not exist.
- `std::runtime_error` for I/O errors, missing required sidecars, incompatible
  indexes, query limits, and cancellation.

Opening and querying can touch several files, so catch `std::exception` at an
application boundary and include `what()` in diagnostics.

## Ownership, lifetime, and concurrency

- Keep an `IndexedGraph` alive for as long as its queries are running.
- `IndexedGraph` is movable but not copyable and never changes its graph or
  sidecars.
- Every materialized `Graph` owns its nodes, links, paths, headers, and tags.
- Concurrent const queries on one `IndexedGraph` are safe. Some stream-backed
  reader work is synchronized internally.
- Separate `Graph` objects can be changed independently. Do not mutate one
  `Graph` concurrently without external locking.
- Choose `max_nodes` deliberately for servers and user-supplied queries.
  Prefer streaming when callers only need serialized output.

## API map

| Goal | API |
| --- | --- |
| Test or fetch one indexed node | `node_exists`, `get_node` |
| Explore topology | `neighbors`, `incident_edges`, `get_community` |
| Extract a mutable BFS result | `get_subgraph` |
| List or slice paths | `paths`, `get_path`, `path_steps` |
| Extract a path interval | `get_path_subgraph` |
| List coordinate tracks | `coordinate_tracks` |
| Extract a reference interval | `get_region` |
| Avoid materializing output | `stream_subgraph`, `stream_region`, `stream_path_subgraph` |
| Read or create a working graph | `Graph::from_gfa`, `Graph` |
| Edit and check a working graph | `add_*`, `update_*`, `remove_*`, `validate` |
| Serialize a working graph | `write_gfa`, `visit_gfa_lines` |

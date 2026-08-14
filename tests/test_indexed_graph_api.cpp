#include <gfaidx/gfaidx.hpp>

#include <cassert>
#include <sstream>
#include <string>

int main(int argc, char** argv) {
    assert(argc == 2);
    gfaidx::IndexedGraph index(argv[1]);
    assert(index.node_count() == 3);
    assert(index.node_exists("1"));
    assert(!index.node_exists("missing"));
    assert(index.get_node("1").sequence == "AA");
    assert(index.neighbors("1") == std::vector<std::string>{"2"});
    assert(index.paths().size() == 2);

    gfaidx::ExtractionOptions options;
    options.max_nodes = 3;
    options.include_paths = true;
    options.include_coordinates = true;
    const auto graph = index.get_subgraph({"1"}, options);
    assert(graph.node_count() == 3);
    assert(graph.edge_count() == 2);
    assert(graph.path_count() == 2);

    const auto path = index.get_path_subgraph("reference_path", 1, 2, options);
    assert(path.node_count() == 2);
    assert(path.edge_count() == 1);
    assert(path.path_count() == 1);

    std::ostringstream streamed;
    index.stream_subgraph({"1"}, [&](std::string_view line) {
        streamed << line << '\n';
        return true;
    }, options);
    std::ostringstream materialized;
    graph.write_gfa(materialized);
    assert(streamed.str() == materialized.str());
}

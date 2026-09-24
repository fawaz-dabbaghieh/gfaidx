#include <gfaidx/gfaidx.hpp>

#include <cassert>
#include <sstream>
#include <stdexcept>
#include <string>

int main() {
    // Exercise owned record storage, parallel/oriented edges, paths, safe
    // deletion, and deterministic round-tripping without an indexed fixture.
    gfaidx::Graph graph;
    graph.add_header("H\tVN:Z:1.1");
    graph.add_node({"a", "AC", {"LN:i:2"}});
    graph.add_node({"b", "G", {}});

    gfaidx::Edge first;
    first.from = {"a", false};
    first.to = {"b", true};
    first.overlap = "0M";
    const auto first_id = graph.add_edge(first);

    gfaidx::Edge parallel = first;
    parallel.tags = {"XX:Z:parallel"};
    const auto parallel_id = graph.add_edge(parallel);
    assert(first_id != parallel_id);
    assert(graph.neighbors("a") == std::vector<std::string>{"b"});
    assert(graph.incident_edges("a").size() == 2);

    gfaidx::Path path;
    path.name = "reference";
    path.steps = {{"a", false}, {"b", true}};
    const auto path_id = graph.add_path(path);
    assert(path_id != 0);

    bool rejected = false;
    try {
        graph.remove_node("b");
    } catch (const std::invalid_argument&) {
        rejected = true;
    }
    assert(rejected);
    graph.remove_path("reference");
    graph.remove_node("b");
    assert(graph.node_count() == 1);
    assert(graph.edge_count() == 0);
    assert(graph.validate().empty());

    std::ostringstream encoded;
    graph.write_gfa(encoded);
    std::istringstream input(encoded.str());
    const auto decoded = gfaidx::Graph::from_gfa(input);
    assert(decoded.node_count() == 1);
    assert(decoded.get_node("a").sequence == "AC");
    assert(decoded.headers().size() == 1);

    // Readers accept valid GFA records even when links precede their segments.
    std::istringstream unordered(
        "L\tx\t+\ty\t-\t0M\nS\ty\tT\nS\tx\tA\n");
    const auto reordered = gfaidx::Graph::from_gfa(unordered);
    assert(reordered.node_count() == 2);
    assert(reordered.edge_count() == 1);
}

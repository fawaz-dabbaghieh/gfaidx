#include <gfaidx/gfaidx.hpp>

int main() {
    gfaidx::Graph graph;
    graph.add_node({"consumer", "ACGT", {}});
    return graph.node_exists("consumer") ? 0 : 1;
}

#ifndef GFAIDX_GET_SUBGRAPH_COMMAND_H
#define GFAIDX_GET_SUBGRAPH_COMMAND_H

#include <cstdint>
#include <string>
#include <vector>

#include <argparse/argparse.hpp>

namespace gfaidx::chunk {

// Shared extraction options used by the node and coordinate query commands.
struct SubgraphExtractionOptions {
    std::string input_gz;
    std::string output_gfa;
    std::string idx_path;
    std::string ndx_path;
    std::string pdx_path;
    std::string lnx_path;
    std::uint32_t max_nodes{};
    bool include_paths{true};
    bool with_walk_coordinates{false};
    bool debug_trace{false};
};

// Configure the get_subgraph CLI for BFS neighborhood extraction from an
// indexed graph.
void configure_get_subgraph_parser(argparse::ArgumentParser& parser);

// Execute the get_subgraph command and write the extracted GFA subgraph to the
// requested output file, appending indexed P/W subpaths when a matching .pdx
// is available.
int run_get_subgraph(const argparse::ArgumentParser& program);

// Extract one graph neighborhood from one or more initial nodes. Coordinate
// queries use this multi-source entry point after resolving their node ranks.
int extract_subgraph_from_seeds(const SubgraphExtractionOptions& options,
                                const std::vector<std::string>& seed_nodes);

}  // namespace gfaidx::chunk

#endif  // GFAIDX_GET_SUBGRAPH_COMMAND_H

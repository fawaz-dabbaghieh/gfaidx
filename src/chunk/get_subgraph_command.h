#ifndef GFAIDX_GET_SUBGRAPH_COMMAND_H
#define GFAIDX_GET_SUBGRAPH_COMMAND_H

#include <argparse/argparse.hpp>

namespace gfaidx::chunk {

// Configure the get_subgraph CLI for BFS neighborhood extraction from an
// indexed graph.
void configure_get_subgraph_parser(argparse::ArgumentParser& parser);

// Execute the get_subgraph command and write the extracted GFA subgraph to the
// requested output file, appending indexed P/W subpaths when a matching .pdx
// is available.
int run_get_subgraph(const argparse::ArgumentParser& program);

}  // namespace gfaidx::chunk

#endif  // GFAIDX_GET_SUBGRAPH_COMMAND_H

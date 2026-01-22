#ifndef GFAIDX_DIRECT_BINARY_WRITER_H
#define GFAIDX_DIRECT_BINARY_WRITER_H

#include <cstdint>
#include <string>

void write_binary_graph_from_edgelist(const std::string& edge_list_path,
                                      const std::string& out_binary_path,
                                      std::uint32_t num_nodes);

#endif // GFAIDX_DIRECT_BINARY_WRITER_H

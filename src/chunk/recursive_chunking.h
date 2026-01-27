#ifndef GFAIDX_RECURSIVE_CHUNKING_H
#define GFAIDX_RECURSIVE_CHUNKING_H

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

#include "fs/Reader.h"

namespace gfaidx::chunk {


struct CommunityStats {
    std::uint64_t node_count{0};
    std::uint64_t seq_bp_total{0};
    std::uint64_t edge_count{0};
};

struct RecursiveChunkingConfig {
    bool enabled{false};
    std::uint64_t max_nodes{1000000};
    std::uint64_t max_seq_bp{500000000};
    std::uint64_t max_edges{5000000};
    std::uint64_t hard_max_nodes{5000000};
    std::uint64_t hard_max_seq_bp{3000000000};
};

// Refines id_to_comm with one recursive community-detection pass.
// Updates ncom to the new number of communities after refinement.
// Returns true when refinement occurred.
bool refine_id_to_comm_recursive(const std::string& input_gfa,
                                 const std::string& sorted_edgelist,
                                 const std::string& tmp_dir,
                                 const std::unordered_map<std::string, unsigned int>& node_id_map,
                                 const Reader::Options& reader_options,
                                 const RecursiveChunkingConfig& config,
                                 std::vector<std::uint32_t>& id_to_comm,
                                 std::uint32_t& ncom);

}  // namespace gfaidx::chunk

#endif  // GFAIDX_RECURSIVE_CHUNKING_H

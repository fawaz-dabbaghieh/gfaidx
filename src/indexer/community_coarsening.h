#ifndef GFAIDX_COMMUNITY_COARSENING_H
#define GFAIDX_COMMUNITY_COARSENING_H

#include <cstdint>
#include <string>
#include <vector>

namespace gfaidx::indexer {

// Report the effect of the undersized-community merge pass to callers and logs.
struct CommunityCoarseningSummary {
    std::uint32_t initial_community_count = 0;
    std::uint32_t final_community_count = 0;
    std::uint32_t merged_community_count = 0;
    std::uint32_t remaining_undersized_count = 0;
    std::uint64_t cross_community_edge_count = 0;
};

// Merge communities smaller than min_chunk_nodes into their strongest eligible
// neighboring community. A max_chunk_nodes value of zero disables the upper
// size bound. The function rewrites id_to_comm and compacts the resulting ids.
CommunityCoarseningSummary merge_undersized_communities(
    const std::string& sorted_edge_list_path,
    std::vector<std::uint32_t>& id_to_comm,
    std::uint32_t& community_count,
    std::uint32_t min_chunk_nodes,
    std::uint32_t max_chunk_nodes);

}  // namespace gfaidx::indexer

#endif  // GFAIDX_COMMUNITY_COARSENING_H

#ifndef GFAIDX_COMMUNITY_REFINEMENT_H
#define GFAIDX_COMMUNITY_REFINEMENT_H

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include <graph_binary.h>

namespace gfaidx::indexer {

// Carries the original global node ids for one oversized community that will be
// refined with one extra local Louvain pass.
struct CommunityRefinementWorkItem {
    std::uint32_t original_community_id = 0;
    std::vector<int> global_nodes;
};

// Summarizes how many communities were refined and what the final community
// count became after appending any new split-off community ids.
struct CommunityRefinementSummary {
    std::uint32_t final_community_count = 0;
    std::uint32_t refined_community_count = 0;
    std::uint32_t added_community_count = 0;
};

// Move the node lists for communities at or above the node threshold out of the
// Louvain result while skipping the optional singleton-only bucket that is
// appended later.
std::vector<CommunityRefinementWorkItem> collect_oversized_community_work(
    BGraph& final_graph,
    std::uint32_t max_chunk_nodes,
    std::optional<std::uint32_t> singleton_community_id);

// Refine the selected oversized communities one at a time by streaming the
// global sorted edge list, building a local subgraph, and remapping the final
// local Louvain communities back onto the global node ids.
CommunityRefinementSummary refine_oversized_communities(
    const std::string& sorted_edge_list_path,
    const std::string& tmp_dir,
    const std::vector<CommunityRefinementWorkItem>& work_items,
    std::vector<std::uint32_t>& id_to_comm,
    std::uint32_t initial_community_count,
    bool keep_tmp);

}  // namespace gfaidx::indexer

#endif  // GFAIDX_COMMUNITY_REFINEMENT_H

#include "indexer/community_refinement.h"

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include "fs/fs_helpers.h"
#include "indexer/direct_binary_writer.h"
#include "indexer/index_gfa_helpers.h"
#include "utils/Timer.h"

namespace fs = std::filesystem;

namespace gfaidx::indexer {

namespace {

// Build a compact global-id -> local-id map only for the community currently
// being refined, which keeps RAM proportional to one oversized chunk at a time.
struct LocalNodeMap {
    std::unordered_map<std::uint32_t, std::uint32_t> global_to_local;
    std::vector<std::uint32_t> local_to_global;
};

// Create the local numbering for one oversized community without allocating a
// dense array over every node in the full graph.
LocalNodeMap build_local_node_map(const std::vector<int>& global_nodes) {
    LocalNodeMap local_map;
    local_map.global_to_local.reserve(global_nodes.size());
    local_map.local_to_global.reserve(global_nodes.size());

    for (std::size_t local_id = 0; local_id < global_nodes.size(); ++local_id) {
        const int global_node = global_nodes[local_id];
        if (global_node < 0) {
            throw std::runtime_error("Negative global node id encountered during community refinement");
        }
        const auto global_id = static_cast<std::uint32_t>(global_node);
        local_map.global_to_local.emplace(global_id, static_cast<std::uint32_t>(local_id));
        local_map.local_to_global.push_back(global_id);
    }

    return local_map;
}

// Stream the sorted global edge list once and keep only the edges whose
// endpoints both fall inside the oversized community currently being refined.
std::uint64_t build_local_edge_list(const std::string& sorted_edge_list_path,
                                    const LocalNodeMap& local_map,
                                    const std::string& local_edge_list_path) {
    std::ifstream in(sorted_edge_list_path);
    if (!in) {
        throw std::runtime_error("Failed to open sorted edge list for refinement: " + sorted_edge_list_path);
    }

    std::ofstream out(local_edge_list_path);
    if (!out) {
        throw std::runtime_error("Failed to open local edge list for refinement: " + local_edge_list_path);
    }

    bool first_line = true;
    std::uint64_t kept_edges = 0;
    std::uint32_t src = 0;
    std::uint32_t dst = 0;
    while (in >> src >> dst) {
        const auto src_it = local_map.global_to_local.find(src);
        if (src_it == local_map.global_to_local.end()) {
            continue;
        }

        const auto dst_it = local_map.global_to_local.find(dst);
        if (dst_it == local_map.global_to_local.end()) {
            continue;
        }

        if (!first_line) {
            out << '\n';
        }
        out << src_it->second << ' ' << dst_it->second;
        first_line = false;
        kept_edges++;
    }

    out.close();
    if (!out) {
        throw std::runtime_error("Failed while writing local edge list: " + local_edge_list_path);
    }

    return kept_edges;
}

// Rewrite the refined local Louvain result back onto the global node ids while
// preserving the original community id for the first split piece and appending
// any additional pieces after the original global community range.
std::uint32_t apply_refined_partition(const CommunityRefinementWorkItem& work_item,
                                      const BGraph& refined_graph,
                                      const LocalNodeMap& local_map,
                                      std::vector<std::uint32_t>& id_to_comm,
                                      std::uint32_t& next_community_id) {
    std::uint32_t added_communities = 0;

    for (std::size_t local_comm_id = 0; local_comm_id < refined_graph.nodes.size(); ++local_comm_id) {
        const std::uint32_t final_comm_id =
            (local_comm_id == 0)
                ? work_item.original_community_id
                : next_community_id++;

        if (local_comm_id != 0) {
            added_communities++;
        }

        for (const int local_node : refined_graph.nodes[local_comm_id]) {
            if (local_node < 0) {
                throw std::runtime_error("Negative local node id encountered while applying refined partition");
            }
            const auto local_index = static_cast<std::size_t>(local_node);
            if (local_index >= local_map.local_to_global.size()) {
                throw std::runtime_error("Local node id out of range while applying refined partition");
            }

            const std::uint32_t global_id = local_map.local_to_global[local_index];
            if (global_id >= id_to_comm.size()) {
                throw std::runtime_error("Global node id out of range while applying refined partition");
            }
            id_to_comm[global_id] = final_comm_id;
        }
    }

    return added_communities;
}

// Remove per-community refinement temp files unless the caller explicitly wants
// to keep the run directory for debugging.
void cleanup_refinement_artifacts(const fs::path& edge_path,
                                  const fs::path& binary_path,
                                  bool keep_tmp) {
    if (keep_tmp) {
        return;
    }

    remove_path_if_exists(edge_path.string());
    remove_path_if_exists(binary_path.string());
}

}  // namespace

std::vector<CommunityRefinementWorkItem> collect_oversized_community_work(
    BGraph& final_graph,
    std::uint32_t max_chunk_nodes,
    std::optional<std::uint32_t> singleton_community_id) {
    std::vector<CommunityRefinementWorkItem> work_items;

    if (max_chunk_nodes == 0) {
        return work_items;
    }

    for (std::uint32_t comm_id = 0; comm_id < final_graph.nodes.size(); ++comm_id) {
        if (singleton_community_id.has_value() && comm_id == *singleton_community_id) {
            // Leave the special singleton-only bucket untouched even if it is large.
            continue;
        }

        if (final_graph.nodes[comm_id].size() < max_chunk_nodes) {
            continue;
        }

        CommunityRefinementWorkItem work_item;
        work_item.original_community_id = comm_id;
        // Move the oversized membership list out now so the full Louvain graph can be released sooner.
        work_item.global_nodes = std::move(final_graph.nodes[comm_id]);
        work_items.push_back(std::move(work_item));
    }

    return work_items;
}

CommunityRefinementSummary refine_oversized_communities(
    const std::string& sorted_edge_list_path,
    const std::string& tmp_dir,
    const std::vector<CommunityRefinementWorkItem>& work_items,
    std::vector<std::uint32_t>& id_to_comm,
    std::uint32_t initial_community_count,
    bool keep_tmp) {
    CommunityRefinementSummary summary;
    summary.final_community_count = initial_community_count;

    if (work_items.empty()) {
        return summary;
    }

    const fs::path refine_dir = fs::path(tmp_dir) / "community_refinement";
    // Group local refinement artifacts under a dedicated subdirectory so they are easy to inspect or remove.
    fs::create_directories(refine_dir);

    std::uint32_t next_community_id = initial_community_count;
    for (const auto& work_item : work_items) {
        Timer refine_timer;
        std::cout << get_time() << ": Refining community " << work_item.original_community_id
                  << " with " << work_item.global_nodes.size() << " nodes" << std::endl;

        const LocalNodeMap local_map = build_local_node_map(work_item.global_nodes);
        const fs::path local_edge_list_path =
            refine_dir / ("community_" + std::to_string(work_item.original_community_id) + ".edges.txt");
        const fs::path local_binary_path =
            refine_dir / ("community_" + std::to_string(work_item.original_community_id) + ".bin");

        const std::uint64_t local_edge_count = build_local_edge_list(sorted_edge_list_path,
                                                                     local_map,
                                                                     local_edge_list_path.string());

        // Reuse the existing direct binary writer so the local refinement path matches the global Louvain input path.
        write_binary_graph_from_edgelist(local_edge_list_path.string(),
                                         local_binary_path.string(),
                                         static_cast<std::uint32_t>(local_map.local_to_global.size()));

        BGraph refined_graph;
        // Reuse the same Louvain driver for the local subgraph so this stays a true one-extra-pass refinement.
        generate_communities(local_binary_path.string(), refined_graph, display_level);

        const std::uint32_t added_communities = apply_refined_partition(work_item,
                                                                        refined_graph,
                                                                        local_map,
                                                                        id_to_comm,
                                                                        next_community_id);

        summary.refined_community_count++;
        summary.added_community_count += added_communities;

        const auto largest_refined_piece = std::max_element(refined_graph.nodes.begin(),
                                                            refined_graph.nodes.end(),
                                                            [](const auto& left, const auto& right) {
                                                                return left.size() < right.size();
                                                            });
        const std::size_t largest_refined_size =
            (largest_refined_piece == refined_graph.nodes.end()) ? 0 : largest_refined_piece->size();

        std::cout << get_time() << ": Finished refining community " << work_item.original_community_id
                  << " into " << refined_graph.nodes.size() << " pieces using "
                  << local_edge_count << " local edges; largest refined piece has "
                  << largest_refined_size << " nodes in " << refine_timer.elapsed()
                  << " seconds" << std::endl;

        cleanup_refinement_artifacts(local_edge_list_path, local_binary_path, keep_tmp);
    }

    if (!keep_tmp) {
        std::error_code ec;
        // Remove the refinement subdirectory after the per-community artifacts have been deleted.
        fs::remove(refine_dir, ec);
    }

    summary.final_community_count = next_community_id;
    return summary;
}

}  // namespace gfaidx::indexer

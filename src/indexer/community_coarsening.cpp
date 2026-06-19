#include "indexer/community_coarsening.h"

#include <cstdint>
#include <fstream>
#include <iostream>
#include <limits>
#include <queue>
#include <stdexcept>
#include <unordered_map>
#include <utility>
#include <vector>

#include "utils/Timer.h"

namespace gfaidx::indexer {
namespace {

using NeighborWeights = std::unordered_map<std::uint32_t, std::uint64_t>;

// Keep merge direction explicit: an undersized source community is always
// attached to the selected target community rather than unioned by rank.
class DirectedCommunitySet {
public:
    explicit DirectedCommunitySet(std::uint32_t count) : parent_(count) {
        for (std::uint32_t id = 0; id < count; ++id) {
            parent_[id] = id;
        }
    }

    // Resolve a current community representative and compress the traversed
    // parent chain so later stale neighbor references remain inexpensive.
    std::uint32_t find(std::uint32_t id) {
        std::uint32_t root = id;
        while (parent_[root] != root) {
            root = parent_[root];
        }
        while (parent_[id] != id) {
            const std::uint32_t next = parent_[id];
            parent_[id] = root;
            id = next;
        }
        return root;
    }

    // Preserve the chosen target as the surviving representative so its
    // community size and adjacency state stay under one stable id.
    void merge_into(std::uint32_t source, std::uint32_t target) {
        parent_[source] = target;
    }

private:
    std::vector<std::uint32_t> parent_;
};

// Priority-queue entries carry the size observed when they were inserted.
// Entries become stale after a merge and are discarded when popped.
struct PendingCommunity {
    std::uint64_t node_count = 0;
    std::uint32_t community_id = 0;
};

// Process the smallest communities first and use the id only as a deterministic
// tie-breaker when two communities currently have the same node count.
struct PendingCommunityGreater {
    bool operator()(const PendingCommunity& left, const PendingCommunity& right) const {
        if (left.node_count != right.node_count) {
            return left.node_count > right.node_count;
        }
        return left.community_id > right.community_id;
    }
};

// Collapse neighbor ids through the current merge representatives and combine
// edge weights that now point at the same surviving community.
void collapse_neighbor_weights(std::uint32_t community_id,
                               DirectedCommunitySet& sets,
                               NeighborWeights& weights) {
    NeighborWeights collapsed;
    collapsed.reserve(weights.size());
    for (const auto& [neighbor_id, edge_count] : weights) {
        const std::uint32_t neighbor_root = sets.find(neighbor_id);
        if (neighbor_root == community_id) {
            continue;
        }
        collapsed[neighbor_root] += edge_count;
    }
    weights.swap(collapsed);
}

// Verify that a proposed merge respects the optional maximum without risking
// unsigned overflow when the two community sizes are added.
bool merge_fits_maximum(std::uint64_t left_size,
                        std::uint64_t right_size,
                        std::uint32_t max_chunk_nodes) {
    if (max_chunk_nodes == 0) {
        return true;
    }
    const std::uint64_t maximum = max_chunk_nodes;
    return left_size <= maximum && right_size <= maximum - left_size;
}

}  // namespace

CommunityCoarseningSummary merge_undersized_communities(
    const std::string& sorted_edge_list_path,
    std::vector<std::uint32_t>& id_to_comm,
    std::uint32_t& community_count,
    std::uint32_t min_chunk_nodes,
    std::uint32_t max_chunk_nodes) {
    CommunityCoarseningSummary summary;
    summary.initial_community_count = community_count;
    summary.final_community_count = community_count;

    // A zero minimum preserves the existing indexing behavior and avoids even
    // scanning the edge list when coarsening was not requested.
    if (min_chunk_nodes == 0 || community_count == 0 || id_to_comm.empty()) {
        std::cout << get_time()
                  << ": Small-community merging disabled because --min_chunk_nodes is 0"
                  << std::endl;
        return summary;
    }

    if (max_chunk_nodes != 0 && min_chunk_nodes > max_chunk_nodes) {
        std::cerr << "Warning: --min_chunk_nodes must not exceed a none-zero --max_chunk_nodes. Skipping this step";
        return summary;
    }

    Timer merge_timer;
    std::cout << get_time() << ": Starting small-community merging with minimum "
              << min_chunk_nodes << " nodes";
    if (max_chunk_nodes == 0) {
        std::cout << " and no maximum merged-community size";
    } else {
        std::cout << " and maximum " << max_chunk_nodes << " nodes";
    }
    std::cout << std::endl;

    // Count current membership directly from id_to_comm because the full
    // Louvain graph has deliberately been released before this stage.
    std::vector<std::uint64_t> community_sizes(community_count, 0);
    for (const std::uint32_t community_id : id_to_comm) {
        if (community_id >= community_count) {
            throw std::runtime_error("Community id out of range before small-community merging");
        }
        community_sizes[community_id]++;
    }

    // Store quotient-graph adjacency only for communities that initially need
    // merging. Any merged root that remains undersized can only be composed of
    // these communities, so no large-community adjacency map is required.
    std::unordered_map<std::uint32_t, NeighborWeights> small_adjacency;
    std::size_t initial_undersized_count = 0;
    for (std::uint32_t community_id = 0; community_id < community_count; ++community_id) {
        if (community_sizes[community_id] > 0 &&
            community_sizes[community_id] < min_chunk_nodes) {
            initial_undersized_count++;
        }
    }
    small_adjacency.reserve(initial_undersized_count);
    for (std::uint32_t community_id = 0; community_id < community_count; ++community_id) {
        if (community_sizes[community_id] > 0 &&
            community_sizes[community_id] < min_chunk_nodes) {
            small_adjacency.try_emplace(community_id);
        }
    }

    // Avoid an unnecessary full edge-list scan when Louvain and refinement have
    // already produced chunks that all satisfy the requested lower bound.
    if (initial_undersized_count == 0) {
        std::cout << get_time() << ": Finished small-community merging in "
                  << merge_timer.elapsed()
                  << " seconds; no communities were smaller than "
                  << min_chunk_nodes << " nodes" << std::endl;
        return summary;
    }

    // Build weighted community-to-community connections in one pass over the
    // existing numeric edge list. The weight is the number of graph edges that
    // would become internal if the two communities were merged.
    std::ifstream edge_list(sorted_edge_list_path);
    if (!edge_list) {
        throw std::runtime_error("Failed to open sorted edge list for small-community merging: " +
                                 sorted_edge_list_path);
    }

    std::uint32_t source_node = 0;
    std::uint32_t target_node = 0;
    while (edge_list >> source_node >> target_node) {
        if (source_node >= id_to_comm.size() || target_node >= id_to_comm.size()) {
            throw std::runtime_error("Node id out of range during small-community merging");
        }

        const std::uint32_t source_community = id_to_comm[source_node];
        const std::uint32_t target_community = id_to_comm[target_node];
        if (source_community == target_community) {
            continue;
        }

        summary.cross_community_edge_count++;
        const auto source_it = small_adjacency.find(source_community);
        if (source_it != small_adjacency.end()) {
            source_it->second[target_community]++;
        }
        const auto target_it = small_adjacency.find(target_community);
        if (target_it != small_adjacency.end()) {
            target_it->second[source_community]++;
        }
    }
    if (!edge_list.eof()) {
        throw std::runtime_error("Failed while reading sorted edge list for small-community merging");
    }

    std::cout << get_time() << ": Built small-community neighbor weights for "
              << initial_undersized_count << " undersized communities from "
              << summary.cross_community_edge_count << " cross-community edges"
              << std::endl;

    DirectedCommunitySet sets(community_count);
    std::priority_queue<PendingCommunity,
                        std::vector<PendingCommunity>,
                        PendingCommunityGreater> pending;

    // Seed the work queue with all non-empty communities below the requested
    // minimum; isolated communities remain present but will have no candidate.
    for (const auto& [community_id, unused_weights] : small_adjacency) {
        (void)unused_weights;
        pending.push(PendingCommunity{community_sizes[community_id], community_id});
    }

    while (!pending.empty()) {
        const PendingCommunity item = pending.top();
        pending.pop();

        // Skip representatives that were absorbed or queue entries whose size
        // predates a more recent merge into the same surviving community.
        const std::uint32_t source_root = sets.find(item.community_id);
        if (source_root != item.community_id ||
            community_sizes[source_root] != item.node_count ||
            community_sizes[source_root] == 0 ||
            community_sizes[source_root] >= min_chunk_nodes) {
            continue;
        }

        auto source_weights_it = small_adjacency.find(source_root);
        if (source_weights_it == small_adjacency.end()) {
            continue;
        }
        collapse_neighbor_weights(source_root, sets, source_weights_it->second);

        // Select the neighbor that internalizes the most cross-community edges.
        // Prefer the smallest combined result, then the lowest id, on ties.
        std::uint32_t best_target = std::numeric_limits<std::uint32_t>::max();
        std::uint64_t best_weight = 0;
        std::uint64_t best_combined_size = std::numeric_limits<std::uint64_t>::max();
        for (const auto& [neighbor_id, edge_count] : source_weights_it->second) {
            const std::uint32_t neighbor_root = sets.find(neighbor_id);
            if (neighbor_root == source_root || community_sizes[neighbor_root] == 0) {
                continue;
            }
            if (!merge_fits_maximum(community_sizes[source_root],
                                    community_sizes[neighbor_root],
                                    max_chunk_nodes)) {
                continue;
            }

            const std::uint64_t combined_size =
                community_sizes[source_root] + community_sizes[neighbor_root];
            if (edge_count > best_weight ||
                (edge_count == best_weight && combined_size < best_combined_size) ||
                (edge_count == best_weight && combined_size == best_combined_size &&
                 neighbor_root < best_target)) {
                best_target = neighbor_root;
                best_weight = edge_count;
                best_combined_size = combined_size;
            }
        }

        // A small community may remain when it is isolated or every neighboring
        // merge would exceed the configured maximum size.
        if (best_target == std::numeric_limits<std::uint32_t>::max()) {
            continue;
        }

        // Move the source adjacency out before touching the outer hash table;
        // inserting the target map could otherwise invalidate its iterator.
        NeighborWeights source_weights = std::move(source_weights_it->second);
        small_adjacency.erase(source_weights_it);

        sets.merge_into(source_root, best_target);
        community_sizes[best_target] += community_sizes[source_root];
        community_sizes[source_root] = 0;
        summary.merged_community_count++;

        if (community_sizes[best_target] < min_chunk_nodes) {
            // Merge maps small-to-large to keep repeated chains close to linear
            // in the number of stored quotient-graph relationships.
            NeighborWeights& target_weights = small_adjacency[best_target];
            if (target_weights.size() < source_weights.size()) {
                target_weights.swap(source_weights);
            }
            for (const auto& [neighbor_id, edge_count] : source_weights) {
                target_weights[neighbor_id] += edge_count;
            }
            pending.push(PendingCommunity{community_sizes[best_target], best_target});
        } else {
            // Once the target reaches the minimum it will never be a merge
            // source, so its potentially large adjacency can be released.
            small_adjacency.erase(best_target);
        }

        if (summary.merged_community_count % 10000 == 0) {
            std::cout << get_time() << ": Small-community merging has completed "
                      << summary.merged_community_count << " merges" << std::endl;
        }
    }

    // Assign dense ids in surviving-root order so the existing chunk writer
    // does not create empty files or .idx entries for removed community ids.
    std::vector<std::uint32_t> compact_id(community_count,
                                          std::numeric_limits<std::uint32_t>::max());
    std::uint32_t compact_count = 0;
    for (std::uint32_t community_id = 0; community_id < community_count; ++community_id) {
        const std::uint32_t root = sets.find(community_id);
        if (root == community_id && community_sizes[root] > 0) {
            compact_id[root] = compact_count++;
            if (community_sizes[root] < min_chunk_nodes) {
                summary.remaining_undersized_count++;
            }
        }
    }

    // Rewrite every node assignment through its final representative and the
    // dense id map consumed by split_gzip_gfa and the node hash index writer.
    for (std::uint32_t& community_id : id_to_comm) {
        const std::uint32_t root = sets.find(community_id);
        if (compact_id[root] == std::numeric_limits<std::uint32_t>::max()) {
            throw std::runtime_error("Missing compact community id after small-community merging");
        }
        community_id = compact_id[root];
    }

    community_count = compact_count;
    summary.final_community_count = compact_count;
    std::cout << get_time() << ": Finished small-community merging in "
              << merge_timer.elapsed() << " seconds; merged "
              << summary.merged_community_count << " communities, reduced the count from "
              << summary.initial_community_count << " to "
              << summary.final_community_count << ", and left "
              << summary.remaining_undersized_count
              << " undersized communities without an eligible merge"
              << std::endl;

    return summary;
}

}  // namespace gfaidx::indexer

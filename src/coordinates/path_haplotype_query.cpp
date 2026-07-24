#include "coordinates/path_haplotype_query.h"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <vector>

#include "paths/path_index.h"

namespace gfaidx::coordinates {
namespace {

// One compact entry per path avoids a hash table for posting aggregation. The
// path table is already loaded by PathIndexReader and is normally much smaller
// than the graph node and step tables.
struct PathStepBounds {
    std::uint32_t min_step{std::numeric_limits<std::uint32_t>::max()};
    std::uint32_t max_step{};
    bool seen{false};
};

}  // namespace

PathHaplotypeQueryResult query_path_haplotype_nodes(
    const paths::PathIndexReader& path_index,
    const std::vector<std::uint32_t>& reference_node_ranks) {

    if (reference_node_ranks.empty()) {
        throw std::runtime_error(
            "At least one reference node is required for all-haplotype extraction");
    }

    // De-duplicate coordinate hits before reading postings. A reference path
    // can revisit a node, while its node posting block only needs to be read
    // once for this query.
    std::vector<std::uint32_t> unique_reference_nodes = reference_node_ranks;
    std::sort(unique_reference_nodes.begin(), unique_reference_nodes.end());
    unique_reference_nodes.erase(
        std::unique(unique_reference_nodes.begin(), unique_reference_nodes.end()),
        unique_reference_nodes.end());

    std::vector<PathStepBounds> path_bounds(path_index.path_count());
    PathHaplotypeQueryResult result;
    result.reference_node_count = unique_reference_nodes.size();

    // The posting table is the node-to-path inverted index. Only postings for
    // nodes overlapping the coordinate interval are decompressed.
    for (const auto node_rank : unique_reference_nodes) {
        if (node_rank >= path_index.node_count()) {
            throw std::runtime_error(
                "Reference node rank is outside the .pdx node table");
        }

        path_index.for_each_node_posting(
            node_rank,
            [&](const std::uint32_t path_id, const std::uint32_t step_rank) {
                if (path_id >= path_bounds.size()) {
                    throw std::runtime_error(
                        "Path posting refers to a path outside the .pdx path table");
                }

                auto& bounds = path_bounds[path_id];
                if (!bounds.seen) {
                    bounds.min_step = step_rank;
                    bounds.max_step = step_rank;
                    bounds.seen = true;
                    ++result.matched_path_count;
                } else {
                    bounds.min_step = std::min(bounds.min_step, step_rank);
                    bounds.max_step = std::max(bounds.max_step, step_rank);
                }
                ++result.posting_count;
            });
    }

    // Keep every reference node even if a malformed or incomplete path index
    // has no posting for it. Under the all-nodes-covered assumption these will
    // already occur in at least one selected path slice.
    result.node_ranks = unique_reference_nodes;

    // Read only the min/max slice of each path that touched the reference
    // interval. Intermediate path nodes capture substitutions and insertions
    // without expanding unrelated graph branches through BFS.
    for (std::uint32_t path_id = 0; path_id < path_bounds.size(); ++path_id) {
        const auto& bounds = path_bounds[path_id];
        if (!bounds.seen) continue;

        const auto info = path_index.get_path_info(path_id);
        if (bounds.max_step >= info.step_count) {
            throw std::runtime_error(
                "Path posting step rank is outside its .pdx path step range");
        }

        const std::uint64_t step_count =
            static_cast<std::uint64_t>(bounds.max_step) - bounds.min_step + 1;
        if (step_count >
            std::numeric_limits<std::uint64_t>::max() -
                result.selected_path_step_count) {
            throw std::runtime_error(
                "All-haplotype selected path step count overflow");
        }
        result.selected_path_step_count += step_count;
        // Keep the path-specific interval alongside the node union. These
        // bounds are the original anchor-supported haplotype selection and are
        // later emitted directly as one P/W subpath per matched record.
        result.path_runs.push_back(
            paths::SubpathRun{path_id, bounds.min_step, step_count});

        path_index.for_each_step(
            path_id,
            bounds.min_step,
            step_count,
            [&](const paths::StepRecord& step, std::uint64_t) {
                result.node_ranks.push_back(step.node_id);
            });
    }

    // Different haplotypes commonly share nodes, so collapse the accumulated
    // path slices before chunk lookup and graph materialization.
    std::sort(result.node_ranks.begin(), result.node_ranks.end());
    result.node_ranks.erase(
        std::unique(result.node_ranks.begin(), result.node_ranks.end()),
        result.node_ranks.end());
    return result;
}

}  // namespace gfaidx::coordinates

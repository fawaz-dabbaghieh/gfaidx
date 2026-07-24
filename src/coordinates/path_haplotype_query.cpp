#include "coordinates/path_haplotype_query.h"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <utility>
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

std::vector<std::vector<paths::SubpathRun>> group_exact_reference_runs(
    const paths::PathIndexReader& path_index,
    const std::vector<paths::SubpathRun>& runs) {
    std::vector<std::vector<paths::SubpathRun>> grouped(path_index.path_count());
    for (const auto& run : runs) {
        if (run.path_id >= grouped.size()) {
            throw std::runtime_error(
                "Coordinate-selected reference run has an invalid path id");
        }
        const auto info = path_index.get_path_info(run.path_id);
        if (run.step_count == 0 ||
            run.start_step > info.step_count ||
            run.step_count > info.step_count - run.start_step) {
            throw std::runtime_error(
                "Coordinate-selected reference run is outside its .pdx path");
        }
        grouped[run.path_id].push_back(run);
    }

    // Coordinate tracks are normally disjoint. Merge adjacent slices anyway so
    // a query crossing compatible fragments emits one stable interval.
    for (auto& path_runs : grouped) {
        std::sort(path_runs.begin(), path_runs.end(),
                  [](const auto& lhs, const auto& rhs) {
                      return lhs.start_step < rhs.start_step;
                  });
        std::vector<paths::SubpathRun> merged;
        for (const auto& run : path_runs) {
            if (merged.empty()) {
                merged.push_back(run);
                continue;
            }
            auto& back = merged.back();
            const auto back_end = back.start_step + back.step_count;
            const auto run_end = run.start_step + run.step_count;
            if (run.start_step <= back_end) {
                back.step_count = std::max(back_end, run_end) - back.start_step;
            } else {
                merged.push_back(run);
            }
        }
        path_runs = std::move(merged);
    }
    return grouped;
}

}  // namespace

PathHaplotypeQueryResult query_path_haplotype_nodes(
    const paths::PathIndexReader& path_index,
    const std::vector<std::uint32_t>& reference_node_ranks,
    const std::vector<paths::SubpathRun>& exact_reference_path_runs) {

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
    auto exact_runs =
        group_exact_reference_runs(path_index, exact_reference_path_runs);
    PathHaplotypeQueryResult result;
    result.reference_node_count = unique_reference_nodes.size();

    // The posting table is the node-to-path inverted index. Each non-reference
    // path deliberately keeps its outermost anchor occurrences so insertions
    // and duplications between them remain part of the extracted haplotype.
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

    auto append_selected_run = [&](const paths::SubpathRun& run) {
        if (run.step_count >
            std::numeric_limits<std::uint64_t>::max() -
                result.selected_path_step_count) {
            throw std::runtime_error(
                "All-haplotype selected path step count overflow");
        }
        result.selected_path_step_count += run.step_count;
        result.path_runs.push_back(run);
        path_index.for_each_step(
            run.path_id,
            run.start_step,
            run.step_count,
            [&](const paths::StepRecord& step, std::uint64_t) {
                result.node_ranks.push_back(step.node_id);
            });
    };

    for (std::uint32_t path_id = 0; path_id < path_bounds.size(); ++path_id) {
        const auto& bounds = path_bounds[path_id];
        if (!bounds.seen) continue;

        const auto info = path_index.get_path_info(path_id);
        if (bounds.max_step >= info.step_count) {
            throw std::runtime_error(
                "Path posting step rank is outside its .pdx path step range");
        }

        // The queried P/W source is the sole exception to min/max aggregation:
        // its coordinate lookup already identified the exact repeated-node
        // occurrence, so using postings again would reintroduce the original
        // reference-widening bug.
        if (!exact_runs[path_id].empty()) {
            ++result.exact_reference_path_count;
            for (const auto& run : exact_runs[path_id]) {
                append_selected_run(run);
            }
            continue;
        }

        append_selected_run(paths::SubpathRun{
            path_id,
            bounds.min_step,
            static_cast<std::uint64_t>(bounds.max_step) - bounds.min_step + 1,
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

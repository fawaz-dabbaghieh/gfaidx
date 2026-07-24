#include "coordinates/path_haplotype_query.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <stdexcept>
#include <unordered_map>
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
    std::uint32_t distinct_anchor_count{};
    bool min_anchor_repeated{false};
    bool max_anchor_repeated{false};
    bool seen{false};
};

// One occurrence of any queried reference anchor on a candidate path.
struct TargetAnchorHit {
    std::uint32_t path_step{};
    std::uint32_t node_rank{};
};

// A compact predecessor node used to reconstruct the endpoints of one longest
// monotonic anchor chain without retaining path sequence strings.
struct ChainNode {
    std::uint32_t reference_order{};
    std::uint32_t path_step{};
    std::size_t previous{std::numeric_limits<std::size_t>::max()};
};

struct ChainSelection {
    std::uint32_t min_step{};
    std::uint32_t max_step{};
    std::uint32_t support{};
    bool reverse{false};
};

using ReferencePositions =
    std::unordered_map<std::uint32_t, std::vector<std::uint32_t>>;

ReferencePositions build_reference_positions(
    const std::vector<std::uint32_t>& ordered_reference_node_ranks) {
    if (ordered_reference_node_ranks.size() >
        static_cast<std::size_t>(std::numeric_limits<std::uint32_t>::max())) {
        throw std::runtime_error(
            "Reference interval has too many path steps for anchor chaining");
    }

    ReferencePositions positions;
    positions.reserve(ordered_reference_node_ranks.size());
    for (std::uint32_t order = 0;
         order < ordered_reference_node_ranks.size();
         ++order) {
        positions[ordered_reference_node_ranks[order]].push_back(order);
    }
    return positions;
}

template <typename PositionIterator>
void append_chain_candidates(
    const TargetAnchorHit& hit,
    PositionIterator begin,
    PositionIterator end,
    std::uint32_t reference_count,
    bool reverse,
    std::vector<ChainNode>& nodes,
    std::vector<std::size_t>& tails) {
    for (auto it = begin; it != end; ++it) {
        const auto reference_order =
            reverse ? reference_count - 1 - *it : *it;

        // lower_bound implements a strictly increasing subsequence. Candidate
        // orders for one target step are supplied in descending order, so two
        // reference occurrences cannot chain through the same path step.
        const auto tail_it = std::lower_bound(
            tails.begin(),
            tails.end(),
            reference_order,
            [&](const std::size_t node_index, const std::uint32_t value) {
                return nodes[node_index].reference_order < value;
            });
        const auto chain_length =
            static_cast<std::size_t>(tail_it - tails.begin());
        const auto previous = chain_length == 0
            ? std::numeric_limits<std::size_t>::max()
            : tails[chain_length - 1];

        nodes.push_back(ChainNode{reference_order, hit.path_step, previous});
        const auto node_index = nodes.size() - 1;
        if (tail_it == tails.end()) {
            tails.push_back(node_index);
        } else {
            *tail_it = node_index;
        }
    }
}

std::optional<ChainSelection> find_monotonic_chain(
    const std::vector<TargetAnchorHit>& hits,
    const ReferencePositions& reference_positions,
    std::uint32_t reference_count,
    bool reverse) {
    std::vector<ChainNode> nodes;
    std::vector<std::size_t> tails;
    nodes.reserve(hits.size());
    tails.reserve(reference_count);

    for (const auto& hit : hits) {
        const auto found = reference_positions.find(hit.node_rank);
        if (found == reference_positions.end()) continue;
        const auto& positions = found->second;

        if (reverse) {
            // Ascending original positions become descending transformed
            // positions, which prevents one target step from extending itself.
            append_chain_candidates(hit,
                                    positions.begin(),
                                    positions.end(),
                                    reference_count,
                                    true,
                                    nodes,
                                    tails);
        } else {
            append_chain_candidates(hit,
                                    positions.rbegin(),
                                    positions.rend(),
                                    reference_count,
                                    false,
                                    nodes,
                                    tails);
        }
    }

    if (tails.empty()) return std::nullopt;

    const auto support = static_cast<std::uint32_t>(tails.size());
    auto node_index = tails.back();
    const auto max_step = nodes[node_index].path_step;
    auto min_step = max_step;
    while (node_index != std::numeric_limits<std::size_t>::max()) {
        min_step = nodes[node_index].path_step;
        node_index = nodes[node_index].previous;
    }
    return ChainSelection{min_step, max_step, support, reverse};
}

bool chain_is_better(const ChainSelection& candidate,
                     const ChainSelection& current) {
    if (candidate.support != current.support) {
        return candidate.support > current.support;
    }
    const auto candidate_span =
        static_cast<std::uint64_t>(candidate.max_step) - candidate.min_step;
    const auto current_span =
        static_cast<std::uint64_t>(current.max_step) - current.min_step;
    if (candidate_span != current_span) return candidate_span < current_span;
    if (candidate.min_step != current.min_step) {
        return candidate.min_step < current.min_step;
    }
    // Prefer forward order only as a final deterministic tie-break.
    return !candidate.reverse && current.reverse;
}

std::optional<ChainSelection> select_coherent_anchor_interval(
    const paths::PathIndexReader& path_index,
    std::uint32_t path_id,
    const PathStepBounds& global_bounds,
    const ReferencePositions& reference_positions,
    std::uint32_t reference_count) {
    if (global_bounds.distinct_anchor_count < 2) {
        // A repeated single anchor has no ordering information. Keeping its
        // complete posting span is safer than choosing one arbitrary copy.
        return std::nullopt;
    }

    std::vector<TargetAnchorHit> hits;
    const auto global_step_count =
        static_cast<std::uint64_t>(global_bounds.max_step) -
        global_bounds.min_step + 1;
    path_index.for_each_step(
        path_id,
        global_bounds.min_step,
        global_step_count,
        [&](const paths::StepRecord& step, const std::uint64_t step_rank) {
            if (reference_positions.find(step.node_id) ==
                reference_positions.end()) {
                return;
            }
            if (step_rank > std::numeric_limits<std::uint32_t>::max()) {
                throw std::runtime_error(
                    "Path step rank exceeds the posting index range");
            }
            hits.push_back(TargetAnchorHit{
                static_cast<std::uint32_t>(step_rank),
                step.node_id,
            });
        });

    const auto forward = find_monotonic_chain(
        hits, reference_positions, reference_count, false);
    const auto reverse = find_monotonic_chain(
        hits, reference_positions, reference_count, true);
    if (!forward) return reverse;
    if (!reverse) return forward;
    return chain_is_better(*reverse, *forward) ? reverse : forward;
}

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
    const std::vector<std::uint32_t>& ordered_reference_node_ranks,
    const std::vector<paths::SubpathRun>& exact_reference_path_runs) {

    if (ordered_reference_node_ranks.empty()) {
        throw std::runtime_error(
            "At least one reference node is required for all-haplotype extraction");
    }

    // De-duplicate coordinate hits before reading postings. A reference path
    // can revisit a node, while its node posting block only needs to be read
    // once for this query.
    std::vector<std::uint32_t> unique_reference_nodes =
        ordered_reference_node_ranks;
    std::sort(unique_reference_nodes.begin(), unique_reference_nodes.end());
    unique_reference_nodes.erase(
        std::unique(unique_reference_nodes.begin(), unique_reference_nodes.end()),
        unique_reference_nodes.end());

    std::vector<PathStepBounds> path_bounds(path_index.path_count());
    auto exact_runs =
        group_exact_reference_runs(path_index, exact_reference_path_runs);
    PathHaplotypeQueryResult result;
    result.reference_node_count = unique_reference_nodes.size();

    // The posting table is the node-to-path inverted index. Only postings for
    // nodes overlapping the coordinate interval are decompressed.
    for (const auto node_rank : unique_reference_nodes) {
        if (node_rank >= path_index.node_count()) {
            throw std::runtime_error(
                "Reference node rank is outside the .pdx node table");
        }

        auto current_path_id = std::numeric_limits<std::uint32_t>::max();
        auto group_min_step = std::numeric_limits<std::uint32_t>::max();
        std::uint32_t group_max_step = 0;
        std::uint64_t group_posting_count = 0;

        auto flush_path_group = [&]() {
            if (current_path_id == std::numeric_limits<std::uint32_t>::max()) {
                return;
            }
            auto& bounds = path_bounds[current_path_id];
            const bool repeated = group_posting_count > 1;
            ++bounds.distinct_anchor_count;
            if (!bounds.seen) {
                bounds.min_step = group_min_step;
                bounds.max_step = group_max_step;
                bounds.min_anchor_repeated = repeated;
                bounds.max_anchor_repeated = repeated;
                bounds.seen = true;
                ++result.matched_path_count;
            } else {
                if (group_min_step < bounds.min_step) {
                    bounds.min_step = group_min_step;
                    bounds.min_anchor_repeated = repeated;
                }
                if (group_max_step > bounds.max_step) {
                    bounds.max_step = group_max_step;
                    bounds.max_anchor_repeated = repeated;
                }
            }
        };

        path_index.for_each_node_posting(
            node_rank,
            [&](const std::uint32_t path_id, const std::uint32_t step_rank) {
                if (path_id >= path_bounds.size()) {
                    throw std::runtime_error(
                        "Path posting refers to a path outside the .pdx path table");
                }

                // Posting blocks are grouped by path id. Aggregate all copies
                // of this one anchor before deciding whether it made either
                // global endpoint ambiguous.
                if (path_id != current_path_id) {
                    flush_path_group();
                    current_path_id = path_id;
                    group_min_step = step_rank;
                    group_max_step = step_rank;
                    group_posting_count = 1;
                } else {
                    group_min_step = std::min(group_min_step, step_rank);
                    group_max_step = std::max(group_max_step, step_rank);
                    ++group_posting_count;
                }
                ++result.posting_count;
            });
        flush_path_group();
    }

    // Keep every reference node even if a malformed or incomplete path index
    // has no posting for it. Under the all-nodes-covered assumption these will
    // already occur in at least one selected path slice.
    result.node_ranks = unique_reference_nodes;

    bool needs_chaining = false;
    for (std::size_t path_id = 0; path_id < path_bounds.size(); ++path_id) {
        if (path_bounds[path_id].seen &&
            (path_bounds[path_id].min_anchor_repeated ||
             path_bounds[path_id].max_anchor_repeated) &&
            exact_runs[path_id].empty()) {
            needs_chaining = true;
            break;
        }
    }
    std::optional<ReferencePositions> reference_positions;
    if (needs_chaining) {
        reference_positions =
            build_reference_positions(ordered_reference_node_ranks);
    }
    if (ordered_reference_node_ranks.size() >
        static_cast<std::size_t>(std::numeric_limits<std::uint32_t>::max())) {
        throw std::runtime_error(
            "Reference interval has too many anchors for path selection");
    }
    const auto reference_count =
        static_cast<std::uint32_t>(ordered_reference_node_ranks.size());

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

    // Exact source runs come directly from coordinate binary searches. Other
    // paths retain the fast min/max selection unless duplicated anchors make
    // those endpoints ambiguous, in which case ordered chaining is used.
    for (std::uint32_t path_id = 0; path_id < path_bounds.size(); ++path_id) {
        const auto& bounds = path_bounds[path_id];
        if (!bounds.seen) continue;

        const auto info = path_index.get_path_info(path_id);
        if (bounds.max_step >= info.step_count) {
            throw std::runtime_error(
                "Path posting step rank is outside its .pdx path step range");
        }

        if (!exact_runs[path_id].empty()) {
            ++result.exact_reference_path_count;
            for (const auto& run : exact_runs[path_id]) {
                append_selected_run(run);
            }
            continue;
        }

        auto selected_min = bounds.min_step;
        auto selected_max = bounds.max_step;
        if (bounds.min_anchor_repeated || bounds.max_anchor_repeated) {
            const auto chained = select_coherent_anchor_interval(
                path_index,
                path_id,
                bounds,
                *reference_positions,
                reference_count);
            if (chained && chained->support >= 2) {
                selected_min = chained->min_step;
                selected_max = chained->max_step;
                ++result.repeat_chained_path_count;
            } else {
                ++result.repeat_fallback_path_count;
            }
        }

        append_selected_run(paths::SubpathRun{
            path_id,
            selected_min,
            static_cast<std::uint64_t>(selected_max) - selected_min + 1,
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

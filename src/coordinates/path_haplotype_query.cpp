#include "coordinates/path_haplotype_query.h"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <utility>
#include <vector>

#include "paths/path_index.h"
#include "utils/Timer.h"

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
    const std::vector<paths::SubpathRun>& exact_reference_path_runs,
    const PathHaplotypeQueryOptions& options) {

    if (reference_node_ranks.empty()) {
        throw std::runtime_error(
            "At least one reference node is required for all-haplotype extraction");
    }

    const bool local_gap_mode = options.max_gap_bases.has_value();
    if (local_gap_mode && options.node_lengths == nullptr) {
        throw std::runtime_error(
            "Local haplotype gap selection requires a node length index");
    }
    if (local_gap_mode &&
        options.node_lengths->node_count() != path_index.node_count()) {
        throw std::runtime_error(
            ".lnx node count does not match .pdx for local haplotype selection");
    }

    // De-duplicate coordinate hits before reading postings. A reference path
    // can revisit a node, while its posting block is needed only once.
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

    // A dense bitset bounds temporary rank memory by the graph node count.
    // Every selected path run sets bits here, and the final ascending scan
    // produces the sorted unique rank vector required by materialization.
    std::vector<std::uint64_t> selected_node_bits(
        (static_cast<std::size_t>(path_index.node_count()) + 63) / 64,
        0);
    std::uint64_t selected_node_count = 0;
    auto select_node_rank = [&](const std::uint32_t node_rank) {
        if (node_rank >= path_index.node_count()) {
            throw std::runtime_error(
                "Selected path step has a node rank outside the .pdx node table");
        }
        auto& word = selected_node_bits[node_rank / 64];
        const std::uint64_t mask = 1ULL << (node_rank % 64);
        if ((word & mask) == 0) {
            word |= mask;
            ++selected_node_count;
        }
    };
    for (const auto node_rank : unique_reference_nodes) {
        select_node_rank(node_rank);
    }

    // Local mode marks anchor occurrences by absolute packed-step rank. This
    // avoids retaining and sorting one uint32 posting for every anchor hit.
    std::vector<std::uint64_t> anchor_step_bits;
    std::vector<std::uint64_t> path_step_begins;
    std::vector<std::uint64_t> path_step_counts;
    if (local_gap_mode) {
        const auto total_steps = path_index.total_step_count();
        const auto anchor_word_count =
            total_steps / 64 + (total_steps % 64 != 0 ? 1 : 0);
        if (anchor_word_count > anchor_step_bits.max_size()) {
            throw std::runtime_error(
                "Path step table is too large for local anchor selection");
        }
        anchor_step_bits.assign(
            static_cast<std::size_t>(anchor_word_count), 0);
        path_step_begins.resize(path_index.path_count());
        path_step_counts.resize(path_index.path_count());
        for (std::uint32_t path_id = 0;
             path_id < path_index.path_count();
             ++path_id) {
            const auto info = path_index.get_path_info(path_id);
            if (info.step_begin > total_steps ||
                info.step_count > total_steps - info.step_begin) {
                throw std::runtime_error(
                    "Path metadata is outside the packed .pdx step table");
            }
            path_step_begins[path_id] = info.step_begin;
            path_step_counts[path_id] = info.step_count;
        }
    }

    const auto mark_anchor_step = [&](const std::uint32_t path_id,
                                      const std::uint32_t step_rank) {
        if (!local_gap_mode) return;
        if (step_rank >= path_step_counts[path_id]) {
            throw std::runtime_error(
                "Path posting step rank is outside its .pdx path step range");
        }
        const auto absolute_step = path_step_begins[path_id] + step_rank;
        anchor_step_bits[static_cast<std::size_t>(absolute_step / 64)] |=
            1ULL << (absolute_step % 64);
    };
    const auto is_anchor_step = [&](const std::uint32_t path_id,
                                    const std::uint64_t step_rank) {
        const auto absolute_step = path_step_begins[path_id] + step_rank;
        return (anchor_step_bits[static_cast<std::size_t>(absolute_step / 64)] &
                (1ULL << (absolute_step % 64))) != 0;
    };

    // The default path still aggregates only min/max bounds. Local mode adds
    // the absolute anchor marker used later to split distant repeat hits.
    Timer phase_timer;
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
                mark_anchor_step(path_id, step_rank);
                ++result.posting_count;
            });
    }
    result.posting_seconds = phase_timer.elapsed();

    phase_timer.reset();
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
                select_node_rank(step.node_id);
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

        // The queried P/W source always keeps its exact coordinate occurrence,
        // so repeat hits elsewhere on that path cannot widen or split it.
        if (!exact_runs[path_id].empty()) {
            ++result.exact_reference_path_count;
            for (const auto& run : exact_runs[path_id]) {
                append_selected_run(run);
            }
            continue;
        }

        if (!local_gap_mode) {
            // Omitting --haplotype_gap preserves the original conservative
            // outermost-anchor behavior and its one-run-per-path output.
            append_selected_run(paths::SubpathRun{
                path_id,
                bounds.min_step,
                static_cast<std::uint64_t>(bounds.max_step) -
                    bounds.min_step + 1,
            });
            continue;
        }

        // Scan once between this path's outer anchors. A run remains open while
        // the total unanchored sequence since its last anchor is within the
        // limit; once exceeded, the next anchor starts a separate local run.
        std::vector<paths::SubpathRun> local_runs;
        bool have_anchor = false;
        bool gap_exceeded = false;
        std::uint64_t run_start = 0;
        std::uint64_t last_anchor = 0;
        std::uint64_t gap_bases = 0;
        const auto max_gap_bases = *options.max_gap_bases;

        path_index.for_each_step(
            path_id,
            bounds.min_step,
            static_cast<std::uint64_t>(bounds.max_step) -
                bounds.min_step + 1,
            [&](const paths::StepRecord& step,
                const std::uint64_t step_rank) {
                if (is_anchor_step(path_id, step_rank)) {
                    if (!have_anchor) {
                        have_anchor = true;
                        run_start = step_rank;
                    } else if (gap_exceeded) {
                        local_runs.push_back(paths::SubpathRun{
                            path_id,
                            run_start,
                            last_anchor - run_start + 1,
                        });
                        run_start = step_rank;
                    }
                    last_anchor = step_rank;
                    gap_bases = 0;
                    gap_exceeded = false;
                    return;
                }

                // Once the limit is exceeded, no more length lookups are needed
                // until the next anchor. The saturating comparison also avoids
                // uint64 overflow for very large user-provided limits.
                if (have_anchor && !gap_exceeded) {
                    if (step.node_id >= options.node_lengths->node_count()) {
                        throw std::runtime_error(
                            "Path references a node outside the .lnx length table");
                    }
                    const auto length = options.node_lengths->length(step.node_id);
                    if (length > max_gap_bases - gap_bases) {
                        gap_exceeded = true;
                    } else {
                        gap_bases += length;
                    }
                }
            });

        if (!have_anchor) {
            throw std::runtime_error(
                "Matched path bounds contain no marked anchor occurrence");
        }
        local_runs.push_back(paths::SubpathRun{
            path_id,
            run_start,
            last_anchor - run_start + 1,
        });
        result.local_non_reference_run_count += local_runs.size();
        if (local_runs.size() > 1) {
            ++result.local_split_path_count;
        }
        for (const auto& run : local_runs) {
            append_selected_run(run);
        }
    }
    result.selected_step_seconds = phase_timer.elapsed();

    phase_timer.reset();
    // Enumerating the rank-addressed bitset preserves deterministic node order
    // without retaining duplicate occurrences from selected path runs.
    result.node_ranks.reserve(static_cast<std::size_t>(selected_node_count));
    for (std::uint32_t node_rank = 0;
         node_rank < path_index.node_count();
         ++node_rank) {
        if ((selected_node_bits[node_rank / 64] &
             (1ULL << (node_rank % 64))) != 0) {
            result.node_ranks.push_back(node_rank);
        }
    }
    result.node_rank_materialization_seconds = phase_timer.elapsed();
    return result;
}

}  // namespace gfaidx::coordinates

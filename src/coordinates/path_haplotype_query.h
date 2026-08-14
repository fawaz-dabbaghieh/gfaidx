#ifndef GFAIDX_PATH_HAPLOTYPE_QUERY_H
#define GFAIDX_PATH_HAPLOTYPE_QUERY_H

#include <cstdint>
#include <optional>
#include <vector>

#include "indexer/node_length_index.h"
#include "paths/path_index.h"

namespace gfaidx::coordinates {

// Optional query controls. An absent gap preserves the conservative outermost
// anchor behavior; a present value enables local base-gap clustering.
struct PathHaplotypeQueryOptions {
    std::optional<std::uint64_t> max_gap_bases;
    const indexer::NodeLengthIndexReader* node_lengths{nullptr};
};

// Summary of one posting-driven all-haplotype selection. The returned node
// ranks are sorted and unique in the shared .ndx/.pdx rank space.
struct PathHaplotypeQueryResult {
    std::vector<std::uint32_t> node_ranks;
    // Preserve selected intervals exactly. Local mode can emit several runs
    // from one path, so output must not reconstruct them from the node union.
    std::vector<paths::SubpathRun> path_runs;
    std::uint64_t reference_node_count{};
    std::uint64_t posting_count{};
    std::uint64_t matched_path_count{};
    std::uint64_t selected_path_step_count{};
    // Exact coordinate-selected reference records are reported separately from
    // non-reference paths, including those split into local anchor clusters.
    std::uint64_t exact_reference_path_count{};
    std::uint64_t local_non_reference_run_count{};
    std::uint64_t local_split_path_count{};
    // Phase timings distinguish posting lookup, selected path scanning, and
    // final dense-rank materialization for large-query benchmarks.
    double posting_seconds{};
    double selected_step_seconds{};
    double node_rank_materialization_seconds{};
};

// Find every indexed P/W record containing at least one reference anchor.
// Exact coordinate-selected source runs always retain exact bounds. By default,
// other paths preserve their minimum/maximum anchor span; an optional base-gap
// limit instead splits distant anchor clusters into multiple local runs.
[[nodiscard]] PathHaplotypeQueryResult query_path_haplotype_nodes(
    const paths::PathIndexReader& path_index,
    const std::vector<std::uint32_t>& reference_node_ranks,
    const std::vector<paths::SubpathRun>& exact_reference_path_runs = {},
    const PathHaplotypeQueryOptions& options = {});

}  // namespace gfaidx::coordinates

#endif  // GFAIDX_PATH_HAPLOTYPE_QUERY_H

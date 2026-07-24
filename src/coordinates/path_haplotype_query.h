#ifndef GFAIDX_PATH_HAPLOTYPE_QUERY_H
#define GFAIDX_PATH_HAPLOTYPE_QUERY_H

#include <cstdint>
#include <vector>

#include "paths/path_index.h"

namespace gfaidx::coordinates {

// Summary of one posting-driven all-haplotype selection. The returned node
// ranks are sorted and unique in the shared .ndx/.pdx rank space.
struct PathHaplotypeQueryResult {
    std::vector<std::uint32_t> node_ranks;
    // Preserve the exact min/max interval selected on each matched path. The
    // output stage must not reconstruct these intervals from the node union,
    // because shared node ids can occur elsewhere and create incidental runs.
    std::vector<paths::SubpathRun> path_runs;
    std::uint64_t reference_node_count{};
    std::uint64_t posting_count{};
    std::uint64_t matched_path_count{};
    std::uint64_t selected_path_step_count{};
    // Resolution counters are reported by get_region so expensive chaining and
    // conservative single-anchor fallbacks remain visible to the user.
    std::uint64_t exact_reference_path_count{};
    std::uint64_t repeat_chained_path_count{};
    std::uint64_t repeat_fallback_path_count{};
};

// Find every indexed P/W record containing at least one ordered reference
// anchor. Exact coordinate-selected source runs override posting-derived bounds.
// Other paths use fast min/max bounds unless a repeated anchor defines an
// endpoint; ambiguous paths use forward/reverse ordered-anchor chaining.
[[nodiscard]] PathHaplotypeQueryResult query_path_haplotype_nodes(
    const paths::PathIndexReader& path_index,
    const std::vector<std::uint32_t>& ordered_reference_node_ranks,
    const std::vector<paths::SubpathRun>& exact_reference_path_runs = {});

}  // namespace gfaidx::coordinates

#endif  // GFAIDX_PATH_HAPLOTYPE_QUERY_H

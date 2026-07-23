#ifndef GFAIDX_PATH_HAPLOTYPE_QUERY_H
#define GFAIDX_PATH_HAPLOTYPE_QUERY_H

#include <cstdint>
#include <vector>

namespace gfaidx::paths {
class PathIndexReader;
}

namespace gfaidx::coordinates {

// Summary of one posting-driven all-haplotype selection. The returned node
// ranks are sorted and unique in the shared .ndx/.pdx rank space.
struct PathHaplotypeQueryResult {
    std::vector<std::uint32_t> node_ranks;
    std::uint64_t reference_node_count{};
    std::uint64_t posting_count{};
    std::uint64_t matched_path_count{};
    std::uint64_t selected_path_step_count{};
};

// Find every indexed P/W record that contains at least one reference interval
// node. For each matching record, include the complete step range between its
// minimum and maximum matching step, then return the union of those node ranks.
[[nodiscard]] PathHaplotypeQueryResult query_path_haplotype_nodes(
    const paths::PathIndexReader& path_index,
    const std::vector<std::uint32_t>& reference_node_ranks);

}  // namespace gfaidx::coordinates

#endif  // GFAIDX_PATH_HAPLOTYPE_QUERY_H

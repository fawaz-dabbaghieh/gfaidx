#ifndef GFAIDX_PATH_COORDINATE_QUERY_H
#define GFAIDX_PATH_COORDINATE_QUERY_H

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "paths/path_index.h"

namespace gfaidx::coordinates {

// Sequence coordinates covered by the first and last steps of one exact run.
struct ReferenceRunBounds {
    std::uint64_t first_step_start{};
    std::uint64_t last_step_end{};
};

struct PathCoordinateQueryResult {
    std::vector<std::uint32_t> node_ranks;
    // P/W fallback lookup already knows the exact path step interval, so pass
    // it forward instead of rediscovering it through node postings.
    std::vector<paths::SubpathRun> reference_path_runs;
    // Parallel to reference_path_runs.
    std::vector<ReferenceRunBounds> reference_run_bounds;
    std::size_t matched_path_count{};
};

// Resolve a coordinate interval directly from .pdx path steps and rank-aligned
// .lnx node lengths. This is the slower fallback for paths/walks that were not
// preselected into the .cdx coordinate sidecar.
PathCoordinateQueryResult query_path_coordinates_on_the_fly(
    const paths::PathIndexReader& path_index,
    const std::string& length_index_path,
    std::string_view reference_name,
    std::string_view sequence_name,
    std::uint64_t begin,
    std::uint64_t end);

}  // namespace gfaidx::coordinates

#endif  // GFAIDX_PATH_COORDINATE_QUERY_H

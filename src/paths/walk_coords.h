#ifndef GFAIDX_WALK_COORDS_H
#define GFAIDX_WALK_COORDS_H

#include <cstdint>
#include <functional>
#include <iosfwd>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "indexer/node_hash_index.h"
#include "indexer/node_length_index.h"
#include "paths/path_index.h"

namespace gfaidx::paths {

// Optional warning sink used by callers that want coordinate failures to degrade
// to ordinary W subpaths instead of aborting the whole graph extraction.
using WalkCoordWarning = std::function<void(const std::string&)>;

struct WalkCoordState {
    bool usable{false};
    std::vector<std::uint64_t> node_lengths;
    std::unique_ptr<indexer::NodeLengthIndexReader> length_index;

    [[nodiscard]] std::uint64_t length_count() const;
    [[nodiscard]] std::uint64_t node_length(std::uint32_t node_id) const;
};

struct PathCoordCacheEntry {
    bool ready{false};
    bool usable{false};
    PathInfo info;
    std::vector<StepRecord> steps;
    std::vector<std::uint64_t> prefix_lengths;
};

// Load rank-aligned node lengths, preferring a mmap-backed .lnx sidecar and
// falling back to scanning S lines from a GFA whose node set matches .pdx/.ndx.
WalkCoordState load_node_lengths_by_index(const PathIndexReader& index,
                                          const indexer::NodeHashIndex& node_index,
                                          const std::string& source_gfa,
                                          const std::string& length_index_path = std::string{},
                                          const WalkCoordWarning& warn = WalkCoordWarning{});

// Build and cache per-step prefix lengths for one W record so repeated subpath
// runs from the same walk do not repeatedly read and scan the full step vector.
PathCoordCacheEntry& get_or_build_path_coord_cache(
    const PathIndexReader& index,
    std::uint32_t path_id,
    const WalkCoordState& walk_coord_state,
    std::unordered_map<std::uint32_t, PathCoordCacheEntry>& cache,
    const WalkCoordWarning& warn = WalkCoordWarning{});

// Emit a W subpath with concrete SeqStart/SeqEnd coordinates derived from the
// cached prefix lengths. Callers should only use this when entry.usable is true.
void write_w_subpath_with_coords(std::ostream& out,
                                 const PathIndexReader& index,
                                 const PathCoordCacheEntry& entry,
                                 std::uint64_t start_step,
                                 std::uint64_t step_count,
                                 std::string_view subpath_label);

}  // namespace gfaidx::paths

#endif  // GFAIDX_WALK_COORDS_H

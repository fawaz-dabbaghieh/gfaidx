#ifndef GFAIDX_COORDINATE_INDEX_H
#define GFAIDX_COORDINATE_INDEX_H

#include <cstdint>
#include <fstream>
#include <string>
#include <string_view>
#include <vector>

#include "fs/Reader.h"

namespace gfaidx::coordinates {

// Public metadata for one continuous coordinate track fragment.
struct CoordinateTrackInfo {
    char source_type{};  // 'W' walk, 'P' path, or 'S' rGFA SR:i:0 nodes.
    std::string reference_name;
    std::string sequence_name;
    std::uint64_t haplotype{};
    std::uint64_t sequence_start{};
    std::uint64_t sequence_end{};
    std::uint64_t entry_begin{};
    std::uint64_t entry_count{};
};

// One exact slice selected from a coordinate track. start_step and step_count
// are local to the original P/W/S track, so a repeated node elsewhere cannot
// widen the coordinate-selected reference interval.
struct CoordinateTrackSlice {
    CoordinateTrackInfo track;
    std::uint64_t start_step{};
    std::uint64_t step_count{};
};

struct CoordinateQueryResult {
    // Sorted unique ranks are used as graph/BFS seeds.
    std::vector<std::uint32_t> node_ranks;
    // Exact slices retain the path occurrences selected by coordinates.
    std::vector<CoordinateTrackSlice> slices;
};

// Build a standalone coordinate index aligned to ranks in the supplied .ndx.
// An empty reference filter indexes every sample named by the header RS:Z tag.
bool build_coordinate_index(const std::string& input_gfa,
                            const std::string& output_index,
                            const std::string& node_index_path,
                            const std::string& reference_filter = std::string(""),
                            const Reader::Options& reader_options = Reader::Options{},
                            const std::string& path_index_path = std::string(""),
                            const std::string& path_names_file = std::string(""));

// Read the compact .cdx metadata eagerly while leaving the potentially large
// coordinate entry table on disk for binary-search and range reads.
class CoordinateIndexReader {
public:
    explicit CoordinateIndexReader(const std::string& index_path);

    [[nodiscard]] std::uint64_t node_count() const { return node_count_; }
    [[nodiscard]] const std::vector<CoordinateTrackInfo>& tracks() const { return tracks_; }

    // Return both unique graph ranks and the exact track slices that
    // overlap the requested 0-based, half-open interval [begin, end).
    [[nodiscard]] CoordinateQueryResult query_region(
        std::string_view reference_name,
        std::string_view sequence_name,
        std::uint64_t begin,
        std::uint64_t end) const;

    // Return sorted, unique .ndx/.pdx node ranks whose reference intervals
    // overlap the requested interval. This compatibility helper discards exact
    // track bounds; all-haplotype queries use query_region().
    [[nodiscard]] std::vector<std::uint32_t> query_node_ranks(
        std::string_view reference_name,
        std::string_view sequence_name,
        std::uint64_t begin,
        std::uint64_t end) const;

private:
    struct CoordinateEntry {
        std::uint64_t start{};
        std::uint32_t node_rank{};
    };

    [[nodiscard]] CoordinateEntry read_entry(std::uint64_t entry_index) const;
    void read_entries(std::uint64_t entry_index,
                      std::uint64_t count,
                      std::vector<CoordinateEntry>& out) const;

    std::string index_path_;
    mutable std::ifstream in_;
    std::uint64_t node_count_{};
    std::uint64_t entry_count_{};
    std::uint64_t entry_table_offset_{};
    std::vector<CoordinateTrackInfo> tracks_;
};

}  // namespace gfaidx::coordinates

#endif  // GFAIDX_COORDINATE_INDEX_H

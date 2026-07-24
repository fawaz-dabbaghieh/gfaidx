#ifndef GFAIDX_PATH_COORDINATE_CHECKPOINTS_H
#define GFAIDX_PATH_COORDINATE_CHECKPOINTS_H

#include <cstddef>
#include <cstdint>
#include <string>

namespace gfaidx::paths {

class PathIndexReader;

// A checkpoint every 4096 path steps keeps the sidecar small while limiting a
// coordinate query to at most 4095 prefix steps before its requested slice.
inline constexpr std::uint64_t kDefaultPathCheckpointStride = 4096;
// Large path indexes can take substantial time to scan. Report progress in
// bounded groups by default; callers may use zero to disable these messages.
inline constexpr std::uint64_t kDefaultPathCheckpointProgressEvery = 10;

// Build an optional path-coordinate checkpoint sidecar from an existing .pdx
// and its rank-aligned .lnx. The original graph and other indexes are unchanged.
void build_path_coordinate_checkpoint_index(
    const std::string& path_index_path,
    const std::string& node_length_index_path,
    const std::string& output_path,
    std::uint64_t checkpoint_stride = kDefaultPathCheckpointStride,
    std::uint64_t progress_every_paths =
        kDefaultPathCheckpointProgressEvery);

// Mmap-backed reader for the small .pcx sidecar. Checkpoint values are path-local
// cumulative sequence lengths immediately before a checkpointed step.
class PathCoordinateCheckpointIndexReader {
public:
    explicit PathCoordinateCheckpointIndexReader(const std::string& path);
    ~PathCoordinateCheckpointIndexReader();

    PathCoordinateCheckpointIndexReader(
        const PathCoordinateCheckpointIndexReader&) = delete;
    PathCoordinateCheckpointIndexReader& operator=(
        const PathCoordinateCheckpointIndexReader&) = delete;

    // Validate that this sidecar was built for the supplied path index and
    // rank-aligned node-length table. A mismatch must fall back to prefix scans.
    void validate_against(const PathIndexReader& path_index,
                          std::uint64_t node_length_count) const;

    [[nodiscard]] std::uint64_t checkpoint_stride() const;
    [[nodiscard]] std::uint64_t path_count() const;
    [[nodiscard]] std::uint64_t checkpoint_count() const;

    // Return the cumulative path-local sequence length at the checkpoint at or
    // before step_rank, and return that checkpoint's step rank separately.
    [[nodiscard]] std::uint64_t prefix_before_step(
        std::uint32_t path_id,
        std::uint64_t step_rank,
        std::uint64_t& checkpoint_step_rank) const;

private:
    void close_mapping();

    int fd_{-1};
    void* mapping_{nullptr};
    std::size_t file_size_{0};
    const void* header_{nullptr};
    const void* path_records_{nullptr};
    const std::uint64_t* checkpoints_{nullptr};
};

}  // namespace gfaidx::paths

#endif  // GFAIDX_PATH_COORDINATE_CHECKPOINTS_H

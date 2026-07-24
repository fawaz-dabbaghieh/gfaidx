#include "paths/path_coordinate_checkpoints.h"

#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "fs/fs_helpers.h"
#include "indexer/node_length_index.h"
#include "paths/path_index.h"
#include "utils/Timer.h"

namespace gfaidx::paths {
namespace {

constexpr char kCheckpointMagic[8] =
    {'G', 'F', 'A', 'P', 'C', 'X', '0', '1'};
constexpr std::uint32_t kCheckpointVersion = 1;

// The header records enough information to reject a sidecar paired with a
// different .pdx even when the filenames happen to look compatible.
struct CheckpointHeaderDisk {
    char magic[8]{};
    std::uint32_t version{};
    std::uint32_t reserved{};
    std::uint64_t path_count{};
    std::uint64_t node_count{};
    std::uint64_t total_step_count{};
    std::uint64_t checkpoint_stride{};
    std::uint64_t checkpoint_count{};
    std::uint64_t path_layout_hash{};
    std::uint64_t path_table_offset{};
    std::uint64_t checkpoint_table_offset{};
};

// One record per .pdx path locates its contiguous checkpoint slice and records
// the expected step count for compatibility validation.
struct CheckpointPathRecordDisk {
    std::uint64_t step_count{};
    std::uint64_t checkpoint_begin{};
    std::uint64_t checkpoint_count{};
};

static_assert(sizeof(CheckpointHeaderDisk) == 80,
              "Unexpected path checkpoint header size");
static_assert(sizeof(CheckpointPathRecordDisk) == 24,
              "Unexpected path checkpoint record size");

constexpr std::uint64_t kFnvOffset = 1469598103934665603ULL;
constexpr std::uint64_t kFnvPrime = 1099511628211ULL;

void hash_bytes(std::uint64_t& hash, const void* data, std::size_t size) {
    const auto* bytes = static_cast<const unsigned char*>(data);
    for (std::size_t i = 0; i < size; ++i) {
        hash ^= bytes[i];
        hash *= kFnvPrime;
    }
}

void hash_string(std::uint64_t& hash, std::string_view value) {
    const auto size = static_cast<std::uint64_t>(value.size());
    hash_bytes(hash, &size, sizeof(size));
    hash_bytes(hash, value.data(), value.size());
}

// Hash path metadata that is cheap to validate at query time. Step contents are
// not hashed because rereading the full step table would defeat the checkpoint.
std::uint64_t path_layout_hash(const PathIndexReader& index) {
    std::uint64_t hash = kFnvOffset;
    const auto path_count = index.path_count();
    const auto node_count = index.node_count();
    const auto total_steps = index.total_step_count();
    hash_bytes(hash, &path_count, sizeof(path_count));
    hash_bytes(hash, &node_count, sizeof(node_count));
    hash_bytes(hash, &total_steps, sizeof(total_steps));

    for (std::uint32_t path_id = 0; path_id < path_count; ++path_id) {
        const auto info = index.get_path_info(path_id);
        hash_bytes(hash, &info.record_type, sizeof(info.record_type));
        hash_bytes(hash, &info.step_count, sizeof(info.step_count));
        hash_bytes(hash, &info.hap_index, sizeof(info.hap_index));
        hash_bytes(hash, &info.seq_start, sizeof(info.seq_start));
        hash_bytes(hash, &info.seq_end, sizeof(info.seq_end));
        hash_string(hash, info.name);
        hash_string(hash, info.sample_id);
        hash_string(hash, info.seq_id);
    }
    return hash;
}

std::uint64_t checked_add(std::uint64_t lhs,
                          std::uint64_t rhs,
                          std::string_view context) {
    if (rhs > std::numeric_limits<std::uint64_t>::max() - lhs) {
        throw std::runtime_error(std::string(context) + " overflow");
    }
    return lhs + rhs;
}

std::uint64_t checked_multiply(std::uint64_t lhs,
                               std::uint64_t rhs,
                               std::string_view context) {
    if (lhs != 0 &&
        rhs > std::numeric_limits<std::uint64_t>::max() / lhs) {
        throw std::runtime_error(std::string(context) + " overflow");
    }
    return lhs * rhs;
}

void write_or_throw(std::ofstream& out,
                    const void* data,
                    std::size_t size,
                    std::string_view context) {
    if (size == 0) return;
    out.write(static_cast<const char*>(data),
              static_cast<std::streamsize>(size));
    if (!out) {
        throw std::runtime_error("Failed while writing " +
                                 std::string(context));
    }
}

const CheckpointHeaderDisk& mapped_header(const void* header) {
    return *static_cast<const CheckpointHeaderDisk*>(header);
}

const CheckpointPathRecordDisk* mapped_path_records(const void* records) {
    return static_cast<const CheckpointPathRecordDisk*>(records);
}

}  // namespace

void build_path_coordinate_checkpoint_index(
    const std::string& path_index_path,
    const std::string& node_length_index_path,
    const std::string& output_path,
    std::uint64_t checkpoint_stride,
    std::uint64_t progress_every_paths) {

    if (checkpoint_stride == 0) {
        throw std::runtime_error(
            "Path coordinate checkpoint stride must be greater than zero");
    }
    if (file_exists(output_path.c_str())) {
        throw std::runtime_error(
            "Path coordinate checkpoint index already exists: " + output_path);
    }

    PathIndexReader path_index(path_index_path);
    indexer::NodeLengthIndexReader lengths(node_length_index_path);
    if (path_index.node_count() != lengths.node_count()) {
        throw std::runtime_error(
            ".pdx and .lnx node counts differ; rebuild aligned indexes");
    }

    // Keep the growing output in a visible run directory beside the final
    // sidecar. If the process is interrupted, users can find that directory
    // through latest_path_checkpoints instead of accumulating hidden files.
    const std::filesystem::path output_target(output_path);
    const auto output_parent = output_target.has_parent_path()
        ? output_target.parent_path()
        : std::filesystem::current_path();
    constexpr const char* kLatestCheckpointTemp =
        "latest_path_checkpoints";
    const auto temp_dir = create_temp_dir(
        output_parent.string(),
        "gfaidx_path_checkpoints_tmp_",
        kLatestCheckpointTemp,
        true);
    const auto staged_output =
        (std::filesystem::path(temp_dir) / "path_checkpoints.pcx").string();
    const auto latest_path =
        (output_parent / kLatestCheckpointTemp).string();
    auto cleanup_temp_output = [&]() {
        cleanup_temp_dir(temp_dir, latest_path);
    };

    try {
        Timer progress_timer;
        CheckpointHeaderDisk header{};
        std::memcpy(header.magic, kCheckpointMagic, sizeof(header.magic));
        header.version = kCheckpointVersion;
        header.path_count = path_index.path_count();
        header.node_count = path_index.node_count();
        header.total_step_count = path_index.total_step_count();
        header.checkpoint_stride = checkpoint_stride;
        header.path_layout_hash = path_layout_hash(path_index);
        header.path_table_offset = sizeof(CheckpointHeaderDisk);

        const auto path_table_bytes = checked_multiply(
            header.path_count,
            sizeof(CheckpointPathRecordDisk),
            "Path checkpoint table size");
        header.checkpoint_table_offset =
            checked_add(header.path_table_offset,
                        path_table_bytes,
                        "Path checkpoint table offset");

        std::vector<CheckpointPathRecordDisk> path_records(
            static_cast<std::size_t>(header.path_count));
        std::ofstream out(staged_output,
                          std::ios::binary | std::ios::trunc);
        if (!out) {
            throw std::runtime_error(
                "Failed to open path checkpoint output: " + staged_output);
        }
        std::cout << get_time()
                  << ": Streaming path checkpoints to temporary file "
                  << staged_output << std::endl;
        std::cout << get_time() << ": Latest checkpoint temp directory: "
                  << latest_path << std::endl;
        std::cout << get_time() << ": Checkpoint scan covers "
                  << header.path_count << " paths and "
                  << header.total_step_count << " path steps";
        if (progress_every_paths == 0) {
            std::cout << "; periodic progress is disabled" << std::endl;
        } else {
            std::cout << "; reporting every " << progress_every_paths
                      << " completed path(s)" << std::endl;
        }

        // Reserve the fixed tables first. They are rewritten after checkpoint
        // counts and offsets become known during the streaming path scan.
        write_or_throw(out, &header, sizeof(header),
                       "path checkpoint header");
        if (!path_records.empty()) {
            write_or_throw(out,
                           path_records.data(),
                           path_records.size() *
                               sizeof(CheckpointPathRecordDisk),
                           "path checkpoint metadata");
        }

        std::uint64_t total_steps_seen = 0;
        std::uint64_t total_checkpoints = 0;
        for (std::uint32_t path_id = 0;
             path_id < path_index.path_count();
             ++path_id) {
            const auto info = path_index.get_path_info(path_id);
            if (progress_every_paths != 0 && path_id == 0) {
                // Announce the first path before scanning it so one unusually
                // long first record does not look like a stalled process.
                std::cout << get_time() << ": Starting checkpoint path "
                          << (static_cast<std::uint64_t>(path_id) + 1)
                          << "/" << header.path_count << " ("
                          << info.step_count << " steps)" << std::endl;
            }
            auto& record = path_records[path_id];
            record.step_count = info.step_count;
            record.checkpoint_begin = total_checkpoints;

            // Every path stores checkpoint zero, including an empty path.
            std::uint64_t cumulative = 0;
            write_or_throw(out, &cumulative, sizeof(cumulative),
                           "path checkpoint value");
            record.checkpoint_count = 1;
            ++total_checkpoints;

            path_index.for_each_step(
                path_id,
                0,
                info.step_count,
                [&](const StepRecord& step, std::uint64_t step_rank) {
                    if (step.node_id >= lengths.node_count()) {
                        throw std::runtime_error(
                            "Path step refers to a node outside the .lnx table");
                    }
                    cumulative = checked_add(
                        cumulative,
                        lengths.length(step.node_id),
                        "Path cumulative coordinate");

                    const auto next_step = step_rank + 1;
                    if (next_step % checkpoint_stride == 0) {
                        write_or_throw(out,
                                       &cumulative,
                                       sizeof(cumulative),
                                       "path checkpoint value");
                        ++record.checkpoint_count;
                        ++total_checkpoints;
                    }
                });
            total_steps_seen = checked_add(total_steps_seen,
                                           info.step_count,
                                           "Total path step count");

            const auto completed_paths =
                static_cast<std::uint64_t>(path_id) + 1;
            if (progress_every_paths != 0 &&
                (completed_paths % progress_every_paths == 0 ||
                 completed_paths == header.path_count)) {
                // Flushing at reporting boundaries makes staged-file growth
                // visible to filesystem tools and detects write errors early.
                out.flush();
                if (!out) {
                    throw std::runtime_error(
                        "Failed while flushing path checkpoint output");
                }
                const double percent = header.total_step_count == 0
                    ? 100.0
                    : 100.0 * static_cast<double>(total_steps_seen) /
                          static_cast<double>(header.total_step_count);
                std::cout << get_time() << ": Processed "
                          << completed_paths << "/" << header.path_count
                          << " paths, " << total_steps_seen << "/"
                          << header.total_step_count << " steps ("
                          << std::fixed << std::setprecision(1) << percent
                          << "%), wrote " << total_checkpoints
                          << " checkpoints in " << std::defaultfloat
                          << std::setprecision(6)
                          << progress_timer.elapsed() << " seconds";
                if (completed_paths < header.path_count) {
                    const auto next_info = path_index.get_path_info(
                        static_cast<std::uint32_t>(completed_paths));
                    std::cout << "; next path "
                              << (completed_paths + 1) << "/"
                              << header.path_count << " has "
                              << next_info.step_count << " steps";
                }
                std::cout << std::endl;
            }
        }

        if (total_steps_seen != header.total_step_count) {
            throw std::runtime_error(
                ".pdx path metadata does not sum to its header step count");
        }
        header.checkpoint_count = total_checkpoints;

        // Publish the completed metadata only after every checkpoint value has
        // been written successfully.
        out.seekp(0, std::ios::beg);
        if (!out) {
            throw std::runtime_error(
                "Failed to seek while finalizing path checkpoint index");
        }
        write_or_throw(out, &header, sizeof(header),
                       "path checkpoint header");
        if (!path_records.empty()) {
            write_or_throw(out,
                           path_records.data(),
                           path_records.size() *
                               sizeof(CheckpointPathRecordDisk),
                           "path checkpoint metadata");
        }
        out.close();
        if (!out) {
            throw std::runtime_error(
                "Failed while closing path checkpoint index: " + output_path);
        }

        rename_path_or_throw(staged_output, output_path);
        cleanup_temp_output();
    } catch (...) {
        // Handled failures are cleaned immediately. An abrupt interruption
        // leaves the visible run directory and latest symlink discoverable.
        cleanup_temp_output();
        throw;
    }
}

PathCoordinateCheckpointIndexReader::
PathCoordinateCheckpointIndexReader(const std::string& path) {
    fd_ = ::open(path.c_str(), O_RDONLY);
    if (fd_ == -1) {
        throw std::runtime_error(
            "Failed to open path checkpoint index: " + path);
    }

    struct stat st{};
    if (fstat(fd_, &st) == -1) {
        ::close(fd_);
        fd_ = -1;
        throw std::runtime_error(
            "Failed to stat path checkpoint index: " + path);
    }
    file_size_ = static_cast<std::size_t>(st.st_size);
    if (file_size_ < sizeof(CheckpointHeaderDisk)) {
        close_mapping();
        throw std::runtime_error(
            "Path checkpoint index is too small: " + path);
    }

    mapping_ = mmap(nullptr, file_size_, PROT_READ, MAP_SHARED, fd_, 0);
    if (mapping_ == MAP_FAILED) {
        mapping_ = nullptr;
        close_mapping();
        throw std::runtime_error(
            "mmap failed for path checkpoint index: " + path);
    }

    header_ = mapping_;
    const auto& header = mapped_header(header_);
    if (std::memcmp(header.magic,
                    kCheckpointMagic,
                    sizeof(header.magic)) != 0) {
        close_mapping();
        throw std::runtime_error(
            "Invalid path checkpoint index magic: " + path);
    }
    if (header.version != kCheckpointVersion) {
        close_mapping();
        throw std::runtime_error(
            "Unsupported path checkpoint index version: " +
            std::to_string(header.version));
    }
    if (header.checkpoint_stride == 0) {
        close_mapping();
        throw std::runtime_error(
            "Path checkpoint index has zero checkpoint stride");
    }

    std::uint64_t path_table_bytes = 0;
    std::uint64_t checkpoint_table_bytes = 0;
    try {
        path_table_bytes = checked_multiply(
            header.path_count,
            sizeof(CheckpointPathRecordDisk),
            "Path checkpoint table size");
        checkpoint_table_bytes = checked_multiply(
            header.checkpoint_count,
            sizeof(std::uint64_t),
            "Path checkpoint value table size");
    } catch (...) {
        close_mapping();
        throw;
    }
    const bool offsets_are_aligned =
        header.path_table_offset % alignof(CheckpointPathRecordDisk) == 0 &&
        header.checkpoint_table_offset % alignof(std::uint64_t) == 0;
    if (header.path_table_offset < sizeof(CheckpointHeaderDisk) ||
        header.path_table_offset > file_size_ ||
        path_table_bytes > file_size_ - header.path_table_offset ||
        header.checkpoint_table_offset > file_size_ ||
        checkpoint_table_bytes >
            file_size_ - header.checkpoint_table_offset ||
        !offsets_are_aligned ||
        header.checkpoint_table_offset < header.path_table_offset ||
        path_table_bytes >
            header.checkpoint_table_offset - header.path_table_offset) {
        close_mapping();
        throw std::runtime_error(
            "Path checkpoint index table offsets are invalid");
    }

    const auto* bytes = static_cast<const unsigned char*>(mapping_);
    path_records_ = bytes + header.path_table_offset;
    checkpoints_ = reinterpret_cast<const std::uint64_t*>(
        bytes + header.checkpoint_table_offset);
}

PathCoordinateCheckpointIndexReader::
~PathCoordinateCheckpointIndexReader() {
    close_mapping();
}

void PathCoordinateCheckpointIndexReader::close_mapping() {
    if (mapping_) {
        munmap(mapping_, file_size_);
        mapping_ = nullptr;
    }
    if (fd_ != -1) {
        ::close(fd_);
        fd_ = -1;
    }
    file_size_ = 0;
    header_ = nullptr;
    path_records_ = nullptr;
    checkpoints_ = nullptr;
}

void PathCoordinateCheckpointIndexReader::validate_against(
    const PathIndexReader& path_index,
    std::uint64_t node_length_count) const {

    const auto& header = mapped_header(header_);
    if (header.path_count != path_index.path_count() ||
        header.node_count != path_index.node_count() ||
        header.node_count != node_length_count ||
        header.total_step_count != path_index.total_step_count()) {
        throw std::runtime_error(
            ".pcx counts do not match the supplied .pdx/.lnx");
    }
    if (header.path_layout_hash != path_layout_hash(path_index)) {
        throw std::runtime_error(
            ".pcx path metadata does not match the supplied .pdx");
    }

    const auto* records = mapped_path_records(path_records_);
    std::uint64_t step_sum = 0;
    for (std::uint32_t path_id = 0;
         path_id < path_index.path_count();
         ++path_id) {
        const auto info = path_index.get_path_info(path_id);
        const auto& record = records[path_id];
        const auto expected_checkpoints =
            info.step_count / header.checkpoint_stride + 1;
        if (record.step_count != info.step_count ||
            record.checkpoint_count != expected_checkpoints ||
            record.checkpoint_begin > header.checkpoint_count ||
            record.checkpoint_count >
                header.checkpoint_count - record.checkpoint_begin) {
            throw std::runtime_error(
                ".pcx path checkpoint metadata is incompatible with .pdx");
        }
        step_sum = checked_add(step_sum,
                               info.step_count,
                               "Validated path step count");
    }
    if (step_sum != header.total_step_count) {
        throw std::runtime_error(
            ".pcx validated path steps do not match its header");
    }
}

std::uint64_t
PathCoordinateCheckpointIndexReader::checkpoint_stride() const {
    return mapped_header(header_).checkpoint_stride;
}

std::uint64_t
PathCoordinateCheckpointIndexReader::path_count() const {
    return mapped_header(header_).path_count;
}

std::uint64_t
PathCoordinateCheckpointIndexReader::checkpoint_count() const {
    return mapped_header(header_).checkpoint_count;
}

std::uint64_t PathCoordinateCheckpointIndexReader::prefix_before_step(
    std::uint32_t path_id,
    std::uint64_t step_rank,
    std::uint64_t& checkpoint_step_rank) const {

    const auto& header = mapped_header(header_);
    if (path_id >= header.path_count) {
        throw std::runtime_error(
            "Path id is outside the .pcx path table");
    }
    const auto& record = mapped_path_records(path_records_)[path_id];
    if (step_rank > record.step_count) {
        throw std::runtime_error(
            "Step rank is outside the .pcx path step range");
    }

    const auto checkpoint_index = step_rank / header.checkpoint_stride;
    if (checkpoint_index >= record.checkpoint_count) {
        throw std::runtime_error(
            "Step rank has no matching .pcx checkpoint");
    }
    checkpoint_step_rank =
        checkpoint_index * header.checkpoint_stride;
    const auto absolute_checkpoint =
        checked_add(record.checkpoint_begin,
                    checkpoint_index,
                    "Path checkpoint lookup");
    if (absolute_checkpoint >= header.checkpoint_count) {
        throw std::runtime_error(
            "Path checkpoint lookup is outside the .pcx value table");
    }
    return checkpoints_[absolute_checkpoint];
}

}  // namespace gfaidx::paths

#ifndef GFAIDX_PATH_INDEX_H
#define GFAIDX_PATH_INDEX_H

#include <cstdint>
#include <fstream>
#include <functional>
#include <iosfwd>
#include <limits>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "fs/Reader.h"

namespace gfaidx::paths {

// Lightweight path metadata returned by the reader. String views point into the
// in-memory string blob owned by PathIndexReader.
struct PathInfo {
    char record_type{};
    std::uint32_t path_id{};
    std::uint64_t step_begin{};
    std::uint64_t step_count{};
    std::string_view name;
    std::string_view overlap_field;
    std::string_view tags;
    std::string_view sample_id;
    std::uint64_t hap_index{};
    std::string_view seq_id;
    std::int64_t seq_start{-1};
    std::int64_t seq_end{-1};
};

struct StepRecord {
    std::uint32_t node_id{};
    bool is_reverse{};
};

// A contiguous run of steps from one original path that stays inside a queried
// node set. Node-set queries can yield multiple runs per path.
struct SubpathRun {
    std::uint32_t path_id{};
    std::uint64_t start_step{};
    std::uint64_t step_count{};
};

bool build_path_index(const std::string& input_gfa,
                      const std::string& output_index,
                      const Reader::Options& reader_options = Reader::Options{});

class PathIndexReader {
public:
    explicit PathIndexReader(const std::string& index_path);

    [[nodiscard]] std::uint32_t path_count() const {
        return static_cast<std::uint32_t>(paths_.size());
    }

    [[nodiscard]] std::uint32_t node_count() const {
        return static_cast<std::uint32_t>(nodes_.size());
    }

    [[nodiscard]] bool lookup_path_id(const std::string& name, std::uint32_t& out_path_id) const;
    [[nodiscard]] bool lookup_node_id(const std::string& name, std::uint32_t& out_node_id) const;

    [[nodiscard]] PathInfo get_path_info(std::uint32_t path_id) const;
    [[nodiscard]] std::string_view get_path_name(std::uint32_t path_id) const;
    [[nodiscard]] std::string_view get_node_name(std::uint32_t node_id) const;
    [[nodiscard]] std::string_view get_overlap_field(std::uint32_t path_id) const;
    [[nodiscard]] std::string_view get_tags(std::uint32_t path_id) const;

    [[nodiscard]] std::vector<StepRecord> read_steps(
        std::uint32_t path_id,
        std::uint64_t start_step = 0,
        std::uint64_t max_steps = std::numeric_limits<std::uint64_t>::max()) const;

    void for_each_node_posting(
        std::uint32_t node_id,
        const std::function<void(std::uint32_t path_id, std::uint32_t step_rank)>& callback) const;

private:
    // On-disk metadata for one path. The actual strings live in the shared
    // string blob, while steps and postings live in flat binary tables.
    struct PathMeta {
        char record_type{};
        std::uint64_t name_offset{};
        std::uint64_t name_len{};
        std::uint64_t step_begin{};
        std::uint64_t step_count{};
        std::uint64_t overlap_offset{};
        std::uint64_t overlap_len{};
        std::uint64_t tags_offset{};
        std::uint64_t tags_len{};
        std::uint64_t sample_offset{};
        std::uint64_t sample_len{};
        std::uint64_t hap_index{};
        std::uint64_t seq_id_offset{};
        std::uint64_t seq_id_len{};
        std::int64_t seq_start{-1};
        std::int64_t seq_end{-1};
    };

    struct NodeMeta {
        std::uint64_t name_offset{};
        std::uint64_t name_len{};
        std::uint64_t posting_begin{};
        std::uint64_t posting_count{};
    };

    [[nodiscard]] std::string_view view_string(std::uint64_t offset, std::uint64_t len) const;
    void read_exact(std::uint64_t offset, void* dst, std::size_t bytes) const;

    std::string index_path_;
    mutable std::ifstream in_;
    std::uint64_t step_table_offset_{};
    std::uint64_t posting_table_offset_{};
    std::vector<PathMeta> paths_;
    std::vector<NodeMeta> nodes_;
    std::string strings_;
    std::unordered_map<std::string, std::uint32_t> path_name_to_id_;
    std::unordered_map<std::string, std::uint32_t> node_name_to_id_;
};

// Resolve a node set into all path runs that remain contiguous inside that set.
std::vector<SubpathRun> find_subpaths_for_nodes(const PathIndexReader& index,
                                                const std::vector<std::string>& node_names);

void write_path_as_gfa_line(std::ostream& out,
                            const PathIndexReader& index,
                            std::uint32_t path_id);

void write_subpath_as_gfa_line(std::ostream& out,
                               const PathIndexReader& index,
                               std::uint32_t path_id,
                               std::uint64_t start_step,
                               std::uint64_t step_count,
                               std::string_view output_name);

}  // namespace gfaidx::paths

#endif  // GFAIDX_PATH_INDEX_H

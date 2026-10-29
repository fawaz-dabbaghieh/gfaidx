#ifndef GFAIDX_PATH_INDEX_H
#define GFAIDX_PATH_INDEX_H

#include <cstddef>
#include <cstdint>
#include <deque>
#include <fstream>
#include <functional>
#include <iosfwd>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "fs/Reader.h"

namespace gfaidx::paths {

// Lightweight path metadata returned by the reader. String views point into the
// path metadata strings owned by PathIndexReader.
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

// Step table layouts. packed32 is the v4 layout (one uint32 per step);
// delta_varint stores zigzag deltas in 128-step blocks (see pdx_codec.h).
enum class StepEncoding : std::uint32_t {
    packed32 = 0,
    delta_varint = 1,
};

// Per-node posting blob layouts.
enum class PostingEncoding : std::uint32_t {
    // v4: per path group varint path_delta, count, first rank, rank deltas.
    path_groups = 0,
    // Occurrences predicted from the previous nodes of a block of consecutive
    // node ids (see pdx_codec.h); needs locality-ordered node ids.
    path_slots = 1,
};

inline constexpr std::uint32_t kDefaultPostingBlockNodes = 32;

struct PathIndexEncoding {
    StepEncoding steps{StepEncoding::delta_varint};
    PostingEncoding postings{PostingEncoding::path_slots};
    std::uint32_t posting_block_nodes{kDefaultPostingBlockNodes};
};

// Build the disk-backed .pdx path index for P/W records in a GFA file. Node
// ids are aligned to the supplied .ndx file so path queries and graph queries
// share the same integer node space. A legacy .ndx with the v4 encodings
// writes a v4 file byte for byte; everything else writes v5.
bool build_path_index(const std::string& input_gfa,
                      const std::string& output_index,
                      const std::string& node_index_path,
                      const Reader::Options& reader_options = Reader::Options{},
                      const std::string& tmp_base_dir = std::string(""),
                      bool keep_tmp = false,
                      const PathIndexEncoding& encoding = PathIndexEncoding{});

namespace detail {

// Temporary posting emitted for each step occurrence before postings are sorted
// by node and compressed into the final per-node posting blob.
struct TempPosting {
    std::uint32_t node_id{};
    std::uint32_t path_id{};
    std::uint32_t step_rank{};
};

// Bounded in-memory posting accumulator that spills sorted posting runs to disk
// once the configured chunk size is reached.
class PostingRunBuilder {
public:
    explicit PostingRunBuilder(std::string temp_dir, std::size_t max_records);

    void add(std::uint32_t node_id, std::uint32_t path_id, std::uint32_t step_rank);
    void finish();

    [[nodiscard]] const std::vector<std::string>& run_paths() const;
    [[nodiscard]] std::uint64_t total_postings() const;
    [[nodiscard]] std::size_t run_count() const;

private:
    void flush_run();

    std::string temp_dir_;
    std::size_t max_records_{};
    std::uint64_t total_postings_{};
    std::vector<TempPosting> chunk_;
    std::vector<std::string> run_paths_;
};

// Sequential reader over one sorted posting run file used during k-way merge.
struct PostingRunCursor {
    explicit PostingRunCursor(const std::string& path);

    bool advance();

    std::ifstream in;
    TempPosting current{};
    bool valid{false};
};

// Heap payload and comparator used by the posting-run k-way merge.
struct PostingHeapItem {
    TempPosting posting{};
    std::size_t run_index{};
};

struct PostingHeapGreater {
    bool operator()(const PostingHeapItem& lhs, const PostingHeapItem& rhs) const;
};

}  // namespace detail

class PathIndexReader;

namespace detail {
// Decoded posting block plus the decoder state needed to continue it.
struct PostingBlockCache;
// Recently decoded 128-step blocks and the last step superblock record.
struct StepBlockCache;
}  // namespace detail

// Posting-only view of one .pdx for one worker thread. Reads use pread on the
// reader's file descriptor, so cursors never share stream state; each cursor
// keeps its own decoded-block cache, which makes runs of consecutive node ids
// decode each block once.
//
// Construct one via PathIndexReader::open_posting_cursor(); the reader must
// outlive every cursor it hands out.
class PostingCursor {
public:
    explicit PostingCursor(const PathIndexReader& index);
    ~PostingCursor();
    PostingCursor(PostingCursor&&) noexcept;
    PostingCursor& operator=(PostingCursor&&) noexcept;

    void for_each_node_posting(
        std::uint32_t node_id,
        const std::function<void(std::uint32_t path_id, std::uint32_t step_rank)>& callback);

private:
    const PathIndexReader* index_;
    std::unique_ptr<detail::PostingBlockCache> cache_;
};

class PathIndexReader {
public:
    explicit PathIndexReader(const std::string& index_path);
    ~PathIndexReader();

    // The reader owns its file descriptor. Like the node-name caches, the
    // step and posting caches make one reader unsafe to share between
    // threads; workers open their own reader or PostingCursor.
    PathIndexReader(const PathIndexReader&) = delete;
    PathIndexReader& operator=(const PathIndexReader&) = delete;

    [[nodiscard]] std::uint32_t path_count() const {
        return static_cast<std::uint32_t>(paths_.size());
    }

    [[nodiscard]] std::uint32_t node_count() const {
        return node_count_;
    }

    [[nodiscard]] std::uint64_t total_step_count() const {
        return total_step_count_;
    }

    // Fingerprint of the .ndx node numbering this index was built against
    // (0 for legacy hash-rank ids). See indexer::require_matching_node_ids.
    [[nodiscard]] std::uint64_t node_id_space() const {
        return node_id_space_;
    }

    [[nodiscard]] std::uint32_t format_version() const {
        return format_version_;
    }

    [[nodiscard]] PathIndexEncoding encoding() const {
        return PathIndexEncoding{step_encoding_, posting_encoding_, posting_block_nodes_};
    }

    [[nodiscard]] bool lookup_path_id(const std::string& name, std::uint32_t& out_path_id) const;

    [[nodiscard]] PathInfo get_path_info(std::uint32_t path_id) const;
    [[nodiscard]] std::string_view get_path_name(std::uint32_t path_id) const;
    [[nodiscard]] std::string_view get_node_name(std::uint32_t node_id) const;
    // Return one owned node name without retaining it in the reader cache.
    // Large one-pass rank materializations use this to keep memory bounded.
    [[nodiscard]] std::string copy_node_name(std::uint32_t node_id) const;
    [[nodiscard]] std::string_view get_overlap_field(std::uint32_t path_id) const;
    [[nodiscard]] std::string_view get_tags(std::uint32_t path_id) const;

    [[nodiscard]] std::vector<StepRecord> read_steps(
        std::uint32_t path_id,
        std::uint64_t start_step = 0,
        std::uint64_t max_steps = std::numeric_limits<std::uint64_t>::max()) const;

    void for_each_step(
        std::uint32_t path_id,
        std::uint64_t start_step,
        std::uint64_t max_steps,
        const std::function<void(const StepRecord& step, std::uint64_t step_rank)>& callback) const;

    // Uses a reader-owned block cache, so like the node-name caches it is not
    // safe to call concurrently; worker threads use open_posting_cursor().
    // Postings are delivered sorted by (path id, step rank).
    void for_each_node_posting(
        std::uint32_t node_id,
        const std::function<void(std::uint32_t path_id, std::uint32_t step_rank)>& callback) const;

    // Open a posting-only view for a worker thread. See PostingCursor.
    [[nodiscard]] PostingCursor open_posting_cursor() const {
        return PostingCursor(*this);
    }

private:
    friend class PostingCursor;

    // Path metadata is loaded eagerly because the number of paths is small
    // compared to the number of graph nodes. Node metadata, steps and
    // postings stay in the mapped file and are only touched on demand, which
    // keeps query-time memory bounded on large graphs.
    struct PathMeta {
        char record_type{};
        std::string name;
        std::uint64_t step_begin{};
        std::uint64_t step_count{};
        std::string overlap_field;
        std::string tags;
        std::string sample_id;
        std::uint64_t hap_index{};
        std::string seq_id;
        std::int64_t seq_start{-1};
        std::int64_t seq_end{-1};
    };

    // Visit global steps [first, first + count) in order.
    template <typename Fn>
    void visit_global_steps(std::uint64_t first, std::uint64_t count, Fn&& fn) const;
    [[nodiscard]] std::uint64_t step_block_offset(std::uint64_t block) const;
    // Decoded steps of one 128-step block, served from the step cache.
    [[nodiscard]] const std::vector<StepRecord>& cached_step_block(std::uint64_t block) const;

    void decode_node_postings(
        std::uint32_t node_id,
        detail::PostingBlockCache& cache,
        const std::function<void(std::uint32_t path_id, std::uint32_t step_rank)>& callback) const;

    void load_header_and_paths();
    [[nodiscard]] std::string read_string(std::uint64_t offset, std::uint64_t len) const;
    void promote_node_name_cache() const;
    void read_exact(std::uint64_t offset, void* dst, std::size_t bytes) const;
    void check_section(std::uint64_t offset, std::uint64_t bytes) const;

    std::string index_path_;
    int fd_{-1};
    std::uint64_t file_size_{};
    std::uint32_t format_version_{};
    StepEncoding step_encoding_{StepEncoding::packed32};
    PostingEncoding posting_encoding_{PostingEncoding::path_groups};
    std::uint32_t posting_block_nodes_{};
    std::uint64_t node_id_space_{};
    std::uint64_t node_table_offset_{};
    std::uint64_t step_table_offset_{};
    std::uint64_t step_data_bytes_{};
    std::uint64_t step_index_offset_{};
    std::uint64_t step_superblock_count_{};
    std::uint64_t posting_table_offset_{};
    std::uint64_t strings_offset_{};
    std::uint64_t posting_table_bytes_{};
    std::uint64_t total_step_count_{};
    std::uint32_t node_count_{};
    std::vector<PathMeta> paths_;
    std::unordered_map<std::string, std::uint32_t> path_name_to_id_;
    mutable std::unordered_map<std::uint32_t, std::string> node_name_cache_;
    // Large scans promote the sparse hash cache into a rank-to-dense-name
    // lookup. The deque keeps returned string views stable as more names are
    // loaded, while the uint32 rank table avoids one string object per graph
    // node and is not allocated for ordinary small queries.
    mutable std::vector<std::uint32_t> node_name_rank_to_dense_;
    mutable std::deque<std::string> dense_node_names_;
    mutable std::unique_ptr<detail::PostingBlockCache> posting_cache_;
    // Short step reads (one subpath run each) often hit the same few blocks:
    // node-set queries in repeats yield millions of runs a few steps apart.
    mutable std::unique_ptr<detail::StepBlockCache> step_cache_;
};

// Immutable rank-to-name lookup over names already owned by extraction. Small
// selections use a sparse map; large selections use one direct rank table.
// Keeping only name indexes here avoids rereading or duplicating node strings,
// and all lookup state is read-only while formatter threads are active.
class SelectedNodeNameLookup {
public:
    SelectedNodeNameLookup(std::uint32_t node_count,
                           const std::vector<std::uint32_t>& node_ids,
                           const std::vector<std::string>& node_names);

    [[nodiscard]] std::string_view get_node_name(std::uint32_t node_id) const {
        if (node_id >= node_count_) {
            throw std::runtime_error("Node id out of range");
        }

        std::uint32_t name_index = std::numeric_limits<std::uint32_t>::max();
        if (!rank_to_name_.empty()) {
            name_index = rank_to_name_[node_id];
        } else {
            const auto it = sparse_rank_to_name_.find(node_id);
            if (it != sparse_rank_to_name_.end()) name_index = it->second;
        }
        if (name_index == std::numeric_limits<std::uint32_t>::max()) {
            throw std::runtime_error(
                "Selected node-name lookup is missing a selected node");
        }
        return (*node_names_)[name_index];
    }

private:
    std::uint32_t node_count_{};
    const std::vector<std::string>* node_names_{};
    std::unordered_map<std::uint32_t, std::uint32_t> sparse_rank_to_name_;
    std::vector<std::uint32_t> rank_to_name_;
};

// Resolve a node set into all path runs that remain contiguous inside that set.
// By default, runs shorter than two consecutive steps are suppressed to avoid
// emitting single-node path fragments that tend to confuse downstream users
// and tools. keep_single_steps retains them, which matches gbz-base's subgraph
// output where every haplotype visit inside the subgraph becomes a walk.
std::vector<SubpathRun> find_subpaths_for_node_ids(const PathIndexReader& index,
                                                   const std::vector<std::uint32_t>& node_ids,
                                                   bool keep_single_steps = false);

void write_path_as_gfa_line(std::ostream& out,
                            const PathIndexReader& index,
                            std::uint32_t path_id);

void write_subpath_as_gfa_line(std::ostream& out,
                               const PathIndexReader& index,
                               std::uint32_t path_id,
                               std::uint64_t start_step,
                               std::uint64_t step_count,
                               std::string_view output_name);

// Format one subpath into an owned buffer. Step reads and node-name lookup are
// explicit so callers can use either a reader cache or the selected-name table.
std::string format_subpath_as_gfa_line(
    const PathIndexReader& step_index,
    const PathIndexReader& node_name_index,
    std::uint32_t path_id,
    std::uint64_t start_step,
    std::uint64_t step_count,
    std::string_view output_name);

// Parallel and optimized serial extraction use names already owned by the
// selected-node vector instead of building a second PathIndexReader cache.
std::string format_subpath_as_gfa_line(
    const PathIndexReader& step_index,
    const SelectedNodeNameLookup& node_name_index,
    std::uint32_t path_id,
    std::uint64_t start_step,
    std::uint64_t step_count,
    std::string_view output_name);

}  // namespace gfaidx::paths

#endif  // GFAIDX_PATH_INDEX_H

#include "paths/path_index.h"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <optional>
#include <queue>
#include <stdexcept>
#include <system_error>
#include <utility>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "fs/fs_helpers.h"
#include "fs/gfa_line_parsers.h"
#include "indexer/node_hash_index.h"
#include "paths/pdx_codec.h"
#include "utils/Timer.h"

namespace gfaidx::paths {
namespace {

// The path index is a single binary file with:
// - a fixed header (v5 appends PathIndexHeaderV5Disk)
// - a path table
// - a node table
// - the step table (v4: flat packed uint32 array; v5: packed32 or
//   delta-varint blocks followed by their superblock index)
// - a per-node compressed posting blob
// - one shared string blob for names / overlap fields / tags
constexpr char kPathIndexMagic[8] = {'G', 'F', 'P', 'A', 'T', 'H', '1', '\0'};
constexpr std::uint32_t kPathIndexVersionLegacy = 4;
constexpr std::uint32_t kPathIndexVersion = 5;

// Each step is stored in one packed uint32:
// - low 31 bits: node_id
// - top bit: reverse-orientation flag
constexpr std::uint32_t kStepPackedNodeMask = 0x7fffffffu;
constexpr std::uint32_t kStepPackedReverseBit = 0x80000000u;

// File header with counts plus offsets to each top-level section.
struct PathIndexHeaderDisk {
    char magic[8]{};
    std::uint32_t version{};
    std::uint32_t reserved{};
    std::uint64_t path_count{};
    std::uint64_t node_count{};
    std::uint64_t step_count{};
    std::uint64_t posting_count{};
    std::uint64_t path_table_offset{};
    std::uint64_t node_table_offset{};
    std::uint64_t step_table_offset{};
    std::uint64_t posting_table_offset{};
    std::uint64_t strings_offset{};
    std::uint64_t strings_size{};
};

// v5 header extension, stored right after PathIndexHeaderDisk.
struct PathIndexHeaderV5Disk {
    // Fingerprint of the .ndx numbering used for node ids (0 = legacy ranks).
    std::uint64_t node_id_space{};
    std::uint32_t step_encoding{};
    std::uint32_t posting_encoding{};
    // Delta-varint only: encoded step bytes, then the superblock table.
    std::uint64_t step_data_bytes{};
    std::uint64_t step_index_offset{};
    std::uint64_t step_superblock_count{};
    std::uint32_t step_block_steps{};
    std::uint32_t step_superblock_blocks{};
    // path_slots only: nodes per independently decodable posting block.
    std::uint32_t posting_block_nodes{};
    std::uint32_t reserved32{};
    std::uint64_t reserved{};
};

// Path metadata supporting both P-lines and W-lines.
struct PathRecordDisk {
    char record_type{};
    char reserved[7]{};
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

// Node metadata points at the node's slice in the posting table.
// posting_begin is a byte offset into
// the compressed posting blob". posting_count remains the decoded posting
// count so callers can still reason about how many occurrences the node has.
struct NodeRecordDisk {
    std::uint64_t name_offset{};
    std::uint64_t name_len{};
    std::uint64_t posting_begin{};
    std::uint64_t posting_count{};
};

// Packed step layout. This cuts the step table from 8 bytes/step to 4
// bytes/step while preserving direct random access by step index.
struct StepRecordDisk {
    std::uint32_t packed{};
};

// Parsed views into one P line before we materialize it into the binary index.
struct ParsedPathFields {
    std::string name;
    std::string_view segments;
    std::string_view overlaps;
    std::string_view tags;
};

struct ParsedWalkFields {
    std::string sample_id;
    std::uint64_t hap_index{};
    std::string seq_id;
    std::int64_t seq_start{-1};
    std::int64_t seq_end{-1};
    std::string_view walk;
    std::string_view tags;
};

// Temporary in-memory metadata used while building the final path table.
struct PathBuildEntry {
    char record_type{'P'};
    std::string name;
    std::uint64_t step_begin{};
    std::uint64_t step_count{};
    std::string overlaps;
    std::string tags;
    std::string sample_id;
    std::uint64_t hap_index{};
    std::string seq_id;
    std::int64_t seq_start{-1};
    std::int64_t seq_end{-1};
};

static_assert(sizeof(PathIndexHeaderDisk) == 96, "Unexpected path index header size");
static_assert(sizeof(PathIndexHeaderV5Disk) == 64, "Unexpected v5 path index header size");
static_assert(sizeof(PathRecordDisk) == 128, "Unexpected path record size");
static_assert(sizeof(NodeRecordDisk) == 32, "Unexpected node record size");
static_assert(sizeof(StepRecordDisk) == 4, "Unexpected packed step record size");

// Bound the posting sort working set so large path collections spill to disk
// instead of accumulating one giant in-memory postings vector.
constexpr std::size_t kPostingChunkBytes = 64ULL * 1024ULL * 1024ULL;
constexpr std::size_t kPostingChunkRecords =
    kPostingChunkBytes / sizeof(detail::TempPosting) == 0
        ? 1
        : kPostingChunkBytes / sizeof(detail::TempPosting);

constexpr std::uint64_t kPathRecordProgressInterval = 5000;
constexpr std::uint64_t kPostingRunProgressInterval = 50;
constexpr std::uint64_t kPostingMergeProgressInterval = 100000000;

// Keep the k-way merge comfortably below typical open-file limits by
// collapsing large run sets in multiple passes when needed.
constexpr std::size_t kPostingMergeFanIn = 128;

constexpr std::size_t kFileCopyBufferBytes = 1ULL << 20;

// Hash lookup is cheaper for small node sets; rank-addressed lookup wins once
// coordinate output repeatedly visits a substantial number of distinct nodes.
constexpr std::size_t kDenseNodeNamePromotionThreshold = 1ULL << 16;

using detail::PostingHeapGreater;
using detail::PostingHeapItem;
using detail::PostingRunBuilder;
using detail::PostingRunCursor;
using detail::TempPosting;

using codec::append_varint;

bool test_seen_bit(const std::vector<std::uint64_t>& bits, std::uint32_t value) {
    const std::size_t word = value / 64;
    const unsigned bit = value % 64;
    return (bits[word] & (1ULL << bit)) != 0;
}

void set_seen_bit(std::vector<std::uint64_t>& bits, std::uint32_t value) {
    const std::size_t word = value / 64;
    const unsigned bit = value % 64;
    bits[word] |= (1ULL << bit);
}

std::uint64_t append_string(std::string& blob, std::string_view s) {
    const auto offset = static_cast<std::uint64_t>(blob.size());
    blob.append(s.data(), s.size());
    return offset;
}

template <typename T>
void write_vector(std::ofstream& out, const std::vector<T>& values) {
    if (values.empty()) return;
    out.write(reinterpret_cast<const char*>(values.data()),
              static_cast<std::streamsize>(values.size() * sizeof(T)));
}

template <typename T>
void write_binary_record(std::ofstream& out, const T& value) {
    out.write(reinterpret_cast<const char*>(&value), static_cast<std::streamsize>(sizeof(T)));
}

bool temp_posting_less(const TempPosting& lhs, const TempPosting& rhs) {
    if (lhs.node_id != rhs.node_id) return lhs.node_id < rhs.node_id;
    if (lhs.path_id != rhs.path_id) return lhs.path_id < rhs.path_id;
    return lhs.step_rank < rhs.step_rank;
}

StepRecordDisk pack_step_record(std::uint32_t node_id, bool is_reverse) {
    if (node_id > kStepPackedNodeMask) {
        throw std::runtime_error("Node id too large for packed step encoding");
    }

    StepRecordDisk out{};
    out.packed = node_id | (is_reverse ? kStepPackedReverseBit : 0u);
    return out;
}

StepRecord unpack_step_record(const StepRecordDisk& rec) {
    return StepRecord{
        rec.packed & kStepPackedNodeMask,
        (rec.packed & kStepPackedReverseBit) != 0
    };
}

// Streams path steps into the temporary step-table files in global step
// order, in either the packed32 or the delta-varint layout.
class StepTableWriter {
public:
    StepTableWriter(StepEncoding encoding,
                    const std::string& data_path,
                    const std::string& index_path)
        : encoding_(encoding),
          data_out_(data_path, std::ios::binary | std::ios::trunc) {
        if (!data_out_) {
            throw std::runtime_error("Failed to open temporary step file: " + data_path);
        }
        if (encoding_ == StepEncoding::delta_varint) {
            index_out_.open(index_path, std::ios::binary | std::ios::trunc);
            if (!index_out_) {
                throw std::runtime_error("Failed to open temporary step index file: " + index_path);
            }
            delta_.emplace(data_out_, index_out_);
        }
    }

    void add(std::uint32_t node_id, bool is_reverse) {
        if (delta_) {
            if (node_id > kStepPackedNodeMask) {
                throw std::runtime_error("Node id too large for step encoding");
            }
            delta_->add(node_id, is_reverse);
            return;
        }
        const auto step = pack_step_record(node_id, is_reverse);
        write_binary_record(data_out_, step);
        if (!data_out_.good()) {
            throw std::runtime_error("Failed while writing temporary step file");
        }
    }

    void finish() {
        if (delta_) delta_->finish();
        data_out_.close();
        if (!data_out_) throw std::runtime_error("Failed while writing temporary step file");
        if (index_out_.is_open()) {
            index_out_.close();
            if (!index_out_) throw std::runtime_error("Failed while writing temporary step index file");
        }
    }

    [[nodiscard]] std::uint64_t data_bytes(std::uint64_t step_count) const {
        return delta_ ? delta_->data_bytes() : step_count * sizeof(StepRecordDisk);
    }
    [[nodiscard]] std::uint64_t superblock_count() const {
        return delta_ ? delta_->superblock_count() : 0;
    }

private:
    StepEncoding encoding_;
    std::ofstream data_out_;
    std::ofstream index_out_;
    std::optional<codec::DeltaStepWriter> delta_;
};

// Encode one node's postings as path-grouped varints:
// - delta(path_id) from the previous path group
// - count of occurrences for this path
// - first step rank in that path
// - delta(step_rank) for each later occurrence in the same path
//
// This keeps random access at node granularity: the node table still points to
// one contiguous byte block per node, but that block is typically much smaller
// than the old fixed-width array of (path_id, step_rank) pairs.
void append_compressed_posting_block(std::string& out,
                                     const std::vector<TempPosting>& postings,
                                     std::size_t begin,
                                     std::size_t end) {
    std::uint32_t prev_path_id = 0;
    bool first_group = true;

    for (std::size_t cursor = begin; cursor < end;) {
        const std::uint32_t path_id = postings[cursor].path_id;
        std::size_t group_end = cursor + 1;
        while (group_end < end && postings[group_end].path_id == path_id) {
            ++group_end;
        }

        const std::uint64_t path_delta = first_group ? path_id
                                                     : static_cast<std::uint64_t>(path_id - prev_path_id);
        append_varint(out, path_delta);
        append_varint(out, group_end - cursor);
        append_varint(out, postings[cursor].step_rank);

        std::uint32_t prev_step_rank = postings[cursor].step_rank;
        for (std::size_t i = cursor + 1; i < group_end; ++i) {
            append_varint(out, static_cast<std::uint64_t>(postings[i].step_rank - prev_step_rank));
            prev_step_rank = postings[i].step_rank;
        }

        prev_path_id = path_id;
        first_group = false;
        cursor = group_end;
    }
}

void append_file_to_stream(std::ofstream& out, const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    if (!in) {
        throw std::runtime_error("Failed to open temporary file for final assembly: " + path);
    }

    std::vector<char> buffer(kFileCopyBufferBytes);
    while (in) {
        in.read(buffer.data(), static_cast<std::streamsize>(buffer.size()));
        const auto got = in.gcount();
        if (got <= 0) break;
        out.write(buffer.data(), got);
    }

    if (!in.eof()) {
        throw std::runtime_error("Failed while reading temporary file: " + path);
    }
    if (!out.good()) {
        throw std::runtime_error("Failed while writing final path index");
    }
}

void remove_paths_if_present(const std::vector<std::string>& paths) {
    for (const auto& path : paths) {
        std::error_code ec;
        std::filesystem::remove(path, ec);
    }
}

// Open every posting run in one bounded merge group so the heap can pull the
// next smallest posting from each run without loading whole files into memory.
std::vector<PostingRunCursor> open_posting_runs(const std::vector<std::string>& run_paths) {
    std::vector<PostingRunCursor> runs;
    runs.reserve(run_paths.size());
    for (const auto& path : run_paths) {
        runs.emplace_back(path);
    }
    return runs;
}

// Seed the heap with the first posting from each sorted run.
std::priority_queue<PostingHeapItem,
                    std::vector<PostingHeapItem>,
                    PostingHeapGreater> build_posting_heap(std::vector<PostingRunCursor>& runs) {
    std::priority_queue<PostingHeapItem,
                        std::vector<PostingHeapItem>,
                        PostingHeapGreater> heap;
    for (std::size_t i = 0; i < runs.size(); ++i) {
        if (runs[i].valid) {
            heap.push(PostingHeapItem{runs[i].current, i});
        }
    }
    return heap;
}

// Merge one bounded group of sorted posting runs into a larger sorted run.
void merge_sorted_runs_to_run_file(const std::vector<std::string>& run_paths,
                                   const std::string& output_path) {
    std::ofstream out(output_path, std::ios::binary | std::ios::trunc);
    if (!out) {
        throw std::runtime_error("Failed to open intermediate posting run file: " + output_path);
    }

    auto runs = open_posting_runs(run_paths);
    auto heap = build_posting_heap(runs);

    while (!heap.empty()) {
        const auto item = heap.top();
        heap.pop();

        write_binary_record(out, item.posting);
        if (!out.good()) {
            throw std::runtime_error("Failed while writing intermediate posting run file: " + output_path);
        }

        auto& run = runs[item.run_index];
        if (run.advance()) {
            heap.push(PostingHeapItem{run.current, item.run_index});
        }
    }
}

// Repeatedly merge posting runs in bounded groups until the final merge width
// fits under the configured fan-in cap.
std::vector<std::string> collapse_posting_runs(std::vector<std::string> run_paths,
                                               const std::string& temp_dir) {
    if (run_paths.size() <= kPostingMergeFanIn) {
        return run_paths;
    }

    std::cout << get_time() << ": Collapsing " << run_paths.size()
              << " posting runs with fan-in " << kPostingMergeFanIn << std::endl;

    std::size_t pass = 0;
    while (run_paths.size() > kPostingMergeFanIn) {
        std::vector<std::string> merged_paths;
        merged_paths.reserve((run_paths.size() + kPostingMergeFanIn - 1) / kPostingMergeFanIn);
        const std::size_t input_run_count = run_paths.size();
        const std::size_t total_groups = merged_paths.capacity();

        std::cout << get_time() << ": Merge pass " << pass
                  << ": " << input_run_count << " runs -> "
                  << total_groups << " merged runs" << std::endl;

        for (std::size_t begin = 0; begin < run_paths.size(); begin += kPostingMergeFanIn) {
            const std::size_t end = std::min(run_paths.size(), begin + kPostingMergeFanIn);
            std::vector<std::string> group(run_paths.begin() + static_cast<std::ptrdiff_t>(begin),
                                           run_paths.begin() + static_cast<std::ptrdiff_t>(end));

            const std::string merged_path =
                temp_dir + "/posting_merge_pass_" + std::to_string(pass) + "_" +
                std::to_string(merged_paths.size()) + ".bin";
            merge_sorted_runs_to_run_file(group, merged_path);
            merged_paths.push_back(merged_path);

            // Once a bounded-fan-in merge group has been materialized, its input
            // runs are no longer needed for later passes or final assembly.
            remove_paths_if_present(group);

            if (merged_paths.size() % 10 == 0 || merged_paths.size() == total_groups) {
                std::cout << get_time() << ": Merge pass " << pass
                          << " completed " << merged_paths.size()
                          << "/" << total_groups << " groups" << std::endl;
            }
        }

        run_paths.swap(merged_paths);
        ++pass;
    }

    return run_paths;
}

// Merge the final posting runs in node/path/step order and compress one node's
// postings at a time into the on-disk posting blob.
std::uint64_t build_compressed_posting_blob_from_runs(
    const std::vector<std::string>& run_paths,
    std::vector<NodeRecordDisk>& node_records,
    const std::string& output_path,
    const PathIndexEncoding& encoding) {

    std::ofstream out(output_path, std::ios::binary | std::ios::trunc);
    if (!out) {
        throw std::runtime_error("Failed to open temporary posting blob: " + output_path);
    }

    if (run_paths.empty()) {
        for (auto& rec : node_records) {
            rec.posting_begin = 0;
            rec.posting_count = 0;
        }
        return 0;
    }

    auto runs = open_posting_runs(run_paths);
    auto heap = build_posting_heap(runs);

    std::cout << get_time() << ": Final posting merge across "
              << run_paths.size() << " run files" << std::endl;

    std::vector<TempPosting> node_postings;
    std::vector<codec::Posting> slot_postings;
    std::optional<codec::PathSlotPostingEncoder> slot_encoder;
    if (encoding.postings == PostingEncoding::path_slots) {
        slot_encoder.emplace(encoding.posting_block_nodes);
    }
    std::string block;
    std::uint64_t current_offset = 0;
    std::size_t next_node_record = 0;
    std::uint32_t current_node_id = 0;
    bool have_current_node = false;
    std::uint64_t merged_postings = 0;

    auto flush_node = [&]() {
        if (!have_current_node) return;

        while (next_node_record < current_node_id) {
            node_records[next_node_record].posting_begin = current_offset;
            node_records[next_node_record].posting_count = 0;
            ++next_node_record;
        }

        block.clear();
        if (slot_encoder) {
            // The merge emits (node, path, rank) order, which is exactly the
            // per-node (path, rank) order the slot coder expects.
            slot_postings.clear();
            for (const auto& posting : node_postings) {
                slot_postings.push_back(codec::Posting{posting.path_id, posting.step_rank});
            }
            slot_encoder->encode_node(current_node_id, slot_postings.data(),
                                      slot_postings.data() + slot_postings.size(), block);
        } else {
            append_compressed_posting_block(block, node_postings, 0, node_postings.size());
        }

        node_records[current_node_id].posting_begin = current_offset;
        node_records[current_node_id].posting_count =
            static_cast<std::uint64_t>(node_postings.size());
        if (!block.empty()) {
            out.write(block.data(), static_cast<std::streamsize>(block.size()));
        }
        if (!out.good()) {
            throw std::runtime_error("Failed while writing temporary posting blob");
        }

        current_offset += block.size();
        ++next_node_record;
        node_postings.clear();
        have_current_node = false;
    };

    while (!heap.empty()) {
        const auto item = heap.top();
        heap.pop();

        if (!have_current_node) {
            current_node_id = item.posting.node_id;
            have_current_node = true;
        } else if (item.posting.node_id != current_node_id) {
            flush_node();
            current_node_id = item.posting.node_id;
            have_current_node = true;
        }

        node_postings.push_back(item.posting);
        ++merged_postings;
        if (merged_postings % kPostingMergeProgressInterval == 0) {
            std::cout << get_time() << ": Merged "
                      << merged_postings << " postings into node blocks" << std::endl;
        }

        auto& run = runs[item.run_index];
        if (run.advance()) {
            heap.push(PostingHeapItem{run.current, item.run_index});
        }
    }

    flush_node();

    std::cout << get_time() << ": Final posting merge wrote "
              << merged_postings << " postings into compressed node blocks" << std::endl;

    while (next_node_record < node_records.size()) {
        node_records[next_node_record].posting_begin = current_offset;
        node_records[next_node_record].posting_count = 0;
        ++next_node_record;
    }

    if (!out.good()) {
        throw std::runtime_error("Failed while finalizing temporary posting blob");
    }
    return current_offset;
}

ParsedPathFields parse_path_fields(std::string_view line) {
    const size_t t1 = line.find('\t');
    if (t1 == npos) offending_line(line);
    const size_t t2 = line.find('\t', t1 + 1);
    if (t2 == npos) offending_line(line);
    const size_t t3 = line.find('\t', t2 + 1);
    if (t3 == npos) offending_line(line);
    const size_t t4 = line.find('\t', t3 + 1);

    ParsedPathFields out;
    out.name = std::string(line.substr(t1 + 1, t2 - (t1 + 1)));
    out.segments = line.substr(t2 + 1, t3 - (t2 + 1));
    if (t4 == npos) {
        out.overlaps = line.substr(t3 + 1);
    } else {
        out.overlaps = line.substr(t3 + 1, t4 - (t3 + 1));
        out.tags = line.substr(t4 + 1);
    }
    return out;
}

std::int64_t parse_optional_int_field(std::string_view value) {
    if (value == "*") return -1;
    return std::stoll(std::string(value));
}

// W-lines do not have a single path-name field, so we synthesize a canonical
// key that exact lookup can use just like a normal path id.
std::string make_walk_key(const std::string& sample_id,
                          std::uint64_t hap_index,
                          const std::string& seq_id,
                          std::int64_t seq_start,
                          std::int64_t seq_end) {
    const auto start_text = (seq_start >= 0) ? std::to_string(seq_start) : std::string("*");
    const auto end_text = (seq_end >= 0) ? std::to_string(seq_end) : std::string("*");
    return sample_id + "|" + std::to_string(hap_index) + "|" + seq_id + "|" + start_text + "|" + end_text;
}

ParsedWalkFields parse_walk_fields(std::string_view line) {
    size_t tabs[6];
    size_t pos = 0;
    for (int i = 0; i < 6; ++i) {
        tabs[i] = line.find('\t', pos);
        if (tabs[i] == npos) offending_line(line);
        pos = tabs[i] + 1;
    }
    const size_t t7 = line.find('\t', tabs[5] + 1);

    ParsedWalkFields out;
    out.sample_id = std::string(line.substr(tabs[0] + 1, tabs[1] - (tabs[0] + 1)));
    out.hap_index = static_cast<std::uint64_t>(std::stoull(std::string(
        line.substr(tabs[1] + 1, tabs[2] - (tabs[1] + 1)))));
    out.seq_id = std::string(line.substr(tabs[2] + 1, tabs[3] - (tabs[2] + 1)));
    out.seq_start = parse_optional_int_field(line.substr(tabs[3] + 1, tabs[4] - (tabs[3] + 1)));
    out.seq_end = parse_optional_int_field(line.substr(tabs[4] + 1, tabs[5] - (tabs[4] + 1)));
    if (t7 == npos) {
        out.walk = line.substr(tabs[5] + 1);
    } else {
        out.walk = line.substr(tabs[5] + 1, t7 - (tabs[5] + 1));
        out.tags = line.substr(t7 + 1);
    }
    return out;
}

// We only need the node id field from S lines here, so this avoids the heavier
// generic S-line parsing path.
std::string extract_s_node_id(std::string_view line) {
    const size_t t1 = line.find('\t');
    if (t1 == npos) offending_line(line);
    const size_t t2 = line.find('\t', t1 + 1);
    if (t2 == npos) offending_line(line);
    return std::string(line.substr(t1 + 1, t2 - (t1 + 1)));
}

// Parse the step list of one P line and emit:
// - the path-first step records
// - the node-first postings used later for node-set queries
std::uint64_t parse_path_steps(
    std::string_view segments,
    const std::unordered_map<std::string, std::uint32_t>& node_to_id,
    std::uint32_t path_id,
    StepTableWriter& steps_out,
    PostingRunBuilder& posting_runs) {

    std::string node_name;
    std::uint32_t step_rank = 0;

    for (size_t pos = 0; pos < segments.size();) {
        const size_t comma = segments.find_first_of(",;", pos);
        const size_t end = (comma == npos) ? segments.size() : comma;
        const std::string_view token = segments.substr(pos, end - pos);
        if (token.size() < 2) {
            throw std::runtime_error("Malformed path step token");
        }

        const char orient = token.back();
        if (orient != '+' && orient != '-') {
            throw std::runtime_error("Malformed path step orientation");
        }

        node_name.assign(token.data(), token.size() - 1);
        const auto it = node_to_id.find(node_name);
        if (it == node_to_id.end()) {
            throw std::runtime_error("Path references unknown node id: " + node_name);
        }

        steps_out.add(it->second, orient == '-');
        posting_runs.add(it->second, path_id, step_rank);
        ++step_rank;

        if (comma == npos) break;
        pos = comma + 1;
    }

    return step_rank;
}

// Parse the oriented step sequence of one W line and emit packed steps plus
// raw postings for the external-sort pipeline.
std::uint64_t parse_walk_steps(
    std::string_view walk,
    const std::unordered_map<std::string, std::uint32_t>& node_to_id,
    std::uint32_t path_id,
    StepTableWriter& steps_out,
    PostingRunBuilder& posting_runs) {

    std::string node_name;
    std::uint32_t step_rank = 0;

    for (size_t pos = 0; pos < walk.size();) {
        const char orient = walk[pos];
        if (orient != '>' && orient != '<') {
            throw std::runtime_error("Malformed W walk orientation");
        }

        const size_t next = walk.find_first_of("><", pos + 1);
        const size_t end = (next == npos) ? walk.size() : next;
        if (end <= pos + 1) {
            throw std::runtime_error("Malformed W walk token");
        }

        node_name.assign(walk.data() + pos + 1, end - (pos + 1));
        const auto it = node_to_id.find(node_name);
        if (it == node_to_id.end()) {
            throw std::runtime_error("Walk references unknown node id: " + node_name);
        }

        steps_out.add(it->second, orient == '<');
        posting_runs.add(it->second, path_id, step_rank);
        ++step_rank;
        pos = end;
    }

    return step_rank;
}

// Reconstruct just the overlap slice for a subpath. A subpath with N steps has
// N-1 overlap tokens; GFA also permits '*' when per-edge overlaps are absent.
std::string make_overlap_slice(std::string_view raw,
                               std::uint64_t start_edge,
                               std::uint64_t edge_count) {
    if (edge_count == 0 || raw.empty() || raw == "*") {
        return "*";
    }

    std::string out;
    std::uint64_t token_index = 0;
    std::uint64_t copied = 0;
    size_t pos = 0;

    while (pos <= raw.size()) {
        const size_t comma = raw.find(',', pos);
        const size_t end = (comma == std::string_view::npos) ? raw.size() : comma;
        if (token_index >= start_edge && copied < edge_count) {
            if (!out.empty()) out.push_back(',');
            out.append(raw.substr(pos, end - pos));
            ++copied;
            if (copied == edge_count) {
                break;
            }
        }
        ++token_index;
        if (comma == std::string_view::npos) break;
        pos = comma + 1;
    }

    if (copied != edge_count) {
        throw std::runtime_error("Overlap field token count does not match requested subpath");
    }
    return out;
}

void write_p_segments(std::ostream& out,
                      const PathIndexReader& index,
                      const std::vector<StepRecord>& steps) {
    for (std::size_t i = 0; i < steps.size(); ++i) {
        if (i > 0) out << ',';
        out << index.get_node_name(steps[i].node_id);
        out << (steps[i].is_reverse ? '-' : '+');
    }
}

void write_w_segments(std::ostream& out,
                      const PathIndexReader& index,
                      const std::vector<StepRecord>& steps) {
    for (const auto& step : steps) {
        out << (step.is_reverse ? '<' : '>');
        out << index.get_node_name(step.node_id);
    }
}

}  // namespace

namespace detail {

PostingRunBuilder::PostingRunBuilder(std::string temp_dir, std::size_t max_records)
    : temp_dir_(std::move(temp_dir)),
      max_records_(std::max<std::size_t>(1, max_records)) {
    chunk_.reserve(max_records_);
}

void PostingRunBuilder::add(std::uint32_t node_id,
                            std::uint32_t path_id,
                            std::uint32_t step_rank) {
    chunk_.push_back(TempPosting{node_id, path_id, step_rank});
    ++total_postings_;
    if (chunk_.size() >= max_records_) {
        flush_run();
    }
}

void PostingRunBuilder::finish() {
    flush_run();
}

const std::vector<std::string>& PostingRunBuilder::run_paths() const {
    return run_paths_;
}

std::uint64_t PostingRunBuilder::total_postings() const {
    return total_postings_;
}

std::size_t PostingRunBuilder::run_count() const {
    return run_paths_.size();
}

void PostingRunBuilder::flush_run() {
    if (chunk_.empty()) return;

    std::sort(chunk_.begin(), chunk_.end(), temp_posting_less);

    const std::string run_path =
        temp_dir_ + "/posting_run_" + std::to_string(run_paths_.size()) + ".bin";
    std::ofstream out(run_path, std::ios::binary | std::ios::trunc);
    if (!out) {
        throw std::runtime_error("Failed to open posting run file: " + run_path);
    }
    write_vector(out, chunk_);
    if (!out.good()) {
        throw std::runtime_error("Failed while writing posting run file: " + run_path);
    }

    run_paths_.push_back(run_path);
    chunk_.clear();

    if (run_paths_.size() % kPostingRunProgressInterval == 0) {
        std::cout << get_time() << ": Spilled "
                  << run_paths_.size() << " posting runs covering "
                  << total_postings_ << " postings" << std::endl;
    }
}

PostingRunCursor::PostingRunCursor(const std::string& path)
    : in(path, std::ios::binary) {
    if (!in) {
        throw std::runtime_error("Failed to open posting run file: " + path);
    }
    advance();
}

bool PostingRunCursor::advance() {
    in.read(reinterpret_cast<char*>(&current), static_cast<std::streamsize>(sizeof(current)));
    if (in.gcount() == 0) {
        valid = false;
        return false;
    }
    if (in.gcount() != static_cast<std::streamsize>(sizeof(current))) {
        throw std::runtime_error("Posting run file ended in the middle of a record");
    }
    valid = true;
    return true;
}

bool PostingHeapGreater::operator()(const PostingHeapItem& lhs,
                                    const PostingHeapItem& rhs) const {
    return temp_posting_less(rhs.posting, lhs.posting);
}

}  // namespace detail

bool build_path_index(const std::string& input_gfa,
                      const std::string& output_index,
                      const std::string& node_index_path,
                      const Reader::Options& reader_options,
                      const std::string& tmp_base_dir,
                      bool keep_tmp,
                      const PathIndexEncoding& encoding) {
    Timer timer;
    // Stage the final .pdx beside its destination so failed builds never leave a truncated index behind.
    const std::string temp_output_index = make_temp_output_path(output_index);

    indexer::NodeHashIndex node_index(node_index_path);
    if (node_index.size() > kStepPackedNodeMask) {
        throw std::runtime_error("Node hash index is too large for packed step encoding");
    }
    // Legacy ids with both legacy encodings reproduce the v4 file exactly so
    // old binaries can still read it; anything else needs the v5 header.
    const bool write_legacy_v4 = node_index.is_legacy() &&
                                 encoding.steps == StepEncoding::packed32 &&
                                 encoding.postings == PostingEncoding::path_groups;

    std::string tmp_base = tmp_base_dir;
    if (tmp_base.empty()) {
        const std::filesystem::path output_path(output_index);
        const auto parent = output_path.parent_path();
        tmp_base = parent.empty() ? std::string("") : parent.string();
    }

    const std::string tmp_dir = create_temp_dir(tmp_base,
                                                "gfaidx_paths_tmp_",
                                                "latest_paths",
                                                false);
    const std::string tmp_steps_path = tmp_dir + "/tmp_steps.bin";
    const std::string tmp_step_index_path = tmp_dir + "/tmp_step_index.bin";
    const std::string tmp_posting_blob_path = tmp_dir + "/tmp_posting_blob.bin";

    if (keep_tmp) {
        std::cout << get_time() << ": Using path-index temp directory " << tmp_dir << std::endl;
    }

    auto cleanup_tmp = [&]() {
        if (keep_tmp) return;
        // Remove the large working directory unless the caller explicitly asked to inspect it.
        std::error_code ec;
        std::filesystem::remove_all(tmp_dir, ec);
    };
    auto cleanup_output = [&]() {
        // Remove any staged .pdx file left behind by an interrupted or failed write.
        remove_path_if_exists(temp_output_index);
    };

    try {
        std::unordered_map<std::string, std::uint32_t> node_to_id;
        node_to_id.reserve(static_cast<std::size_t>(node_index.size()));
        std::vector<NodeRecordDisk> node_records(static_cast<std::size_t>(node_index.size()));
        std::vector<std::uint64_t> seen_nodes((static_cast<std::size_t>(node_index.size()) + 63) / 64, 0);
        std::uint64_t seen_node_count = 0;
        std::string strings_blob;
        strings_blob.reserve(1024);

        {
            // Pass 1: resolve each S-line node through the prebuilt .ndx file.
            // This still keeps the node-name lookup table in RAM, but it avoids
            // carrying the much larger step/posting tables in memory later on.
            std::string_view line;
            Reader reader(reader_options);
            if (!reader.open(input_gfa)) {
                throw std::runtime_error("Could not open file: " + input_gfa);
            }

            std::cout << get_time() << ": Scanning S lines for node ids" << std::endl;
            while (reader.read_line(line)) {
                if (line.empty() || line[0] != 'S') continue;
                auto node_id = extract_s_node_id(line);

                std::uint32_t int_id = 0;
                if (!node_index.lookup_rank(node_id, int_id)) {
                    throw std::runtime_error("S line references a node missing from .ndx: " + node_id);
                }
                if (test_seen_bit(seen_nodes, int_id)) {
                    throw std::runtime_error("Duplicate S line or .ndx collision detected for node: " + node_id);
                }

                set_seen_bit(seen_nodes, int_id);
                ++seen_node_count;
                node_to_id.emplace(node_id, int_id);

                auto& rec = node_records[int_id];
                rec.name_offset = append_string(strings_blob, node_id);
                rec.name_len = node_id.size();
            }

            if (seen_node_count != node_index.size()) {
                throw std::runtime_error("The input GFA and .ndx do not contain the same node set");
            }
        }

        std::vector<std::uint64_t>().swap(seen_nodes);

        std::vector<PathBuildEntry> paths;
        std::uint64_t total_steps = 0;
        StepTableWriter steps_out(encoding.steps, tmp_steps_path, tmp_step_index_path);
        PostingRunBuilder posting_runs(tmp_dir, kPostingChunkRecords);

        {
            // Pass 2: write packed steps straight to a temp file and spill
            // postings into sorted runs once the chunk buffer reaches its
            // memory budget.
            std::string_view line;
            Reader reader(reader_options);
            if (!reader.open(input_gfa)) {
                throw std::runtime_error("Could not open file: " + input_gfa);
            }

            std::cout << get_time() << ": Scanning P/W lines for path steps" << std::endl;
            while (reader.read_line(line)) {
                if (line.empty() || (line[0] != 'P' && line[0] != 'W')) continue;

                const auto path_id = static_cast<std::uint32_t>(paths.size());
                PathBuildEntry entry;
                entry.step_begin = total_steps;

                if (line[0] == 'P') {
                    ParsedPathFields parsed = parse_path_fields(line);
                    entry.record_type = 'P';
                    entry.name = std::move(parsed.name);
                    entry.overlaps = std::string(parsed.overlaps);
                    entry.tags = std::string(parsed.tags);
                    entry.step_count = parse_path_steps(parsed.segments,
                                                        node_to_id,
                                                        path_id,
                                                        steps_out,
                                                        posting_runs);
                } else {
                    ParsedWalkFields parsed = parse_walk_fields(line);
                    entry.record_type = 'W';
                    entry.sample_id = std::move(parsed.sample_id);
                    entry.hap_index = parsed.hap_index;
                    entry.seq_id = std::move(parsed.seq_id);
                    entry.seq_start = parsed.seq_start;
                    entry.seq_end = parsed.seq_end;
                    entry.name = make_walk_key(entry.sample_id,
                                               entry.hap_index,
                                               entry.seq_id,
                                               entry.seq_start,
                                               entry.seq_end);
                    entry.tags = std::string(parsed.tags);
                    entry.step_count = parse_walk_steps(parsed.walk,
                                                        node_to_id,
                                                        path_id,
                                                        steps_out,
                                                        posting_runs);
                }

                total_steps += entry.step_count;
                paths.push_back(std::move(entry));
                if (paths.size() % kPathRecordProgressInterval == 0) {
                    std::cout << get_time() << ": Parsed "
                              << paths.size() << " P/W records covering "
                              << total_steps << " steps and "
                              << posting_runs.run_count() << " completed posting runs"
                              << std::endl;
                }
            }
        }

        steps_out.finish();
        posting_runs.finish();

        std::cout << get_time() << ": Finished scanning " << paths.size()
                  << " P/W records covering " << total_steps
                  << " steps; spilled " << posting_runs.run_count()
                  << " posting runs" << std::endl;

        std::unordered_map<std::string, std::uint32_t>().swap(node_to_id);

        std::vector<PathRecordDisk> path_records(paths.size());
        for (std::size_t i = 0; i < paths.size(); ++i) {
            auto& dst = path_records[i];
            const auto& src = paths[i];
            dst.record_type = src.record_type;
            dst.name_offset = append_string(strings_blob, src.name);
            dst.name_len = src.name.size();
            dst.step_begin = src.step_begin;
            dst.step_count = src.step_count;
            dst.overlap_offset = append_string(strings_blob, src.overlaps);
            dst.overlap_len = src.overlaps.size();
            dst.tags_offset = append_string(strings_blob, src.tags);
            dst.tags_len = src.tags.size();
            dst.sample_offset = append_string(strings_blob, src.sample_id);
            dst.sample_len = src.sample_id.size();
            dst.hap_index = src.hap_index;
            dst.seq_id_offset = append_string(strings_blob, src.seq_id);
            dst.seq_id_len = src.seq_id.size();
            dst.seq_start = src.seq_start;
            dst.seq_end = src.seq_end;
        }

        std::vector<PathBuildEntry>().swap(paths);

        std::cout << get_time() << ": Building per-node path postings" << std::endl;
        // Cap the final merge width so very large graphs do not require one
        // open file handle per spilled posting run.
        auto final_run_paths = collapse_posting_runs(posting_runs.run_paths(), tmp_dir);
        const std::uint64_t posting_blob_bytes =
            build_compressed_posting_blob_from_runs(final_run_paths,
                                                    node_records,
                                                    tmp_posting_blob_path,
                                                    encoding);
        const std::uint64_t posting_count = posting_runs.total_postings();
        remove_paths_if_present(final_run_paths);

        PathIndexHeaderDisk header{};
        std::memcpy(header.magic, kPathIndexMagic, sizeof(kPathIndexMagic));
        header.version = write_legacy_v4 ? kPathIndexVersionLegacy : kPathIndexVersion;
        header.path_count = path_records.size();
        header.node_count = node_records.size();
        header.step_count = total_steps;
        header.posting_count = posting_count;

        PathIndexHeaderV5Disk header_v5{};
        header_v5.node_id_space = node_index.node_id_space();
        header_v5.step_encoding = static_cast<std::uint32_t>(encoding.steps);
        header_v5.posting_encoding = static_cast<std::uint32_t>(encoding.postings);
        if (encoding.postings == PostingEncoding::path_slots) {
            header_v5.posting_block_nodes = encoding.posting_block_nodes;
        }
        header_v5.step_data_bytes = steps_out.data_bytes(total_steps);
        header_v5.step_superblock_count = steps_out.superblock_count();
        if (encoding.steps == StepEncoding::delta_varint) {
            header_v5.step_block_steps = static_cast<std::uint32_t>(codec::kStepBlockSteps);
            header_v5.step_superblock_blocks = static_cast<std::uint32_t>(codec::kStepSuperblockBlocks);
        }
        // Keep the superblock table 8-byte aligned so readers can use it in place.
        const std::uint64_t step_index_bytes =
            header_v5.step_superblock_count * sizeof(codec::StepSuperblockDisk);
        const std::uint64_t step_padding =
            step_index_bytes == 0 ? 0 : (8 - header_v5.step_data_bytes % 8) % 8;

        header.path_table_offset = sizeof(PathIndexHeaderDisk) +
            (write_legacy_v4 ? 0 : sizeof(PathIndexHeaderV5Disk));
        header.node_table_offset = header.path_table_offset + path_records.size() * sizeof(PathRecordDisk);
        header.step_table_offset = header.node_table_offset + node_records.size() * sizeof(NodeRecordDisk);
        header_v5.step_index_offset = header.step_table_offset + header_v5.step_data_bytes + step_padding;
        header.posting_table_offset = header_v5.step_index_offset + step_index_bytes;
        header.strings_offset = header.posting_table_offset + posting_blob_bytes;
        header.strings_size = strings_blob.size();

        // Assemble the final binary index into the staged sibling file first.
        std::ofstream out(temp_output_index, std::ios::binary | std::ios::trunc);
        if (!out) {
            throw std::runtime_error("Failed to open output path index: " + temp_output_index);
        }

        out.write(reinterpret_cast<const char*>(&header), sizeof(header));
        if (!write_legacy_v4) {
            out.write(reinterpret_cast<const char*>(&header_v5), sizeof(header_v5));
        }
        write_vector(out, path_records);
        write_vector(out, node_records);
        append_file_to_stream(out, tmp_steps_path);
        if (step_index_bytes > 0) {
            const char zeros[8] = {};
            out.write(zeros, static_cast<std::streamsize>(step_padding));
            append_file_to_stream(out, tmp_step_index_path);
        }
        append_file_to_stream(out, tmp_posting_blob_path);
        if (!strings_blob.empty()) {
            out.write(strings_blob.data(), static_cast<std::streamsize>(strings_blob.size()));
        }

        if (!out.good()) {
            throw std::runtime_error("Failed while writing path index: " + output_index);
        }
        // Force close before publish so buffered write failures cannot slip past the rename boundary.
        out.close();
        if (!out) {
            throw std::runtime_error("Failed while finalizing path index: " + output_index);
        }

        // Publish the fully written staged index into its final path in one rename step.
        rename_path_or_throw(temp_output_index, output_index);

        cleanup_tmp();
        std::cout << get_time() << ": Indexed " << path_records.size() << " paths, "
                  << node_records.size() << " nodes, "
                  << total_steps << " path steps in " << timer.elapsed() << " seconds" << std::endl;
        return true;
    } catch (...) {
        // Clean up both the staged output and the working directory on any failure.
        cleanup_output();
        cleanup_tmp();
        throw;
    }
}

PathIndexReader::PathIndexReader(const std::string& index_path)
    : index_path_(index_path) {
    // All reads use pread on one descriptor: no shared stream position, and
    // unlike a mapping, page-cache pages never count toward process RSS.
    fd_ = ::open(index_path.c_str(), O_RDONLY);
    if (fd_ == -1) {
        throw std::runtime_error("Failed to open path index: " + index_path);
    }
    struct stat st{};
    if (fstat(fd_, &st) == -1) {
        ::close(fd_);
        fd_ = -1;
        throw std::runtime_error("Failed to stat path index: " + index_path);
    }
    file_size_ = static_cast<std::uint64_t>(st.st_size);
    try {
        load_header_and_paths();
    } catch (...) {
        ::close(fd_);
        fd_ = -1;
        throw;
    }
}

PathIndexReader::~PathIndexReader() {
    if (fd_ != -1) {
        ::close(fd_);
        fd_ = -1;
    }
}

void PathIndexReader::load_header_and_paths() {
    if (file_size_ < sizeof(PathIndexHeaderDisk)) {
        throw std::runtime_error("Failed to read path index header: " + index_path_);
    }
    PathIndexHeaderDisk header{};
    read_exact(0, &header, sizeof(header));

    if (std::memcmp(header.magic, kPathIndexMagic, sizeof(kPathIndexMagic)) != 0) {
        throw std::runtime_error("Invalid path index magic: " + index_path_);
    }
    if (header.version != kPathIndexVersion && header.version != kPathIndexVersionLegacy) {
        throw std::runtime_error("Unsupported path index version: " + std::to_string(header.version));
    }
    if (header.node_count > kStepPackedNodeMask) {
        throw std::runtime_error("Path index node count exceeds packed node id range");
    }
    format_version_ = header.version;

    PathIndexHeaderV5Disk header_v5{};
    if (header.version == kPathIndexVersion) {
        read_exact(sizeof(header), &header_v5, sizeof(header_v5));
        if (header_v5.step_encoding > static_cast<std::uint32_t>(StepEncoding::delta_varint)) {
            throw std::runtime_error("Unsupported .pdx step encoding: " +
                                     std::to_string(header_v5.step_encoding));
        }
        if (header_v5.posting_encoding > static_cast<std::uint32_t>(PostingEncoding::path_slots)) {
            throw std::runtime_error("Unsupported .pdx posting encoding: " +
                                     std::to_string(header_v5.posting_encoding));
        }
        step_encoding_ = static_cast<StepEncoding>(header_v5.step_encoding);
        posting_encoding_ = static_cast<PostingEncoding>(header_v5.posting_encoding);
        node_id_space_ = header_v5.node_id_space;
        if (posting_encoding_ == PostingEncoding::path_slots) {
            if (header_v5.posting_block_nodes == 0) {
                throw std::runtime_error("Invalid .pdx posting block size: " + index_path_);
            }
            posting_block_nodes_ = header_v5.posting_block_nodes;
        }
    }

    node_table_offset_ = header.node_table_offset;
    step_table_offset_ = header.step_table_offset;
    posting_table_offset_ = header.posting_table_offset;
    strings_offset_ = header.strings_offset;
    posting_table_bytes_ = header.strings_offset - header.posting_table_offset;
    total_step_count_ = header.step_count;
    node_count_ = static_cast<std::uint32_t>(header.node_count);

    if (step_encoding_ == StepEncoding::delta_varint) {
        const std::uint64_t expected_superblocks =
            (total_step_count_ + codec::kStepSuperblockSteps - 1) / codec::kStepSuperblockSteps;
        if (header_v5.step_block_steps != codec::kStepBlockSteps ||
            header_v5.step_superblock_blocks != codec::kStepSuperblockBlocks ||
            header_v5.step_superblock_count != expected_superblocks ||
            header_v5.step_index_offset % 8 != 0) {
            throw std::runtime_error("Unsupported .pdx step block layout: " + index_path_);
        }
        step_data_bytes_ = header_v5.step_data_bytes;
        step_index_offset_ = header_v5.step_index_offset;
        step_superblock_count_ = header_v5.step_superblock_count;
        check_section(step_table_offset_, step_data_bytes_);
        check_section(step_index_offset_, step_superblock_count_ * sizeof(codec::StepSuperblockDisk));
    } else {
        step_data_bytes_ = total_step_count_ * sizeof(StepRecordDisk);
        check_section(step_table_offset_, step_data_bytes_);
    }
    check_section(node_table_offset_, static_cast<std::uint64_t>(node_count_) * sizeof(NodeRecordDisk));
    if (header.strings_offset < header.posting_table_offset) {
        throw std::runtime_error("Path index sections are out of order: " + index_path_);
    }
    check_section(posting_table_offset_, posting_table_bytes_);
    check_section(strings_offset_, header.strings_size);

    // Load the small path metadata table eagerly. Node metadata and node names
    // stay on disk and are fetched lazily, which avoids a large fixed memory
    // cost when the graph has many millions of nodes.
    paths_.reserve(header.path_count);
    std::vector<PathRecordDisk> path_records(static_cast<std::size_t>(header.path_count));
    read_exact(header.path_table_offset,
               path_records.data(),
               path_records.size() * sizeof(PathRecordDisk));
    for (const auto& rec : path_records) {
        paths_.push_back(PathMeta{
            rec.record_type,
            read_string(rec.name_offset, rec.name_len),
            rec.step_begin,
            rec.step_count,
            read_string(rec.overlap_offset, rec.overlap_len),
            read_string(rec.tags_offset, rec.tags_len),
            read_string(rec.sample_offset, rec.sample_len),
            rec.hap_index,
            read_string(rec.seq_id_offset, rec.seq_id_len),
            rec.seq_start,
            rec.seq_end
        });
    }

    // Build name -> id maps once so repeated lookups stay cheap.
    for (std::uint32_t i = 0; i < path_count(); ++i) {
        path_name_to_id_.emplace(std::string(get_path_name(i)), i);
    }
}

bool PathIndexReader::lookup_path_id(const std::string& name, std::uint32_t& out_path_id) const {
    const auto it = path_name_to_id_.find(name);
    if (it == path_name_to_id_.end()) return false;
    out_path_id = it->second;
    return true;
}

PathInfo PathIndexReader::get_path_info(std::uint32_t path_id) const {
    if (path_id >= paths_.size()) {
        throw std::runtime_error("Path id out of range");
    }

    const auto& rec = paths_[path_id];
    return PathInfo{
        rec.record_type,
        path_id,
        rec.step_begin,
        rec.step_count,
        rec.name,
        rec.overlap_field,
        rec.tags,
        rec.sample_id,
        rec.hap_index,
        rec.seq_id,
        rec.seq_start,
        rec.seq_end
    };
}

std::string_view PathIndexReader::get_path_name(std::uint32_t path_id) const {
    if (path_id >= paths_.size()) {
        throw std::runtime_error("Path id out of range");
    }
    const auto& rec = paths_[path_id];
    return rec.name;
}

void PathIndexReader::promote_node_name_cache() const {
    const auto missing = std::numeric_limits<std::uint32_t>::max();
    node_name_rank_to_dense_.assign(node_count_, missing);
    for (auto& entry : node_name_cache_) {
        if (dense_node_names_.size() >= missing) {
            throw std::runtime_error("Dense node-name cache exceeds uint32 range");
        }
        const auto dense_id =
            static_cast<std::uint32_t>(dense_node_names_.size());
        node_name_rank_to_dense_[entry.first] = dense_id;
        dense_node_names_.push_back(std::move(entry.second));
    }

    // Release both hash nodes and buckets after moving their strings. Future
    // repeated lookups use one rank-table access and one deque access.
    std::unordered_map<std::uint32_t, std::string>().swap(node_name_cache_);
}

std::string_view PathIndexReader::get_node_name(std::uint32_t node_id) const {
    if (node_id >= node_count_) {
        throw std::runtime_error("Node id out of range");
    }

    if (!node_name_rank_to_dense_.empty()) {
        auto& dense_id = node_name_rank_to_dense_[node_id];
        const auto missing = std::numeric_limits<std::uint32_t>::max();
        if (dense_id == missing) {
            if (dense_node_names_.size() >= missing) {
                throw std::runtime_error(
                    "Dense node-name cache exceeds uint32 range");
            }
            dense_id = static_cast<std::uint32_t>(dense_node_names_.size());
            dense_node_names_.push_back(copy_node_name(node_id));
        }
        return dense_node_names_[dense_id];
    }

    const auto it = node_name_cache_.find(node_id);
    if (it != node_name_cache_.end()) {
        return it->second;
    }

    // Only the owned name is cached; later hits need nothing else from the
    // node record.
    auto [inserted_it, inserted] =
        node_name_cache_.emplace(node_id, copy_node_name(node_id));
    (void)inserted;
    if (node_name_cache_.size() == kDenseNodeNamePromotionThreshold) {
        promote_node_name_cache();
        return dense_node_names_[node_name_rank_to_dense_[node_id]];
    }
    return inserted_it->second;
}

std::string PathIndexReader::copy_node_name(std::uint32_t node_id) const {
    if (node_id >= node_count_) {
        throw std::runtime_error("Node id out of range");
    }

    // Bypass both lazy node caches because callers consume each name once and
    // keep their own selected-node representation.
    NodeRecordDisk rec{};
    const auto offset =
        node_table_offset_ +
        static_cast<std::uint64_t>(node_id) * sizeof(NodeRecordDisk);
    read_exact(offset, &rec, sizeof(rec));
    return read_string(rec.name_offset, rec.name_len);
}

std::string_view PathIndexReader::get_overlap_field(std::uint32_t path_id) const {
    if (path_id >= paths_.size()) {
        throw std::runtime_error("Path id out of range");
    }
    const auto& rec = paths_[path_id];
    return rec.overlap_field;
}

std::string_view PathIndexReader::get_tags(std::uint32_t path_id) const {
    if (path_id >= paths_.size()) {
        throw std::runtime_error("Path id out of range");
    }
    const auto& rec = paths_[path_id];
    return rec.tags;
}

void PathIndexReader::check_section(std::uint64_t offset, std::uint64_t bytes) const {
    if (offset > file_size_ || bytes > file_size_ - offset) {
        throw std::runtime_error("Path index section lies outside the file: " + index_path_);
    }
}

std::uint64_t PathIndexReader::step_block_offset(std::uint64_t block) const {
    const std::uint64_t superblock = block / codec::kStepSuperblockBlocks;
    if (superblock >= step_superblock_count_) return step_data_bytes_;
    codec::StepSuperblockDisk record{};
    read_exact(step_index_offset_ + superblock * sizeof(record), &record, sizeof(record));
    return record.base_offset + record.block_offsets[block % codec::kStepSuperblockBlocks];
}

template <typename Fn>
void PathIndexReader::visit_global_steps(std::uint64_t first, std::uint64_t count, Fn&& fn) const {
    if (count == 0) return;
    if (first > total_step_count_ || count > total_step_count_ - first) {
        throw std::runtime_error("Requested steps lie outside the .pdx step table");
    }

    // Long ranges (chromosome-scale prefix scans) are read in bounded chunks
    // so query memory stays independent of path length.
    constexpr std::uint64_t kChunkSteps = 1ULL << 20;

    if (step_encoding_ == StepEncoding::packed32) {
        // Steps are stored in one flat array, so a path slice is contiguous.
        std::vector<StepRecordDisk> raw(static_cast<std::size_t>(std::min(count, kChunkSteps)));
        for (std::uint64_t done = 0; done < count;) {
            const std::uint64_t take = std::min(count - done, kChunkSteps);
            read_exact(step_table_offset_ + (first + done) * sizeof(StepRecordDisk),
                       raw.data(), static_cast<std::size_t>(take * sizeof(StepRecordDisk)));
            for (std::uint64_t i = 0; i < take; ++i) {
                fn(unpack_step_record(raw[static_cast<std::size_t>(i)]));
            }
            done += take;
        }
        return;
    }

    // Delta-varint steps: locate the byte range of the covering 128-step
    // blocks through the superblock table, read it, and decode it in order,
    // skipping at most kStepBlockSteps - 1 values before `first`.
    constexpr std::uint64_t kChunkBlocks = kChunkSteps / codec::kStepBlockSteps;
    const std::uint64_t first_block = first / codec::kStepBlockSteps;
    const std::uint64_t end_block = (first + count - 1) / codec::kStepBlockSteps + 1;
    const std::uint64_t stop = first + count;
    std::vector<unsigned char> buffer;
    std::uint64_t begin_byte = step_block_offset(first_block);
    for (std::uint64_t block = first_block; block < end_block;) {
        const std::uint64_t chunk_end = std::min(end_block, block + kChunkBlocks);
        const std::uint64_t end_byte = step_block_offset(chunk_end);
        if (end_byte < begin_byte || end_byte > step_data_bytes_) {
            throw std::runtime_error("Corrupt .pdx step block index");
        }
        buffer.resize(static_cast<std::size_t>(end_byte - begin_byte));
        read_exact(step_table_offset_ + begin_byte, buffer.data(), buffer.size());
        const std::uint64_t chunk_first = std::max(first, block * codec::kStepBlockSteps);
        const std::uint64_t chunk_stop = std::min(stop, chunk_end * codec::kStepBlockSteps);
        codec::decode_delta_steps(buffer.data(),
                                  buffer.data() + buffer.size(),
                                  chunk_first,
                                  chunk_stop - chunk_first,
                                  node_count_,
                                  [&](std::uint32_t node_id, bool is_reverse) {
                                      fn(StepRecord{node_id, is_reverse});
                                  });
        begin_byte = end_byte;
        block = chunk_end;
    }
}

std::vector<StepRecord> PathIndexReader::read_steps(std::uint32_t path_id,
                                                    std::uint64_t start_step,
                                                    std::uint64_t max_steps) const {
    const auto info = get_path_info(path_id);
    if (start_step > info.step_count) {
        throw std::runtime_error("Requested start step beyond path length");
    }

    const std::uint64_t available = info.step_count - start_step;
    const std::uint64_t take = std::min(available, max_steps);
    std::vector<StepRecord> out;
    out.reserve(static_cast<std::size_t>(take));
    visit_global_steps(info.step_begin + start_step, take,
                       [&](const StepRecord& step) { out.push_back(step); });
    return out;
}

void PathIndexReader::for_each_step(
    std::uint32_t path_id,
    std::uint64_t start_step,
    std::uint64_t max_steps,
    const std::function<void(const StepRecord& step, std::uint64_t step_rank)>& callback) const {
    const auto info = get_path_info(path_id);
    if (start_step > info.step_count) {
        throw std::runtime_error("Requested start step beyond path length");
    }

    const std::uint64_t available = info.step_count - start_step;
    const std::uint64_t take = std::min(available, max_steps);
    std::uint64_t step_rank = start_step;
    visit_global_steps(info.step_begin + start_step, take,
                       [&](const StepRecord& step) { callback(step, step_rank++); });
}

namespace detail {

// Per-thread posting decode state. For path_slots postings it holds one
// block: its node records, encoded bytes, and the postings of the nodes
// decoded so far, so later lookups in the same block resume decoding instead
// of starting over.
struct PostingBlockCache {
    static constexpr std::uint64_t kNoBlock = std::numeric_limits<std::uint64_t>::max();

    std::uint64_t block{kNoBlock};
    std::uint32_t first_node{};
    std::vector<NodeRecordDisk> records;
    std::vector<unsigned char> bytes;
    std::size_t cursor{};
    std::vector<codec::Posting> postings;
    std::vector<std::uint32_t> node_starts;
    codec::PathSlotPostingDecoder decoder;
};

}  // namespace detail

namespace {

// Decode one node's v4 path-group posting block from [cursor, end).
void decode_path_group_postings(
    const unsigned char* cursor,
    const unsigned char* end,
    std::uint64_t posting_count,
    const std::function<void(std::uint32_t path_id, std::uint32_t step_rank)>& callback) {
    std::uint32_t current_path_id = 0;
    std::uint64_t emitted = 0;

    while (emitted < posting_count) {
        const auto path_delta = codec::read_varint(cursor, end);
        if (path_delta > std::numeric_limits<std::uint32_t>::max() - current_path_id) {
            throw std::runtime_error("Compressed posting block path id overflow");
        }
        current_path_id = static_cast<std::uint32_t>(current_path_id + path_delta);

        const auto group_count = codec::read_varint(cursor, end);
        if (group_count == 0) {
            throw std::runtime_error("Compressed posting block has an empty path group");
        }
        if (group_count > posting_count - emitted) {
            throw std::runtime_error("Compressed posting block overruns node posting count");
        }

        std::uint64_t current_step_rank = codec::read_varint(cursor, end);
        if (current_step_rank > std::numeric_limits<std::uint32_t>::max()) {
            throw std::runtime_error("Compressed posting block step rank overflow");
        }
        callback(current_path_id, static_cast<std::uint32_t>(current_step_rank));
        ++emitted;

        for (std::uint64_t i = 1; i < group_count; ++i) {
            const auto step_delta = codec::read_varint(cursor, end);
            if (step_delta == 0) {
                throw std::runtime_error("Compressed posting block step delta must be positive");
            }
            if (step_delta > std::numeric_limits<std::uint32_t>::max() - current_step_rank) {
                throw std::runtime_error("Compressed posting block step rank overflow");
            }
            current_step_rank += step_delta;
            callback(current_path_id, static_cast<std::uint32_t>(current_step_rank));
            ++emitted;
        }
    }

    if (cursor != end) {
        throw std::runtime_error("Compressed posting block has trailing bytes");
    }
}

}  // namespace

void PathIndexReader::decode_node_postings(
    std::uint32_t node_id,
    detail::PostingBlockCache& cache,
    const std::function<void(std::uint32_t path_id, std::uint32_t step_rank)>& callback) const {
    if (node_id >= node_count_) {
        throw std::runtime_error("Node id out of range");
    }

    if (posting_encoding_ == PostingEncoding::path_groups) {
        // One read covers this node's record and the next node's start.
        NodeRecordDisk records[2]{};
        const bool has_next = node_id + 1 < node_count_;
        read_exact(node_table_offset_ + static_cast<std::uint64_t>(node_id) * sizeof(NodeRecordDisk),
                   records, (has_next ? 2 : 1) * sizeof(NodeRecordDisk));
        const auto& node = records[0];
        if (node.posting_count == 0) return;
        const std::uint64_t block_end = has_next ? records[1].posting_begin : posting_table_bytes_;
        if (block_end < node.posting_begin || block_end > posting_table_bytes_) {
            throw std::runtime_error("Compressed posting block offsets are out of order");
        }
        cache.bytes.resize(static_cast<std::size_t>(block_end - node.posting_begin));
        read_exact(posting_table_offset_ + node.posting_begin, cache.bytes.data(), cache.bytes.size());
        decode_path_group_postings(cache.bytes.data(), cache.bytes.data() + cache.bytes.size(),
                                   node.posting_count, callback);
        return;
    }

    const std::uint64_t block = node_id / posting_block_nodes_;
    if (cache.block != block) {
        // Load the block's node records (plus the next block's first record
        // for the end offset) and its encoded bytes with two reads.
        const auto first = static_cast<std::uint32_t>(block * posting_block_nodes_);
        const auto last = static_cast<std::uint32_t>(
            std::min<std::uint64_t>(static_cast<std::uint64_t>(first) + posting_block_nodes_, node_count_));
        const std::size_t record_count = last - first + (last < node_count_ ? 1 : 0);
        cache.block = detail::PostingBlockCache::kNoBlock;
        cache.records.resize(record_count);
        read_exact(node_table_offset_ + static_cast<std::uint64_t>(first) * sizeof(NodeRecordDisk),
                   cache.records.data(), record_count * sizeof(NodeRecordDisk));
        const std::uint64_t begin = cache.records.front().posting_begin;
        const std::uint64_t end = last < node_count_ ? cache.records.back().posting_begin
                                                     : posting_table_bytes_;
        if (end < begin || end > posting_table_bytes_) {
            throw std::runtime_error("Compressed posting block offsets are out of order");
        }
        cache.bytes.resize(static_cast<std::size_t>(end - begin));
        read_exact(posting_table_offset_ + begin, cache.bytes.data(), cache.bytes.size());
        cache.cursor = 0;
        cache.postings.clear();
        cache.node_starts.assign(1, 0);
        cache.decoder.reset();
        cache.first_node = first;
        cache.block = block;
    }

    const std::uint32_t index = node_id - cache.first_node;
    const std::uint64_t block_begin = cache.records.front().posting_begin;
    while (cache.node_starts.size() <= index + 1) {
        const std::size_t i = cache.node_starts.size() - 1;
        const auto& record = cache.records[i];
        if (record.posting_begin - block_begin != cache.cursor) {
            cache.block = detail::PostingBlockCache::kNoBlock;
            throw std::runtime_error("Compressed posting block offsets do not match the node table");
        }
        if (record.posting_count > 0) {
            const unsigned char* p = cache.bytes.data() + cache.cursor;
            const std::size_t before = cache.postings.size();
            try {
                cache.decoder.decode_node(p, cache.bytes.data() + cache.bytes.size(),
                                          record.posting_count, path_count(), cache.postings);
            } catch (...) {
                cache.block = detail::PostingBlockCache::kNoBlock;
                throw;
            }
            if (cache.postings.size() - before != record.posting_count) {
                cache.block = detail::PostingBlockCache::kNoBlock;
                throw std::runtime_error("Compressed posting block decoded the wrong posting count");
            }
            cache.cursor = static_cast<std::size_t>(p - cache.bytes.data());
        }
        cache.node_starts.push_back(static_cast<std::uint32_t>(cache.postings.size()));
    }

    for (std::uint32_t i = cache.node_starts[index]; i < cache.node_starts[index + 1]; ++i) {
        callback(cache.postings[i].path_id, cache.postings[i].step_rank);
    }
}

void PathIndexReader::for_each_node_posting(
    std::uint32_t node_id,
    const std::function<void(std::uint32_t path_id, std::uint32_t step_rank)>& callback) const {
    if (!posting_cache_) posting_cache_ = std::make_unique<detail::PostingBlockCache>();
    decode_node_postings(node_id, *posting_cache_, callback);
}

PostingCursor::PostingCursor(const PathIndexReader& index)
    : index_(&index), cache_(std::make_unique<detail::PostingBlockCache>()) {}

PostingCursor::~PostingCursor() = default;
PostingCursor::PostingCursor(PostingCursor&&) noexcept = default;
PostingCursor& PostingCursor::operator=(PostingCursor&&) noexcept = default;

void PostingCursor::for_each_node_posting(
    std::uint32_t node_id,
    const std::function<void(std::uint32_t path_id, std::uint32_t step_rank)>& callback) {
    index_->decode_node_postings(node_id, *cache_, callback);
}

std::string PathIndexReader::read_string(std::uint64_t offset, std::uint64_t len) const {
    std::string out(static_cast<std::size_t>(len), '\0');
    if (len == 0) return out;
    read_exact(strings_offset_ + offset, out.data(), out.size());
    return out;
}

void PathIndexReader::read_exact(std::uint64_t offset, void* dst, std::size_t bytes) const {
    if (offset > file_size_ || bytes > file_size_ - offset) {
        throw std::runtime_error("Read beyond the end of path index: " + index_path_);
    }
    auto* out = static_cast<char*>(dst);
    while (bytes > 0) {
        const ssize_t got = ::pread(fd_, out, bytes, static_cast<off_t>(offset));
        if (got < 0) {
            if (errno == EINTR) continue;
            throw std::runtime_error("Failed to read bytes from path index: " + index_path_);
        }
        if (got == 0) {
            throw std::runtime_error("Unexpected end of path index: " + index_path_);
        }
        out += got;
        offset += static_cast<std::uint64_t>(got);
        bytes -= static_cast<std::size_t>(got);
    }
}

SelectedNodeNameLookup::SelectedNodeNameLookup(
    std::uint32_t node_count,
    const std::vector<std::uint32_t>& node_ids,
    const std::vector<std::string>& node_names)
    : node_count_(node_count), node_names_(&node_names) {
    if (node_ids.size() != node_names.size()) {
        throw std::runtime_error(
            "Selected node ranks and names have different counts");
    }
    if (node_names.size() > std::numeric_limits<std::uint32_t>::max()) {
        throw std::runtime_error("Selected node-name lookup exceeds uint32 range");
    }

    // Match PathIndexReader's adaptive policy: avoid a graph-sized table for
    // ordinary small queries, but use direct indexing when billions of steps
    // repeatedly visit a large selected node set.
    if (node_ids.size() < kDenseNodeNamePromotionThreshold) {
        sparse_rank_to_name_.reserve(node_ids.size());
        for (std::size_t i = 0; i < node_ids.size(); ++i) {
            if (node_ids[i] >= node_count_) {
                throw std::runtime_error(
                    "Selected node rank is outside the path index");
            }
            const auto inserted = sparse_rank_to_name_.emplace(
                node_ids[i], static_cast<std::uint32_t>(i));
            if (!inserted.second) {
                throw std::runtime_error(
                    "Selected node-name lookup contains a duplicate rank");
            }
        }
        return;
    }

    const auto missing = std::numeric_limits<std::uint32_t>::max();
    rank_to_name_.assign(node_count_, missing);
    for (std::size_t i = 0; i < node_ids.size(); ++i) {
        const auto node_id = node_ids[i];
        if (node_id >= node_count_) {
            throw std::runtime_error(
                "Selected node rank is outside the path index");
        }
        if (rank_to_name_[node_id] != missing) {
            throw std::runtime_error(
                "Selected node-name lookup contains a duplicate rank");
        }
        rank_to_name_[node_id] = static_cast<std::uint32_t>(i);
    }
}

std::vector<SubpathRun> find_subpaths_for_node_ids(const PathIndexReader& index,
                                                   const std::vector<std::uint32_t>& node_ids,
                                                   const bool keep_single_steps) {
    // Visit nodes in id order: neighbouring ids share posting blocks, so the
    // reader's block cache decodes each block once.
    std::vector<std::uint32_t> unique_nodes = node_ids;
    std::sort(unique_nodes.begin(), unique_nodes.end());
    unique_nodes.erase(std::unique(unique_nodes.begin(), unique_nodes.end()), unique_nodes.end());

    // Collect every step occurrence of every requested node, grouped by path.
    std::unordered_map<std::uint32_t, std::vector<std::uint32_t>> path_to_steps;
    for (const auto node_id : unique_nodes) {
        index.for_each_node_posting(node_id,
        [&](const std::uint32_t path_id, const std::uint32_t step_rank) {
            path_to_steps[path_id].push_back(step_rank);
        });
    }

    std::vector<std::pair<std::uint32_t, std::vector<std::uint32_t>>> grouped;
    grouped.reserve(path_to_steps.size());
    for (auto& kv : path_to_steps) {
        grouped.emplace_back(kv.first, std::move(kv.second));
    }
    std::sort(grouped.begin(), grouped.end(),
              [](const auto& lhs, const auto& rhs) { return lhs.first < rhs.first; });

    std::vector<SubpathRun> runs;
    for (auto& entry : grouped) {
        auto& ranks = entry.second;
        std::sort(ranks.begin(), ranks.end());
        if (ranks.empty()) continue;

        // Only keep multi-step runs so extracted path output always spans at
        // least one edge instead of degenerating into single-node fragments.
        auto push_run_if_useful = [&](const std::uint64_t start_step,
                                      const std::uint64_t end_step) {
            const auto step_count = end_step - start_step + 1;
            // Suppress singleton runs because they are valid formally but tend
            // to confuse users and graph visualizers more than they help.
            if (step_count < 2 && !keep_single_steps) {
                return;
            }
            runs.push_back(SubpathRun{entry.first, start_step, step_count});
        };

        // Consecutive step ranks become one subpath; gaps split the path into
        // multiple runs, which is what we want for arbitrary communities.
        std::uint64_t start = ranks.front();
        std::uint64_t prev = ranks.front();
        for (std::size_t i = 1; i < ranks.size(); ++i) {
            if (ranks[i] == prev + 1) {
                prev = ranks[i];
                continue;
            }
            // Flush the finished contiguous run, but only if it spans at least
            // two path steps after the singleton filter above.
            push_run_if_useful(start, prev);
            start = prev = ranks[i];
        }
        // Apply the same singleton suppression to the trailing run.
        push_run_if_useful(start, prev);
    }

    return runs;
}

void write_path_as_gfa_line(std::ostream& out,
                            const PathIndexReader& index,
                            std::uint32_t path_id) {
    const auto info = index.get_path_info(path_id);
    const auto steps = index.read_steps(path_id);

    if (info.record_type == 'W') {
        out << "W\t" << info.sample_id << '\t' << info.hap_index << '\t'
            << info.seq_id << '\t';
        if (info.seq_start >= 0) out << info.seq_start;
        else out << '*';
        out << '\t';
        if (info.seq_end >= 0) out << info.seq_end;
        else out << '*';
        out << '\t';
        write_w_segments(out, index, steps);
        if (!info.tags.empty()) {
            out << '\t' << info.tags;
        }
    } else {
        out << "P\t" << info.name << '\t';
        write_p_segments(out, index, steps);
        out << '\t' << info.overlap_field;
        if (!info.tags.empty()) {
            out << '\t' << info.tags;
        }
    }
    out << '\n';
}

// Both reader-backed and selected-vector-backed name lookups expose the same
// small API, so one typed formatter keeps their byte-level output identical.
template <typename NodeNameLookup>
std::string format_subpath_as_gfa_line_impl(
    const PathIndexReader& step_index,
    const NodeNameLookup& node_name_index,
    std::uint32_t path_id,
    std::uint64_t start_step,
    std::uint64_t step_count,
    std::string_view output_name) {
    const auto info = step_index.get_path_info(path_id);
    const auto steps = step_index.read_steps(path_id, start_step, step_count);

    // Build one complete record so the ordered writer performs one checked
    // write and never interleaves fields from different worker threads.
    std::string line;
    line.reserve(64 + output_name.size() + info.tags.size() +
                 info.overlap_field.size() + steps.size() * 12);
    line.push_back(info.record_type);
    line.push_back('\t');
    if (info.record_type == 'W') {
        line.append(info.sample_id);
        line.push_back('\t');
        line.append(std::to_string(info.hap_index));
        line.push_back('\t');
        line.append(output_name);
        line.append("\t*\t*\t");
        for (const auto& step : steps) {
            line.push_back(step.is_reverse ? '<' : '>');
            line.append(node_name_index.get_node_name(step.node_id));
        }
    } else {
        line.append(output_name);
        line.push_back('\t');
        for (std::size_t i = 0; i < steps.size(); ++i) {
            if (i > 0) line.push_back(',');
            line.append(node_name_index.get_node_name(steps[i].node_id));
            line.push_back(steps[i].is_reverse ? '-' : '+');
        }
        line.push_back('\t');
        line.append(make_overlap_slice(info.overlap_field,
                                       start_step,
                                       step_count > 0 ? step_count - 1 : 0));
    }
    if (!info.tags.empty()) {
        line.push_back('\t');
        line.append(info.tags);
    }
    line.push_back('\n');
    return line;
}

std::string format_subpath_as_gfa_line(
    const PathIndexReader& step_index,
    const PathIndexReader& node_name_index,
    std::uint32_t path_id,
    std::uint64_t start_step,
    std::uint64_t step_count,
    std::string_view output_name) {
    return format_subpath_as_gfa_line_impl(
        step_index, node_name_index, path_id, start_step, step_count,
        output_name);
}

std::string format_subpath_as_gfa_line(
    const PathIndexReader& step_index,
    const SelectedNodeNameLookup& node_name_index,
    std::uint32_t path_id,
    std::uint64_t start_step,
    std::uint64_t step_count,
    std::string_view output_name) {
    return format_subpath_as_gfa_line_impl(
        step_index, node_name_index, path_id, start_step, step_count,
        output_name);
}

void write_subpath_as_gfa_line(std::ostream& out,
                               const PathIndexReader& index,
                               std::uint32_t path_id,
                               std::uint64_t start_step,
                               std::uint64_t step_count,
                               std::string_view output_name) {
    const auto info = index.get_path_info(path_id);
    const auto steps = index.read_steps(path_id, start_step, step_count);

    if (info.record_type == 'W') {
        out << "W\t" << info.sample_id << '\t' << info.hap_index << '\t'
            << output_name << "\t*\t*\t";
        write_w_segments(out, index, steps);
        if (!info.tags.empty()) {
            out << '\t' << info.tags;
        }
        out << '\n';
    } else {
        // Slice the original overlap field so the emitted P line stays valid.
        const auto overlap = make_overlap_slice(info.overlap_field,
                                                start_step,
                                                step_count > 0 ? step_count - 1 : 0);

        out << "P\t" << output_name << '\t';
        write_p_segments(out, index, steps);
        out << '\t' << overlap;
        if (!info.tags.empty()) {
            out << '\t' << info.tags;
        }
        out << '\n';
    }
}

}  // namespace gfaidx::paths

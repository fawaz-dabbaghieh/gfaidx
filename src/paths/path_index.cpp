#include "paths/path_index.h"

#include <algorithm>
#include <cstring>
#include <fstream>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <unordered_set>
#include <utility>

#include "fs/gfa_line_parsers.h"
#include "utils/Timer.h"

namespace gfaidx::paths {
namespace {

// The path index is a single binary file with:
// - a fixed header
// - a path table
// - a node table
// - a flat step array
// - a per-node posting blob (raw in older versions, compressed in v4)
// - one shared string blob for names / overlap fields / tags
constexpr char kPathIndexMagic[8] = {'G', 'F', 'P', 'A', 'T', 'H', '1', '\0'};
constexpr std::uint32_t kPathIndexVersionV1 = 1;
constexpr std::uint32_t kPathIndexVersionV2 = 2;
constexpr std::uint32_t kPathIndexVersionV3 = 3;
constexpr std::uint32_t kPathIndexVersion = 4;

// Version-3 stores each step in one packed uint32:
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

// Version-1 path metadata for P-lines only.
struct PathRecordDiskV1 {
    std::uint64_t name_offset{};
    std::uint64_t name_len{};
    std::uint64_t step_begin{};
    std::uint64_t step_count{};
    std::uint64_t overlap_offset{};
    std::uint64_t overlap_len{};
    std::uint64_t tags_offset{};
    std::uint64_t tags_len{};
};

// Version-2 path metadata that supports both P-lines and W-lines.
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
// Version-4 switches posting_begin from "posting index" to "byte offset into
// the compressed posting blob". posting_count remains the decoded posting
// count so callers can still reason about how many occurrences the node has.
struct NodeRecordDisk {
    std::uint64_t name_offset{};
    std::uint64_t name_len{};
    std::uint64_t posting_begin{};
    std::uint64_t posting_count{};
};

// Legacy version-1/2 step layout.
struct StepRecordDiskV1V2 {
    std::uint32_t node_id{};
    std::uint8_t flags{};
    std::uint8_t reserved[3]{};
};

// Version-3 step layout. This cuts the step table from 8 bytes/step to 4
// bytes/step while preserving direct random access by step index.
struct StepRecordDisk {
    std::uint32_t packed{};
};

// Legacy raw posting layout used by version-1/2/3 files.
struct PostingRecordDisk {
    std::uint32_t path_id{};
    std::uint32_t step_rank{};
};

struct TempPosting {
    std::uint32_t node_id{};
    std::uint32_t path_id{};
    std::uint32_t step_rank{};
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
static_assert(sizeof(PathRecordDiskV1) == 64, "Unexpected v1 path record size");
static_assert(sizeof(PathRecordDisk) == 128, "Unexpected path record size");
static_assert(sizeof(NodeRecordDisk) == 32, "Unexpected node record size");
static_assert(sizeof(StepRecordDiskV1V2) == 8, "Unexpected legacy step record size");
static_assert(sizeof(StepRecordDisk) == 4, "Unexpected packed step record size");
static_assert(sizeof(PostingRecordDisk) == 8, "Unexpected posting record size");

void append_varint(std::string& out, std::uint64_t value) {
    while (value >= 0x80) {
        out.push_back(static_cast<char>((value & 0x7f) | 0x80));
        value >>= 7;
    }
    out.push_back(static_cast<char>(value));
}

std::uint64_t read_varint(const std::vector<unsigned char>& data, std::size_t& cursor) {
    std::uint64_t value = 0;
    unsigned shift = 0;

    while (cursor < data.size()) {
        const auto byte = static_cast<std::uint64_t>(data[cursor++]);
        value |= (byte & 0x7f) << shift;
        if ((byte & 0x80) == 0) {
            return value;
        }

        shift += 7;
        if (shift >= 64) {
            throw std::runtime_error("Malformed varint in compressed posting block");
        }
    }

    throw std::runtime_error("Unexpected end of compressed posting block");
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

StepRecord unpack_step_record(const StepRecordDiskV1V2& rec) {
    return StepRecord{
        rec.node_id,
        rec.flags != 0
    };
}

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
    std::vector<StepRecordDisk>& steps,
    std::vector<TempPosting>& postings) {

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

        steps.push_back(pack_step_record(it->second, orient == '-'));
        postings.push_back(TempPosting{it->second, path_id, step_rank});
        ++step_rank;

        if (comma == npos) break;
        pos = comma + 1;
    }

    return step_rank;
}

std::uint64_t parse_walk_steps(
    std::string_view walk,
    const std::unordered_map<std::string, std::uint32_t>& node_to_id,
    std::uint32_t path_id,
    std::vector<StepRecordDisk>& steps,
    std::vector<TempPosting>& postings) {

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

        steps.push_back(pack_step_record(it->second, orient == '<'));
        postings.push_back(TempPosting{it->second, path_id, step_rank});
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

bool build_path_index(const std::string& input_gfa,
                      const std::string& output_index,
                      const Reader::Options& reader_options) {
    Timer timer;

    std::unordered_map<std::string, std::uint32_t> node_to_id;
    std::vector<std::string> id_to_node;

    {
        // Pass 1: build the node-id -> integer-id mapping from S lines so path
        // steps can be stored compactly as integers.
        std::string_view line;
        Reader reader(reader_options);
        if (!reader.open(input_gfa)) {
            throw std::runtime_error("Could not open file: " + input_gfa);
        }

        std::cout << get_time() << ": Scanning S lines for node ids" << std::endl;
        while (reader.read_line(line)) {
            if (line.empty() || line[0] != 'S') continue;
            auto node_id = extract_s_node_id(line);
            if (node_to_id.find(node_id) != node_to_id.end()) continue;
            const auto int_id = static_cast<std::uint32_t>(id_to_node.size());
            node_to_id.emplace(node_id, int_id);
            id_to_node.push_back(std::move(node_id));
        }
    }

    std::vector<PathBuildEntry> paths;
    std::vector<StepRecordDisk> steps;
    std::vector<TempPosting> postings;

    {
        // Pass 2: scan P/W lines, store each walk as a flat step range, and
        // also collect postings so later node-set queries can go node -> steps.
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
            entry.step_begin = static_cast<std::uint64_t>(steps.size());

            if (line[0] == 'P') {
                ParsedPathFields parsed = parse_path_fields(line);
                entry.record_type = 'P';
                entry.name = std::move(parsed.name);
                entry.overlaps = std::string(parsed.overlaps);
                entry.tags = std::string(parsed.tags);
                entry.step_count = parse_path_steps(parsed.segments, node_to_id, path_id, steps, postings);
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
                entry.step_count = parse_walk_steps(parsed.walk, node_to_id, path_id, steps, postings);
            }

            paths.push_back(std::move(entry));
        }
    }

    std::cout << get_time() << ": Building per-node path postings" << std::endl;
    // Sort postings by node first so each node becomes one contiguous range in
    // the final posting table. Within a node, sort by path then step rank.
    std::sort(postings.begin(), postings.end(),
              [](const TempPosting& lhs, const TempPosting& rhs) {
                  if (lhs.node_id != rhs.node_id) return lhs.node_id < rhs.node_id;
                  if (lhs.path_id != rhs.path_id) return lhs.path_id < rhs.path_id;
                  return lhs.step_rank < rhs.step_rank;
              });

    std::string strings_blob;
    strings_blob.reserve(1024);

    // Materialize the final path table while packing names / overlaps / tags
    // into one shared string blob.
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

    // Build the node table and assign each node one compressed posting block.
    // Each node record stores:
    // - posting_begin: byte offset of its block inside the posting section
    // - posting_count: number of decoded postings in that block
    std::vector<NodeRecordDisk> node_records(id_to_node.size());
    std::size_t posting_cursor = 0;
    std::string posting_blob;
    posting_blob.reserve(postings.size() * 5 / 2);
    for (std::size_t i = 0; i < id_to_node.size(); ++i) {
        auto& dst = node_records[i];
        dst.name_offset = append_string(strings_blob, id_to_node[i]);
        dst.name_len = id_to_node[i].size();
        dst.posting_begin = static_cast<std::uint64_t>(posting_blob.size());
        const auto node_begin = posting_cursor;
        while (posting_cursor < postings.size() && postings[posting_cursor].node_id == i) {
            ++posting_cursor;
        }
        dst.posting_count = posting_cursor - node_begin;
        append_compressed_posting_block(posting_blob, postings, node_begin, posting_cursor);
    }

    // Compute all section offsets up front so the file can be written in one
    // sequential pass.
    PathIndexHeaderDisk header{};
    std::memcpy(header.magic, kPathIndexMagic, sizeof(kPathIndexMagic));
    header.version = kPathIndexVersion;
    header.path_count = path_records.size();
    header.node_count = node_records.size();
    header.step_count = steps.size();
    header.posting_count = postings.size();

    header.path_table_offset = sizeof(PathIndexHeaderDisk);
    header.node_table_offset = header.path_table_offset + path_records.size() * sizeof(PathRecordDisk);
    header.step_table_offset = header.node_table_offset + node_records.size() * sizeof(NodeRecordDisk);
    header.posting_table_offset = header.step_table_offset + steps.size() * sizeof(StepRecordDisk);
    header.strings_offset = header.posting_table_offset + posting_blob.size();
    header.strings_size = strings_blob.size();

    std::ofstream out(output_index, std::ios::binary | std::ios::trunc);
    if (!out) {
        throw std::runtime_error("Failed to open output path index: " + output_index);
    }

    out.write(reinterpret_cast<const char*>(&header), sizeof(header));
    write_vector(out, path_records);
    write_vector(out, node_records);
    write_vector(out, steps);
    if (!posting_blob.empty()) {
        out.write(posting_blob.data(), static_cast<std::streamsize>(posting_blob.size()));
    }
    if (!strings_blob.empty()) {
        out.write(strings_blob.data(), static_cast<std::streamsize>(strings_blob.size()));
    }

    if (!out.good()) {
        throw std::runtime_error("Failed while writing path index: " + output_index);
    }

    std::cout << get_time() << ": Indexed " << path_records.size() << " paths, "
              << node_records.size() << " nodes, "
              << steps.size() << " path steps in " << timer.elapsed() << " seconds" << std::endl;
    return true;
}

PathIndexReader::PathIndexReader(const std::string& index_path)
    : index_path_(index_path), in_(index_path, std::ios::binary) {
    if (!in_) {
        throw std::runtime_error("Failed to open path index: " + index_path);
    }

    PathIndexHeaderDisk header{};
    in_.read(reinterpret_cast<char*>(&header), sizeof(header));
    if (!in_) {
        throw std::runtime_error("Failed to read path index header: " + index_path);
    }

    if (std::memcmp(header.magic, kPathIndexMagic, sizeof(kPathIndexMagic)) != 0) {
        throw std::runtime_error("Invalid path index magic: " + index_path);
    }
    if (header.version != kPathIndexVersionV1 &&
        header.version != kPathIndexVersionV2 &&
        header.version != kPathIndexVersionV3 &&
        header.version != kPathIndexVersion) {
        throw std::runtime_error("Unsupported path index version: " + std::to_string(header.version));
    }

    step_table_offset_ = header.step_table_offset;
    posting_table_offset_ = header.posting_table_offset;
    posting_table_bytes_ = header.strings_offset - header.posting_table_offset;
    // Legacy v1/v2 files use the old 8-byte step layout. Version-3+ files use
    // the packed 4-byte representation.
    step_record_bytes_ = (header.version >= kPathIndexVersionV3) ? sizeof(StepRecordDisk)
                                                                 : sizeof(StepRecordDiskV1V2);
    postings_are_compressed_ = (header.version >= kPathIndexVersion);

    // Load the small metadata tables and the shared strings eagerly. Step data
    // and posting blocks remain on disk and are fetched on demand.
    std::vector<NodeRecordDisk> node_records(static_cast<std::size_t>(header.node_count));
    read_exact(header.node_table_offset,
               node_records.data(),
               node_records.size() * sizeof(NodeRecordDisk));

    strings_.resize(static_cast<std::size_t>(header.strings_size));
    if (!strings_.empty()) {
        read_exact(header.strings_offset, strings_.data(), strings_.size());
    }

    paths_.reserve(static_cast<std::size_t>(header.path_count));
    if (header.version == kPathIndexVersionV1) {
        std::vector<PathRecordDiskV1> path_records(static_cast<std::size_t>(header.path_count));
        read_exact(header.path_table_offset,
                   path_records.data(),
                   path_records.size() * sizeof(PathRecordDiskV1));
        for (const auto& rec : path_records) {
            paths_.push_back(PathMeta{
                'P',
                rec.name_offset,
                rec.name_len,
                rec.step_begin,
                rec.step_count,
                rec.overlap_offset,
                rec.overlap_len,
                rec.tags_offset,
                rec.tags_len,
                0,
                0,
                0,
                0,
                0,
                -1,
                -1
            });
        }
    } else {
        std::vector<PathRecordDisk> path_records(static_cast<std::size_t>(header.path_count));
        read_exact(header.path_table_offset,
                   path_records.data(),
                   path_records.size() * sizeof(PathRecordDisk));
        for (const auto& rec : path_records) {
            paths_.push_back(PathMeta{
                rec.record_type,
                rec.name_offset,
                rec.name_len,
                rec.step_begin,
                rec.step_count,
                rec.overlap_offset,
                rec.overlap_len,
                rec.tags_offset,
                rec.tags_len,
                rec.sample_offset,
                rec.sample_len,
                rec.hap_index,
                rec.seq_id_offset,
                rec.seq_id_len,
                rec.seq_start,
                rec.seq_end
            });
        }
    }

    nodes_.reserve(node_records.size());
    for (const auto& rec : node_records) {
        nodes_.push_back(NodeMeta{
            rec.name_offset,
            rec.name_len,
            rec.posting_begin,
            rec.posting_count
        });
    }

    // Build name -> id maps once so repeated lookups stay cheap.
    for (std::uint32_t i = 0; i < paths_.size(); ++i) {
        path_name_to_id_.emplace(std::string(get_path_name(i)), i);
    }
    for (std::uint32_t i = 0; i < nodes_.size(); ++i) {
        node_name_to_id_.emplace(std::string(get_node_name(i)), i);
    }
}

bool PathIndexReader::lookup_path_id(const std::string& name, std::uint32_t& out_path_id) const {
    const auto it = path_name_to_id_.find(name);
    if (it == path_name_to_id_.end()) return false;
    out_path_id = it->second;
    return true;
}

bool PathIndexReader::lookup_node_id(const std::string& name, std::uint32_t& out_node_id) const {
    const auto it = node_name_to_id_.find(name);
    if (it == node_name_to_id_.end()) return false;
    out_node_id = it->second;
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
        view_string(rec.name_offset, rec.name_len),
        view_string(rec.overlap_offset, rec.overlap_len),
        view_string(rec.tags_offset, rec.tags_len),
        view_string(rec.sample_offset, rec.sample_len),
        rec.hap_index,
        view_string(rec.seq_id_offset, rec.seq_id_len),
        rec.seq_start,
        rec.seq_end
    };
}

std::string_view PathIndexReader::get_path_name(std::uint32_t path_id) const {
    if (path_id >= paths_.size()) {
        throw std::runtime_error("Path id out of range");
    }
    const auto& rec = paths_[path_id];
    return view_string(rec.name_offset, rec.name_len);
}

std::string_view PathIndexReader::get_node_name(std::uint32_t node_id) const {
    if (node_id >= nodes_.size()) {
        throw std::runtime_error("Node id out of range");
    }
    const auto& rec = nodes_[node_id];
    return view_string(rec.name_offset, rec.name_len);
}

std::string_view PathIndexReader::get_overlap_field(std::uint32_t path_id) const {
    if (path_id >= paths_.size()) {
        throw std::runtime_error("Path id out of range");
    }
    const auto& rec = paths_[path_id];
    return view_string(rec.overlap_offset, rec.overlap_len);
}

std::string_view PathIndexReader::get_tags(std::uint32_t path_id) const {
    if (path_id >= paths_.size()) {
        throw std::runtime_error("Path id out of range");
    }
    const auto& rec = paths_[path_id];
    return view_string(rec.tags_offset, rec.tags_len);
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
    if (take == 0) return out;

    // Steps are stored in one flat array, so reading a path slice is one seek
    // and one contiguous read.
    const std::uint64_t byte_offset = step_table_offset_ +
        (info.step_begin + start_step) * step_record_bytes_;

    if (step_record_bytes_ == sizeof(StepRecordDisk)) {
        std::vector<StepRecordDisk> raw(static_cast<std::size_t>(take));
        read_exact(byte_offset, raw.data(), raw.size() * sizeof(StepRecordDisk));
        for (const auto& rec : raw) {
            out.push_back(unpack_step_record(rec));
        }
    } else {
        std::vector<StepRecordDiskV1V2> raw(static_cast<std::size_t>(take));
        read_exact(byte_offset, raw.data(), raw.size() * sizeof(StepRecordDiskV1V2));
        for (const auto& rec : raw) {
            out.push_back(unpack_step_record(rec));
        }
    }
    return out;
}

void PathIndexReader::for_each_node_posting(
    std::uint32_t node_id,
    const std::function<void(std::uint32_t path_id, std::uint32_t step_rank)>& callback) const {
    if (node_id >= nodes_.size()) {
        throw std::runtime_error("Node id out of range");
    }

    const auto& node = nodes_[node_id];
    if (node.posting_count == 0) return;

    if (!postings_are_compressed_) {
        // Legacy v1/v2/v3 files store fixed-width posting structs.
        std::vector<PostingRecordDisk> postings(static_cast<std::size_t>(node.posting_count));
        const std::uint64_t byte_offset = posting_table_offset_ + node.posting_begin * sizeof(PostingRecordDisk);
        read_exact(byte_offset, postings.data(), postings.size() * sizeof(PostingRecordDisk));

        for (const auto& posting : postings) {
            callback(posting.path_id, posting.step_rank);
        }
        return;
    }

    const std::uint64_t block_begin = node.posting_begin;
    const std::uint64_t block_end = (node_id + 1 < nodes_.size())
        ? nodes_[node_id + 1].posting_begin
        : posting_table_bytes_;
    if (block_end < block_begin) {
        throw std::runtime_error("Compressed posting block offsets are out of order");
    }

    std::vector<unsigned char> block(static_cast<std::size_t>(block_end - block_begin));
    read_exact(posting_table_offset_ + block_begin, block.data(), block.size());

    std::size_t cursor = 0;
    std::uint32_t current_path_id = 0;
    std::uint64_t emitted = 0;

    while (emitted < node.posting_count) {
        const auto path_delta = read_varint(block, cursor);
        if (path_delta > std::numeric_limits<std::uint32_t>::max() - current_path_id) {
            throw std::runtime_error("Compressed posting block path id overflow");
        }
        current_path_id = static_cast<std::uint32_t>(current_path_id + path_delta);

        const auto group_count = read_varint(block, cursor);
        if (group_count == 0) {
            throw std::runtime_error("Compressed posting block has an empty path group");
        }
        if (group_count > node.posting_count - emitted) {
            throw std::runtime_error("Compressed posting block overruns node posting count");
        }

        std::uint64_t current_step_rank = read_varint(block, cursor);
        if (current_step_rank > std::numeric_limits<std::uint32_t>::max()) {
            throw std::runtime_error("Compressed posting block step rank overflow");
        }
        callback(current_path_id, static_cast<std::uint32_t>(current_step_rank));
        ++emitted;

        for (std::uint64_t i = 1; i < group_count; ++i) {
            const auto step_delta = read_varint(block, cursor);
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

    if (cursor != block.size()) {
        throw std::runtime_error("Compressed posting block has trailing bytes");
    }
}

std::string_view PathIndexReader::view_string(std::uint64_t offset, std::uint64_t len) const {
    if (offset + len > strings_.size()) {
        throw std::runtime_error("Path index string offset out of range");
    }
    return {strings_.data() + offset, static_cast<std::size_t>(len)};
}

void PathIndexReader::read_exact(std::uint64_t offset, void* dst, std::size_t bytes) const {
    if (bytes == 0) return;
    in_.clear();
    in_.seekg(static_cast<std::streamoff>(offset), std::ios::beg);
    if (!in_) {
        throw std::runtime_error("seekg failed while reading path index");
    }
    in_.read(reinterpret_cast<char*>(dst), static_cast<std::streamsize>(bytes));
    if (!in_) {
        throw std::runtime_error("Failed to read bytes from path index");
    }
}

std::vector<SubpathRun> find_subpaths_for_nodes(const PathIndexReader& index,
                                                const std::vector<std::string>& node_names) {
    std::unordered_set<std::uint32_t> unique_nodes;
    unique_nodes.reserve(node_names.size());

    for (const auto& node_name : node_names) {
        std::uint32_t node_id = 0;
        if (!index.lookup_node_id(node_name, node_id)) {
            throw std::runtime_error("Node id not found in path index: " + node_name);
        }
        unique_nodes.insert(node_id);
    }

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

        // Consecutive step ranks become one subpath; gaps split the path into
        // multiple runs, which is what we want for arbitrary communities.
        std::uint64_t start = ranks.front();
        std::uint64_t prev = ranks.front();
        for (std::size_t i = 1; i < ranks.size(); ++i) {
            if (ranks[i] == prev + 1) {
                prev = ranks[i];
                continue;
            }
            runs.push_back(SubpathRun{entry.first, start, prev - start + 1});
            start = prev = ranks[i];
        }
        runs.push_back(SubpathRun{entry.first, start, prev - start + 1});
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

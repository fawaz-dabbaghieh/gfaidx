#include "paths/walk_coords.h"

#include <memory>
#include <ostream>
#include <stdexcept>
#include <string>
#include <string_view>

#include "fs/Reader.h"
#include "fs/fs_helpers.h"

namespace gfaidx::paths {
namespace {

void warn_if_requested(const WalkCoordWarning& warn, const std::string& message) {
    if (warn) warn(message);
}

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

bool parse_s_line_name_and_length(std::string_view line,
                                  std::string& out_name,
                                  std::uint64_t& out_length) {
    const auto t1 = line.find('\t');
    if (t1 == std::string_view::npos) return false;
    const auto t2 = line.find('\t', t1 + 1);
    if (t2 == std::string_view::npos) return false;
    const auto t3 = line.find('\t', t2 + 1);

    out_name.assign(line.substr(t1 + 1, t2 - (t1 + 1)));
    const auto seq = (t3 == std::string_view::npos)
        ? line.substr(t2 + 1)
        : line.substr(t2 + 1, t3 - (t2 + 1));

    if (seq != "*") {
        out_length = static_cast<std::uint64_t>(seq.size());
        return true;
    }

    if (t3 == std::string_view::npos) {
        return false;
    }

    std::size_t pos = t3 + 1;
    while (pos < line.size()) {
        const auto next_tab = line.find('\t', pos);
        const auto end = (next_tab == std::string_view::npos) ? line.size() : next_tab;
        const auto field = line.substr(pos, end - pos);
        if (field.substr(0, 5) == "LN:i:" && field.size() > 5) {
            try {
                out_length = static_cast<std::uint64_t>(std::stoull(std::string(field.substr(5))));
                return true;
            } catch (const std::exception&) {
                return false;
            }
        }
        if (next_tab == std::string_view::npos) break;
        pos = next_tab + 1;
    }

    return false;
}

bool p_path_has_no_overlaps(std::string_view overlap_field) {
    return overlap_field.empty() || overlap_field == "*";
}

std::string p_coordinate_subpath_name(std::string_view path_name,
                                      std::uint64_t start,
                                      std::uint64_t end) {
    // P paths selected for coordinate indexing are path-local by default. When
    // the common pangenome name shape sample#hap#seq is present, appending
    // :start-end makes the resulting path name coordinate-addressable.
    return std::string(path_name) + ":" +
           std::to_string(start) + "-" +
           std::to_string(end);
}

struct CoordinateSlice {
    std::uint64_t start{};
    std::uint64_t end{};
    std::vector<StepRecord> steps;
};

bool build_coordinate_slice(const PathIndexReader& index,
                            const PathInfo& info,
                            const WalkCoordState& walk_coord_state,
                            std::uint64_t start_step,
                            std::uint64_t step_count,
                            CoordinateSlice& out,
                            const WalkCoordWarning& warn) {
    // Query commands only need the coordinates and steps for one emitted
    // subpath. Stream the path prefix up to the subpath end instead of
    // materializing the complete chromosome-scale path in memory.
    if (step_count == 0 || start_step > info.step_count ||
        step_count > info.step_count - start_step) {
        warn_if_requested(warn, "requested subpath range is outside path '" +
                                std::string(info.name) + "'");
        return false;
    }
    if (info.record_type == 'P' && !p_path_has_no_overlaps(info.overlap_field)) {
        warn_if_requested(warn, "P-line '" + std::string(info.name) +
                                "' has overlaps, falling back to subpath output without coordinates");
        return false;
    }
    if (info.record_type == 'W' && (info.seq_start < 0 || info.seq_end < 0)) {
        warn_if_requested(warn, "W-line '" + std::string(info.name) +
                                "' is missing SeqStart/SeqEnd, falling back to subwalk output without coordinates");
        return false;
    }
    if (info.record_type == 'W' && info.seq_end < info.seq_start) {
        warn_if_requested(warn, "W-line '" + std::string(info.name) +
                                "' has SeqEnd < SeqStart, falling back to subwalk output without coordinates");
        return false;
    }

    out = CoordinateSlice{};
    out.steps.reserve(static_cast<std::size_t>(step_count));
    const std::uint64_t end_step = start_step + step_count;
    std::uint64_t cumulative = 0;

    try {
        index.for_each_step(info.path_id, 0, end_step,
            [&](const StepRecord& step, std::uint64_t step_rank) {
                if (step.node_id >= walk_coord_state.length_count()) {
                    throw std::runtime_error("path references a node outside the length table");
                }
                if (step_rank == start_step) {
                    out.start = cumulative;
                }
                if (step_rank >= start_step && step_rank < end_step) {
                    out.steps.push_back(step);
                }
                cumulative += walk_coord_state.node_length(step.node_id);
                if (step_rank + 1 == end_step) {
                    out.end = cumulative;
                }
            });
    } catch (const std::exception& err) {
        warn_if_requested(warn, "could not compute coordinates for path '" +
                                std::string(info.name) + "': " + err.what());
        out = CoordinateSlice{};
        return false;
    }

    if (info.record_type == 'W') {
        out.start += static_cast<std::uint64_t>(info.seq_start);
        out.end += static_cast<std::uint64_t>(info.seq_start);
        if (out.end > static_cast<std::uint64_t>(info.seq_end)) {
            warn_if_requested(warn, "W-line '" + std::string(info.name) +
                                    "' has segment lengths beyond SeqEnd, falling back to subwalk output without coordinates");
            out = CoordinateSlice{};
            return false;
        }
    }
    return true;
}

void write_w_segments_from_steps(std::ostream& out,
                                 const PathIndexReader& index,
                                 const std::vector<StepRecord>& steps) {
    for (const auto& step : steps) {
        out << (step.is_reverse ? '<' : '>');
        out << index.get_node_name(step.node_id);
    }
}

void write_p_segments_from_steps(std::ostream& out,
                                 const PathIndexReader& index,
                                 const std::vector<StepRecord>& steps) {
    for (std::size_t i = 0; i < steps.size(); ++i) {
        const auto& step = steps[i];
        out << index.get_node_name(step.node_id);
        out << (step.is_reverse ? '-' : '+');
        if (i + 1 < steps.size()) out << ',';
    }
}

}  // namespace

std::uint64_t WalkCoordState::length_count() const {
    if (length_index) return length_index->node_count();
    return node_lengths.size();
}

std::uint64_t WalkCoordState::node_length(std::uint32_t node_id) const {
    if (length_index) return length_index->length(node_id);
    return node_lengths[static_cast<std::size_t>(node_id)];
}

WalkCoordState load_node_lengths_by_index(const PathIndexReader& index,
                                          const indexer::NodeHashIndex& node_index,
                                          const std::string& source_gfa,
                                          const std::string& length_index_path,
                                          const WalkCoordWarning& warn) {
    WalkCoordState state;

    if (!length_index_path.empty() && file_exists(length_index_path.c_str())) {
        try {
            auto length_index = std::make_unique<indexer::NodeLengthIndexReader>(length_index_path);
            if (length_index->node_count() != index.node_count()) {
                warn_if_requested(warn, ".lnx node count does not match .pdx, falling back to S-line length scan");
            } else {
                state.length_index = std::move(length_index);
                state.usable = true;
                return state;
            }
        } catch (const std::exception& err) {
            warn_if_requested(warn, "could not use node length index '" + length_index_path +
                                    "' (" + err.what() + "), falling back to S-line length scan");
        }
    } else if (!length_index_path.empty()) {
        warn_if_requested(warn, "node length index not found at '" + length_index_path +
                                "', falling back to S-line length scan");
    }

    state.node_lengths.resize(index.node_count());
    std::vector<std::uint64_t> seen_nodes((static_cast<std::size_t>(index.node_count()) + 63) / 64, 0);
    std::uint64_t seen_node_count = 0;

    Reader reader;
    if (!reader.open(source_gfa)) {
        warn_if_requested(warn, "could not open source GFA '" + source_gfa +
                                "', falling back to W subpaths without coordinates");
        state.usable = false;
        return state;
    }

    std::string_view line;
    std::string node_name;
    std::uint64_t node_length = 0;

    while (reader.read_line(line)) {
        if (line.empty() || line[0] != 'S') continue;

        if (!parse_s_line_name_and_length(line, node_name, node_length)) {
            warn_if_requested(warn, "could not derive segment length for node '" + node_name +
                                    "', falling back to W subpaths without coordinates");
            return state;
        }

        std::uint32_t node_id = 0;
        if (!node_index.lookup_rank(node_name, node_id)) {
            warn_if_requested(warn, "node '" + node_name +
                                    "' from source GFA was not found in .ndx, falling back to W subpaths without coordinates");
            return state;
        }
        if (node_id >= index.node_count()) {
            warn_if_requested(warn, "node '" + node_name +
                                    "' resolved to an id outside the .pdx node range, falling back to W subpaths without coordinates");
            return state;
        }
        if (test_seen_bit(seen_nodes, node_id)) {
            warn_if_requested(warn, "duplicate source node or .ndx collision for node '" + node_name +
                                    "', falling back to W subpaths without coordinates");
            return state;
        }

        set_seen_bit(seen_nodes, node_id);
        ++seen_node_count;
        state.node_lengths[node_id] = node_length;
    }

    if (seen_node_count != index.node_count()) {
        warn_if_requested(warn, "the node set in source GFA does not match the node set in .pdx/.ndx, falling back to W subpaths without coordinates");
        return state;
    }

    state.usable = true;
    return state;
}

PathCoordCacheEntry& get_or_build_path_coord_cache(
    const PathIndexReader& index,
    std::uint32_t path_id,
    const WalkCoordState& walk_coord_state,
    std::unordered_map<std::uint32_t, PathCoordCacheEntry>& cache,
    const WalkCoordWarning& warn) {

    auto [it, inserted] = cache.emplace(path_id, PathCoordCacheEntry{});
    auto& entry = it->second;
    if (!inserted && entry.ready) {
        return entry;
    }

    entry.ready = true;
    entry.usable = false;
    entry.info = index.get_path_info(path_id);
    entry.steps = index.read_steps(path_id);

    if (entry.info.record_type != 'W' && entry.info.record_type != 'P') {
        return entry;
    }
    if (entry.info.record_type == 'P' && !p_path_has_no_overlaps(entry.info.overlap_field)) {
        warn_if_requested(warn, "P-line '" + std::string(entry.info.name) +
                                "' has overlaps, falling back to subpath output without coordinates");
        return entry;
    }
    if (entry.info.record_type == 'W' && (entry.info.seq_start < 0 || entry.info.seq_end < 0)) {
        warn_if_requested(warn, "W-line '" + std::string(entry.info.name) +
                                "' is missing SeqStart/SeqEnd, falling back to subwalk output without coordinates");
        return entry;
    }
    if (entry.info.record_type == 'W' && entry.info.seq_end < entry.info.seq_start) {
        warn_if_requested(warn, "W-line '" + std::string(entry.info.name) +
                                "' has SeqEnd < SeqStart, falling back to subwalk output without coordinates");
        return entry;
    }

    entry.prefix_lengths.resize(entry.steps.size() + 1, 0);
    for (std::size_t i = 0; i < entry.steps.size(); ++i) {
        const auto node_id = entry.steps[i].node_id;
        if (node_id >= walk_coord_state.length_count()) {
            warn_if_requested(warn, "Path '" + std::string(entry.info.name) +
                                    "' references a node outside the length table, falling back to subwalk output without coordinates");
            entry.prefix_lengths.clear();
            return entry;
        }
        entry.prefix_lengths[i + 1] = entry.prefix_lengths[i] +
                                      walk_coord_state.node_length(node_id);
    }

    if (entry.info.record_type == 'W') {
        const auto expected_span = static_cast<std::uint64_t>(entry.info.seq_end - entry.info.seq_start);
        if (entry.prefix_lengths.back() != expected_span) {
            warn_if_requested(warn, "W-line '" + std::string(entry.info.name) +
                                    "' has inconsistent SeqStart/SeqEnd versus segment lengths, falling back to subwalk output without coordinates");
            entry.prefix_lengths.clear();
            return entry;
        }
    }

    entry.usable = true;
    return entry;
}

void write_w_subpath_with_coords(std::ostream& out,
                                 const PathIndexReader& index,
                                 const PathCoordCacheEntry& entry,
                                 std::uint64_t start_step,
                                 std::uint64_t step_count,
                                 std::string_view subpath_label) {
    const auto sub_start = static_cast<std::uint64_t>(entry.info.seq_start) + entry.prefix_lengths[start_step];
    const auto sub_end = static_cast<std::uint64_t>(entry.info.seq_start) + entry.prefix_lengths[start_step + step_count];

    out << "W\t" << entry.info.sample_id << '\t' << entry.info.hap_index << '\t'
        << entry.info.seq_id << '\t' << sub_start << '\t' << sub_end << '\t';
    for (std::uint64_t i = start_step; i < start_step + step_count; ++i) {
        const auto& step = entry.steps[static_cast<std::size_t>(i)];
        out << (step.is_reverse ? '<' : '>');
        out << index.get_node_name(step.node_id);
    }
    if (!entry.info.tags.empty()) {
        out << '\t' << entry.info.tags;
    }
    // Keep the W SeqId as the original coordinate namespace. The synthetic
    // subpath label is useful for humans/debugging, but putting it in SeqId
    // makes the concrete SeqStart/SeqEnd coordinates refer to a fake sequence.
    if (!subpath_label.empty()) {
        out << "\tsp:Z:" << subpath_label;
    }
    out << '\n';
}

void write_p_subpath_with_coords(std::ostream& out,
                                 const PathIndexReader& index,
                                 const PathCoordCacheEntry& entry,
                                 std::uint64_t start_step,
                                 std::uint64_t step_count) {
    const auto sub_start = entry.prefix_lengths[start_step];
    const auto sub_end = entry.prefix_lengths[start_step + step_count];
    const auto output_name = p_coordinate_subpath_name(entry.info.name, sub_start, sub_end);

    out << "P\t" << output_name << '\t';
    for (std::uint64_t i = start_step; i < start_step + step_count; ++i) {
        const auto& step = entry.steps[static_cast<std::size_t>(i)];
        out << index.get_node_name(step.node_id);
        out << (step.is_reverse ? '-' : '+');
        if (i + 1 < start_step + step_count) out << ',';
    }
    out << "\t*";
    if (!entry.info.tags.empty()) {
        out << '\t' << entry.info.tags;
    }
    out << '\n';
}

bool write_w_subpath_with_coords_bounded(std::ostream& out,
                                         const PathIndexReader& index,
                                         std::uint32_t path_id,
                                         const WalkCoordState& walk_coord_state,
                                         std::uint64_t start_step,
                                         std::uint64_t step_count,
                                         std::string_view subpath_label,
                                         const WalkCoordWarning& warn) {
    const auto info = index.get_path_info(path_id);
    CoordinateSlice slice;
    if (!build_coordinate_slice(index, info, walk_coord_state, start_step,
                                step_count, slice, warn)) {
        return false;
    }

    out << "W\t" << info.sample_id << '\t' << info.hap_index << '\t'
        << info.seq_id << '\t' << slice.start << '\t' << slice.end << '\t';
    write_w_segments_from_steps(out, index, slice.steps);
    if (!info.tags.empty()) {
        out << '\t' << info.tags;
    }
    if (!subpath_label.empty()) {
        out << "\tsp:Z:" << subpath_label;
    }
    out << '\n';
    return true;
}

bool write_p_subpath_with_coords_bounded(std::ostream& out,
                                         const PathIndexReader& index,
                                         std::uint32_t path_id,
                                         const WalkCoordState& walk_coord_state,
                                         std::uint64_t start_step,
                                         std::uint64_t step_count,
                                         const WalkCoordWarning& warn) {
    const auto info = index.get_path_info(path_id);
    CoordinateSlice slice;
    if (!build_coordinate_slice(index, info, walk_coord_state, start_step,
                                step_count, slice, warn)) {
        return false;
    }

    out << "P\t" << p_coordinate_subpath_name(info.name, slice.start, slice.end) << '\t';
    write_p_segments_from_steps(out, index, slice.steps);
    out << "\t*";
    if (!info.tags.empty()) {
        out << '\t' << info.tags;
    }
    out << '\n';
    return true;
}

}  // namespace gfaidx::paths

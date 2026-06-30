#include "paths/walk_coords.h"

#include <ostream>
#include <stdexcept>
#include <string>

#include "fs/Reader.h"

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

}  // namespace

WalkCoordState load_node_lengths_by_index(const PathIndexReader& index,
                                          const indexer::NodeHashIndex& node_index,
                                          const std::string& source_gfa,
                                          const WalkCoordWarning& warn) {
    WalkCoordState state;
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
    const std::vector<std::uint64_t>& node_lengths,
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

    if (entry.info.record_type != 'W') {
        return entry;
    }
    if (entry.info.seq_start < 0 || entry.info.seq_end < 0) {
        warn_if_requested(warn, "W-line '" + std::string(entry.info.name) +
                                "' is missing SeqStart/SeqEnd, falling back to subwalk output without coordinates");
        return entry;
    }
    if (entry.info.seq_end < entry.info.seq_start) {
        warn_if_requested(warn, "W-line '" + std::string(entry.info.name) +
                                "' has SeqEnd < SeqStart, falling back to subwalk output without coordinates");
        return entry;
    }

    entry.prefix_lengths.resize(entry.steps.size() + 1, 0);
    for (std::size_t i = 0; i < entry.steps.size(); ++i) {
        const auto node_id = entry.steps[i].node_id;
        if (node_id >= node_lengths.size()) {
            warn_if_requested(warn, "W-line '" + std::string(entry.info.name) +
                                    "' references a node outside the length table, falling back to subwalk output without coordinates");
            entry.prefix_lengths.clear();
            return entry;
        }
        entry.prefix_lengths[i + 1] = entry.prefix_lengths[i] + node_lengths[node_id];
    }

    const auto expected_span = static_cast<std::uint64_t>(entry.info.seq_end - entry.info.seq_start);
    if (entry.prefix_lengths.back() != expected_span) {
        warn_if_requested(warn, "W-line '" + std::string(entry.info.name) +
                                "' has inconsistent SeqStart/SeqEnd versus segment lengths, falling back to subwalk output without coordinates");
        entry.prefix_lengths.clear();
        return entry;
    }

    entry.usable = true;
    return entry;
}

void write_w_subpath_with_coords(std::ostream& out,
                                 const PathIndexReader& index,
                                 const PathCoordCacheEntry& entry,
                                 std::uint64_t start_step,
                                 std::uint64_t step_count,
                                 std::string_view output_name) {
    const auto sub_start = static_cast<std::uint64_t>(entry.info.seq_start) + entry.prefix_lengths[start_step];
    const auto sub_end = static_cast<std::uint64_t>(entry.info.seq_start) + entry.prefix_lengths[start_step + step_count];

    out << "W\t" << entry.info.sample_id << '\t' << entry.info.hap_index << '\t'
        << output_name << '\t' << sub_start << '\t' << sub_end << '\t';
    for (std::uint64_t i = start_step; i < start_step + step_count; ++i) {
        const auto& step = entry.steps[static_cast<std::size_t>(i)];
        out << (step.is_reverse ? '<' : '>');
        out << index.get_node_name(step.node_id);
    }
    if (!entry.info.tags.empty()) {
        out << '\t' << entry.info.tags;
    }
    out << '\n';
}

}  // namespace gfaidx::paths

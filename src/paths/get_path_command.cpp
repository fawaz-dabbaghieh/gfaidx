#include "paths/get_path_command.h"

#include <algorithm>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include "fs/Reader.h"
#include "fs/fs_helpers.h"
#include "indexer/node_hash_index.h"
#include "paths/path_index.h"
#include "utils/Timer.h"

namespace gfaidx::paths {
namespace {

struct WalkCoordState {
    bool usable{false};
    std::vector<std::uint64_t> node_lengths;
};

struct PathCoordCacheEntry {
    bool ready{false};
    bool usable{false};
    PathInfo info;
    std::vector<StepRecord> steps;
    std::vector<std::uint64_t> prefix_lengths;
};

void warn_get_path(std::string_view message) {
    std::cerr << get_time() << ": Warning: " << message << std::endl;
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

std::int64_t parse_optional_int_arg(const std::string& value, const char* flag_name) {
    if (value.empty() || value == "*") return -1;
    try {
        return std::stoll(value);
    } catch (const std::exception& err) {
        throw std::runtime_error(std::string("Invalid value for ") + flag_name + ": " + err.what());
    }
}

void append_csv_tokens(std::vector<std::string>& out, const std::string& csv) {
    for (std::size_t pos = 0; pos < csv.size();) {
        const std::size_t comma = csv.find(',', pos);
        const std::size_t end = (comma == std::string::npos) ? csv.size() : comma;
        if (end > pos) {
            out.emplace_back(csv.substr(pos, end - pos));
        }
        if (comma == std::string::npos) break;
        pos = comma + 1;
    }
}

std::vector<std::string> load_node_names(const std::string& csv,
                                         const std::string& file_path) {
    std::vector<std::string> nodes;
    if (!csv.empty()) {
        append_csv_tokens(nodes, csv);
    }

    if (!file_path.empty()) {
        std::ifstream in(file_path);
        if (!in) {
            throw std::runtime_error("Failed to open nodes file: " + file_path);
        }
        std::string line;
        while (std::getline(in, line)) {
            if (line.empty()) continue;
            append_csv_tokens(nodes, line);
        }
    }

    std::sort(nodes.begin(), nodes.end());
    nodes.erase(std::unique(nodes.begin(), nodes.end()), nodes.end());
    return nodes;
}

std::vector<std::string> load_node_names_from_gfa(const std::string& gfa_path) {
    Reader reader;
    if (!reader.open(gfa_path)) {
        throw std::runtime_error("Failed to open subgraph GFA: " + gfa_path);
    }

    std::vector<std::string> nodes;
    std::string_view line;
    while (reader.read_line(line)) {
        if (line.empty() || line[0] != 'S') continue;

        const auto t1 = line.find('\t');
        if (t1 == std::string_view::npos) {
            throw std::runtime_error("Malformed S line in subgraph GFA");
        }
        const auto t2 = line.find('\t', t1 + 1);
        if (t2 == std::string_view::npos) {
            throw std::runtime_error("Malformed S line in subgraph GFA");
        }

        nodes.emplace_back(line.substr(t1 + 1, t2 - (t1 + 1)));
    }

    std::sort(nodes.begin(), nodes.end());
    nodes.erase(std::unique(nodes.begin(), nodes.end()), nodes.end());
    return nodes;
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

std::vector<std::uint32_t> resolve_node_names_with_index(
    const indexer::NodeHashIndex& node_index,
    const std::vector<std::string>& node_names) {
    std::vector<std::uint32_t> node_ids;
    node_ids.reserve(node_names.size());

    for (const auto& node_name : node_names) {
        std::uint32_t node_id = 0;
        if (!node_index.lookup_rank(node_name, node_id)) {
            throw std::runtime_error("Node id not found in .ndx: " + node_name);
        }
        node_ids.push_back(node_id);
    }

    std::sort(node_ids.begin(), node_ids.end());
    node_ids.erase(std::unique(node_ids.begin(), node_ids.end()), node_ids.end());
    return node_ids;
}

WalkCoordState load_node_lengths_by_index(const PathIndexReader& index,
                                          const indexer::NodeHashIndex& node_index,
                                          const std::string& source_gfa) {
    WalkCoordState state;
    state.node_lengths.resize(index.node_count());
    std::vector<std::uint64_t> seen_nodes((static_cast<std::size_t>(index.node_count()) + 63) / 64, 0);
    std::uint64_t seen_node_count = 0;

    Reader reader;
    if (!reader.open(source_gfa)) {
        warn_get_path("could not open --source_gfa '" + source_gfa +
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
            warn_get_path("could not derive segment length for node '" + node_name +
                          "', falling back to W subpaths without coordinates");
            return state;
        }

        std::uint32_t node_id = 0;
        if (!node_index.lookup_rank(node_name, node_id)) {
            warn_get_path("node '" + node_name +
                          "' from --source_gfa was not found in .ndx, falling back to W subpaths without coordinates");
            return state;
        }
        if (node_id >= index.node_count()) {
            warn_get_path("node '" + node_name +
                          "' resolved to an id outside the .pdx node range, falling back to W subpaths without coordinates");
            return state;
        }
        if (test_seen_bit(seen_nodes, node_id)) {
            warn_get_path("duplicate source node or .ndx collision for node '" + node_name +
                          "', falling back to W subpaths without coordinates");
            return state;
        }

        set_seen_bit(seen_nodes, node_id);
        ++seen_node_count;
        state.node_lengths[node_id] = node_length;
    }

    if (seen_node_count != index.node_count()) {
        warn_get_path("the node set in --source_gfa does not match the node set in .pdx/.ndx, falling back to W subpaths without coordinates");
        return state;
    }

    state.usable = true;
    return state;
}

PathCoordCacheEntry& get_or_build_path_coord_cache(
    const PathIndexReader& index,
    std::uint32_t path_id,
    const std::vector<std::uint64_t>& node_lengths,
    std::unordered_map<std::uint32_t, PathCoordCacheEntry>& cache) {

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
        warn_get_path("W-line '" + std::string(entry.info.name) +
                      "' is missing SeqStart/SeqEnd, falling back to subwalk output without coordinates");
        return entry;
    }
    if (entry.info.seq_end < entry.info.seq_start) {
        warn_get_path("W-line '" + std::string(entry.info.name) +
                      "' has SeqEnd < SeqStart, falling back to subwalk output without coordinates");
        return entry;
    }

    entry.prefix_lengths.resize(entry.steps.size() + 1, 0);
    for (std::size_t i = 0; i < entry.steps.size(); ++i) {
        const auto node_id = entry.steps[i].node_id;
        if (node_id >= node_lengths.size()) {
            warn_get_path("W-line '" + std::string(entry.info.name) +
                          "' references a node outside the length table, falling back to subwalk output without coordinates");
            entry.prefix_lengths.clear();
            return entry;
        }
        entry.prefix_lengths[i + 1] = entry.prefix_lengths[i] + node_lengths[node_id];
    }

    const auto expected_span = static_cast<std::uint64_t>(entry.info.seq_end - entry.info.seq_start);
    if (entry.prefix_lengths.back() != expected_span) {
        warn_get_path("W-line '" + std::string(entry.info.name) +
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

bool lookup_walk_path_id(const PathIndexReader& index,
                         const std::string& sample_id,
                         std::uint64_t hap_index,
                         const std::string& seq_id,
                         std::int64_t seq_start,
                         std::int64_t seq_end,
                         std::uint32_t& out_path_id,
                         bool& ambiguous) {
    ambiguous = false;
    bool found = false;

    for (std::uint32_t path_id = 0; path_id < index.path_count(); ++path_id) {
        const auto info = index.get_path_info(path_id);
        if (info.record_type != 'W') continue;
        if (info.sample_id != sample_id) continue;
        if (info.hap_index != hap_index) continue;
        if (info.seq_id != seq_id) continue;
        if (seq_start >= 0 && info.seq_start != seq_start) continue;
        if (seq_end >= 0 && info.seq_end != seq_end) continue;

        if (found) {
            ambiguous = true;
            return false;
        }
        found = true;
        out_path_id = path_id;
    }

    return found;
}

}  // namespace

void configure_get_path_parser(argparse::ArgumentParser& parser) {
    parser.add_argument("in_index")
      .help("input path index (.pdx)");

    parser.add_argument("--ndx")
      .default_value(std::string(""))
      .nargs(1)
      .help("path to the node hash index (.ndx); required for node-set queries and exact W subwalk coordinates");

    parser.add_argument("--path_id")
      .default_value(std::string(""))
      .nargs(1)
      .help("canonical path id; for W-lines use sample|hap|seq_id|start|end");

    parser.add_argument("--sample")
      .default_value(std::string(""))
      .nargs(1)
      .help("W-line sample id for structured walk lookup");

    parser.add_argument("--hap_index")
      .default_value(std::string(""))
      .nargs(1)
      .help("W-line haplotype index for structured walk lookup");

    parser.add_argument("--seq_id")
      .default_value(std::string(""))
      .nargs(1)
      .help("W-line sequence id for structured walk lookup");

    parser.add_argument("--seq_start")
      .default_value(std::string(""))
      .nargs(1)
      .help("optional W-line start coordinate or *");

    parser.add_argument("--seq_end")
      .default_value(std::string(""))
      .nargs(1)
      .help("optional W-line end coordinate or *");

    parser.add_argument("--nodes")
      .default_value(std::string(""))
      .nargs(1)
      .help("comma-separated node ids; prints subpaths that stay inside this node set");

    parser.add_argument("--nodes_file")
      .default_value(std::string(""))
      .nargs(1)
      .help("file with node ids, one per line or comma-separated per line");

    parser.add_argument("--subgraph_gfa")
      .default_value(std::string(""))
      .nargs(1)
      .help("subgraph GFA; extracts node ids from its S lines and returns matching P/W subpaths");

    parser.add_argument("--source_gfa")
      .default_value(std::string(""))
      .nargs(1)
      .help("original source GFA used to build the path index; needed for exact W subwalk coordinates");

    parser.add_argument("--with_walk_coords").default_value(false)
      .implicit_value(true)
      .help("for W subwalk output, recompute exact SeqStart/SeqEnd from --source_gfa when possible");
}

int run_get_path(const argparse::ArgumentParser& program) {
    const auto input_index = program.get<std::string>("in_index");
    const auto node_index_path = program.get<std::string>("ndx");
    if (!file_exists(input_index.c_str())) {
        std::cerr << "Path index file does not exist: " << input_index << std::endl;
        return 1;
    }

    const auto path_name = program.get<std::string>("path_id");
    const auto sample_id = program.get<std::string>("sample");
    const auto hap_index_str = program.get<std::string>("hap_index");
    const auto seq_id = program.get<std::string>("seq_id");
    const auto seq_start_str = program.get<std::string>("seq_start");
    const auto seq_end_str = program.get<std::string>("seq_end");
    const auto nodes_csv = program.get<std::string>("nodes");
    const auto nodes_file = program.get<std::string>("nodes_file");
    const auto subgraph_gfa = program.get<std::string>("subgraph_gfa");
    const auto source_gfa = program.get<std::string>("source_gfa");
    const bool with_walk_coords = program.get<bool>("with_walk_coords");

    const bool has_node_query = !nodes_csv.empty() || !nodes_file.empty() || !subgraph_gfa.empty();
    const bool has_structured_walk_query = !sample_id.empty() || !hap_index_str.empty() ||
                                           !seq_id.empty() || !seq_start_str.empty() || !seq_end_str.empty();

    if (path_name.empty() && !has_structured_walk_query && !has_node_query) {
        std::cerr << "Provide --path_id, a structured W lookup, or a node set via --nodes / --nodes_file / --subgraph_gfa" << std::endl;
        return 1;
    }
    if ((!path_name.empty() && has_structured_walk_query) ||
        (!path_name.empty() && has_node_query) ||
        (has_structured_walk_query && has_node_query)) {
        std::cerr << "Use only one lookup mode at a time" << std::endl;
        return 1;
    }
    if (with_walk_coords && !has_node_query) {
        std::cerr << "--with_walk_coords is only supported with node-set queries" << std::endl;
        return 1;
    }
    if (with_walk_coords && source_gfa.empty()) {
        std::cerr << "--with_walk_coords requires --source_gfa" << std::endl;
        return 1;
    }
    if (has_node_query && node_index_path.empty()) {
        std::cerr << "Node-set queries require --ndx so node ids can be resolved without loading a global node map" << std::endl;
        return 1;
    }
    if (has_node_query && !file_exists(node_index_path.c_str())) {
        std::cerr << "Node index file does not exist: " << node_index_path << std::endl;
        return 1;
    }

    try {
        PathIndexReader index(input_index);

        if (!path_name.empty()) {
            std::uint32_t path_id = 0;
            if (!index.lookup_path_id(path_name, path_id)) {
                std::cerr << "Path id not found in index: " << path_name << std::endl;
                return 1;
            }
            write_path_as_gfa_line(std::cout, index, path_id);
            return 0;
        }

        if (has_structured_walk_query) {
            if (sample_id.empty() || hap_index_str.empty() || seq_id.empty()) {
                std::cerr << "Structured W lookup requires --sample, --hap_index, and --seq_id" << std::endl;
                return 1;
            }

            std::uint64_t hap_index = 0;
            try {
                hap_index = static_cast<std::uint64_t>(std::stoull(hap_index_str));
            } catch (const std::exception& err) {
                std::cerr << "Invalid --hap_index value: " << err.what() << std::endl;
                return 1;
            }

            const auto seq_start = parse_optional_int_arg(seq_start_str, "--seq_start");
            const auto seq_end = parse_optional_int_arg(seq_end_str, "--seq_end");

            std::uint32_t path_id = 0;
            bool ambiguous = false;
            if (!lookup_walk_path_id(index, sample_id, hap_index, seq_id, seq_start, seq_end, path_id, ambiguous)) {
                if (ambiguous) {
                    std::cerr << "Structured W lookup is ambiguous; provide --seq_start and --seq_end" << std::endl;
                } else {
                    std::cerr << "No W-line matched the requested fields" << std::endl;
                }
                return 1;
            }

            write_path_as_gfa_line(std::cout, index, path_id);
            return 0;
        }

        std::vector<std::string> node_names;
        if (!subgraph_gfa.empty()) {
            node_names = load_node_names_from_gfa(subgraph_gfa);
        } else {
            node_names = load_node_names(nodes_csv, nodes_file);
        }
        if (node_names.empty()) {
            std::cerr << "No node ids were provided" << std::endl;
            return 1;
        }

        indexer::NodeHashIndex node_index(node_index_path);
        const auto node_ids = resolve_node_names_with_index(node_index, node_names);
        const auto runs = find_subpaths_for_node_ids(index, node_ids);
        if (runs.empty()) {
            std::cerr << "No subpaths overlap the requested node set" << std::endl;
            return 1;
        }

        WalkCoordState walk_coord_state;
        std::unordered_map<std::uint32_t, PathCoordCacheEntry> path_coord_cache;
        if (with_walk_coords) {
            walk_coord_state = load_node_lengths_by_index(index, node_index, source_gfa);
        }

        for (const auto& run : runs) {
            const auto info = index.get_path_info(run.path_id);
            const auto base_name = (info.record_type == 'W') ? info.seq_id : info.name;
            const std::string subpath_name = std::string(base_name) + "#subpath_" +
                std::to_string(run.start_step) + "_" +
                std::to_string(run.start_step + run.step_count - 1);

            if (with_walk_coords && walk_coord_state.usable && info.record_type == 'W') {
                auto& coord_entry = get_or_build_path_coord_cache(index,
                                                                 run.path_id,
                                                                 walk_coord_state.node_lengths,
                                                                 path_coord_cache);
                if (coord_entry.usable) {
                    write_w_subpath_with_coords(std::cout,
                                                index,
                                                coord_entry,
                                                run.start_step,
                                                run.step_count,
                                                subpath_name);
                    continue;
                }
            }

            write_subpath_as_gfa_line(std::cout,
                                      index,
                                      run.path_id,
                                      run.start_step,
                                      run.step_count,
                                      subpath_name);
        }
    } catch (const std::exception& err) {
        std::cerr << err.what() << std::endl;
        return 1;
    }

    return 0;
}

}  // namespace gfaidx::paths

#include "paths/get_path_command.h"

#include <algorithm>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "fs/Reader.h"
#include "fs/fs_helpers.h"
#include "indexer/node_hash_index.h"
#include "paths/path_index.h"
#include "paths/walk_coords.h"
#include "utils/Timer.h"
#include "utils/cli_helpers.h"

namespace gfaidx::paths {
namespace {

void warn_get_path(std::string_view message) {
    std::cerr << get_time() << ": Warning: " << message << std::endl;
}

std::int64_t parse_optional_int_arg(const std::string& value, const char* flag_name) {
    if (value.empty() || value == "*") return -1;
    return utils::parse_i64_strict(value, flag_name);
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

void write_indexed_path_names(std::ostream& out, const PathIndexReader& index) {
    for (std::uint32_t path_id = 0; path_id < index.path_count(); ++path_id) {
        const auto info = index.get_path_info(path_id);
        if (info.record_type == 'W') {
            // W records do not have one path-name field in GFA; print the fields
            // that identify the walk coordinate namespace and interval.
            out << "W\t" << info.sample_id << '\t' << info.hap_index << '\t'
                << info.seq_id << '\t';
            if (info.seq_start >= 0) out << info.seq_start;
            else out << '*';
            out << '\t';
            if (info.seq_end >= 0) out << info.seq_end;
            else out << '*';
            out << '\n';
        } else {
            // For P records the path name is the second field of the original
            // GFA P line, stored as info.name in the path index.
            out << "P\t" << info.name << '\n';
        }
    }
}

}  // namespace

void configure_get_path_parser(argparse::ArgumentParser& parser) {
    parser.add_argument("in_gfa")
      .help("input indexed GFA graph; the path index defaults to <in_gfa>.pdx");

    parser.add_argument("--pdx")
      .default_value(std::string(""))
      .nargs(1)
      .help("optional path to the path index (.pdx); defaults to <in_gfa>.pdx");

    parser.add_argument("--ndx")
      .default_value(std::string(""))
      .nargs(1)
      .help("optional path to the node hash index (.ndx); node-set queries default to <in_gfa>.ndx");

    parser.add_argument("--lnx")
      .default_value(std::string(""))
      .nargs(1)
      .help("optional node length index (.lnx) for coordinate-bearing path output; defaults to <in_gfa>.lnx when present");

    parser.add_argument("--pcx")
      .default_value(std::string(""))
      .nargs(1)
      .help("optional path coordinate checkpoint index (.pcx); defaults to <in_gfa>.pcx when present");

    parser.add_argument("--path_id")
      .default_value(std::string(""))
      .nargs(1)
      .help("canonical path id; for W-lines use sample|hap|seq_id|start|end");

    parser.add_argument("--print_path_names").default_value(false)
      .implicit_value(true)
      .help("print indexed P path names and W walk coordinate identifiers, then exit");

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
      .help("emit W subwalks with exact SeqStart/SeqEnd and P subpaths with path-local coordinate names");
}

int run_get_path(const argparse::ArgumentParser& program) {
    const auto input_graph = program.get<std::string>("in_gfa");
    std::string path_index_path = program.get<std::string>("pdx");
    std::string node_index_path = program.get<std::string>("ndx");

    if (path_index_path.empty() && utils::has_suffix(input_graph, ".pdx")) {
        std::cerr << "get_path expects the indexed graph path as its positional argument, not the .pdx file." << std::endl;
        std::cerr << "Use: gfaidx get_path <indexed.gfa.gz> [query mode options]" << std::endl;
        std::cerr << "If the path index was renamed, pass it with --pdx <path>." << std::endl;
        return 1;
    }

    if (path_index_path.empty()) {
        path_index_path = utils::companion_path(input_graph, ".pdx");
    }
    if (!file_exists(path_index_path.c_str())) {
        if (program.get<std::string>("pdx").empty()) {
            std::cerr << "Path index file does not exist: " << path_index_path << std::endl;
            std::cerr << "Tried inferred companion index: " << path_index_path << std::endl;
            std::cerr << "Use --pdx <path> if the .pdx file was renamed or stored elsewhere." << std::endl;
        } else {
            std::cerr << "Path index file does not exist: " << path_index_path << std::endl;
        }
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
    std::string length_index_path = program.get<std::string>("lnx");
    std::string checkpoint_index_path = program.get<std::string>("pcx");
    const bool lnx_explicit = !length_index_path.empty();
    const bool pcx_explicit = !checkpoint_index_path.empty();
    const bool with_walk_coords = program.get<bool>("with_walk_coords");
    const bool print_path_names = program.get<bool>("print_path_names");

    const bool has_node_query = !nodes_csv.empty() || !nodes_file.empty() || !subgraph_gfa.empty();
    const bool has_structured_walk_query = !sample_id.empty() || !hap_index_str.empty() ||
                                           !seq_id.empty() || !seq_start_str.empty() || !seq_end_str.empty();
    const int query_mode_count = (print_path_names ? 1 : 0) +
                                 (!path_name.empty() ? 1 : 0) +
                                 (has_structured_walk_query ? 1 : 0) +
                                 (has_node_query ? 1 : 0);

    if (query_mode_count == 0) {
        std::cerr << "Provide --print_path_names, --path_id, a structured W lookup, or a node set via --nodes / --nodes_file / --subgraph_gfa" << std::endl;
        return 1;
    }
    if (query_mode_count > 1) {
        std::cerr << "Use only one lookup mode at a time" << std::endl;
        return 1;
    }
    if (with_walk_coords && !has_node_query) {
        std::cerr << "--with_walk_coords is only supported with node-set queries" << std::endl;
        return 1;
    }
    if (with_walk_coords) {
        if (length_index_path.empty()) {
            length_index_path = utils::companion_path(input_graph, ".lnx");
        }
        if (checkpoint_index_path.empty()) {
            checkpoint_index_path = utils::companion_path(input_graph, ".pcx");
        }
        if (lnx_explicit && !file_exists(length_index_path.c_str())) {
            std::cerr << "Node length index does not exist: " << length_index_path << std::endl;
            return 1;
        }
        if (pcx_explicit && !file_exists(checkpoint_index_path.c_str())) {
            std::cerr << "Path checkpoint index does not exist: "
                      << checkpoint_index_path << std::endl;
            return 1;
        }
        if (!file_exists(length_index_path.c_str())) {
            length_index_path.clear();
        }
        if (!file_exists(checkpoint_index_path.c_str())) {
            checkpoint_index_path.clear();
        }
    }

    // Only node-based queries need .ndx. When the user does not provide one,
    // try the companion file path that index_gfa now emits by default.
    if (has_node_query && node_index_path.empty()) {
        node_index_path = utils::companion_path(input_graph, ".ndx");
    }
    if (has_node_query && !file_exists(node_index_path.c_str())) {
        if (program.get<std::string>("ndx").empty()) {
            std::cerr << "Node-set queries require a matching .ndx file." << std::endl;
            std::cerr << "Tried inferred companion index: " << node_index_path << std::endl;
            std::cerr << "Use --ndx <path> if the .ndx file was renamed or stored elsewhere." << std::endl;
        } else {
            std::cerr << "Node index file does not exist: " << node_index_path << std::endl;
        }
        return 1;
    }

    try {
        PathIndexReader index(path_index_path);

        if (print_path_names) {
            write_indexed_path_names(std::cout, index);
            return 0;
        }

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
                hap_index = utils::parse_u64_strict(hap_index_str, "--hap_index");
            } catch (const std::exception& err) {
                std::cerr << err.what() << std::endl;
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

        // W records have SeqStart/SeqEnd fields. P records can use path-local
        // coordinates in their output names when a length source is available.
        bool has_walk_run = false;
        bool has_p_run = false;
        if (with_walk_coords) {
            for (const auto& run : runs) {
                const auto record_type = index.get_path_info(run.path_id).record_type;
                if (record_type == 'W') {
                    has_walk_run = true;
                } else if (record_type == 'P') {
                    has_p_run = true;
                }
                if (has_walk_run && has_p_run) break;
            }
        }
        if (with_walk_coords && has_walk_run && length_index_path.empty() && source_gfa.empty()) {
            std::cerr << "--with_walk_coords requires a companion .lnx, --lnx <path>, or --source_gfa for W subwalk coordinates" << std::endl;
            return 1;
        }

        WalkCoordState walk_coord_state;
        const bool have_length_source = !length_index_path.empty() || !source_gfa.empty();
        if (with_walk_coords && (has_walk_run || (has_p_run && have_length_source))) {
            walk_coord_state = load_node_lengths_by_index(index,
                                                          node_index,
                                                          source_gfa,
                                                          length_index_path,
                                                          checkpoint_index_path,
                                                          warn_get_path);
        }

        for (const auto& run : runs) {
            const auto info = index.get_path_info(run.path_id);
            const auto base_name = (info.record_type == 'W') ? info.seq_id : info.name;
            const std::string subpath_name = std::string(base_name) + "#subpath_" +
                std::to_string(run.start_step) + "_" +
                std::to_string(run.start_step + run.step_count - 1);

            if (with_walk_coords && walk_coord_state.usable &&
                (info.record_type == 'W' || info.record_type == 'P')) {
                bool wrote_coordinates = false;
                if (info.record_type == 'W') {
                    wrote_coordinates = write_w_subpath_with_coords_bounded(std::cout,
                                                                            index,
                                                                            run.path_id,
                                                                            walk_coord_state,
                                                                            run.start_step,
                                                                            run.step_count,
                                                                            subpath_name,
                                                                            warn_get_path);
                } else {
                    wrote_coordinates = write_p_subpath_with_coords_bounded(std::cout,
                                                                            index,
                                                                            run.path_id,
                                                                            walk_coord_state,
                                                                            run.start_step,
                                                                            run.step_count,
                                                                            warn_get_path);
                }
                if (wrote_coordinates) {
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

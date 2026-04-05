#include "paths/get_path_command.h"

#include <algorithm>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

#include "fs/Reader.h"
#include "fs/fs_helpers.h"
#include "paths/path_index.h"

namespace gfaidx::paths {
namespace {

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
}

int run_get_path(const argparse::ArgumentParser& program) {
    const auto input_index = program.get<std::string>("in_index");
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

        const auto runs = find_subpaths_for_nodes(index, node_names);
        if (runs.empty()) {
            std::cerr << "No subpaths overlap the requested node set" << std::endl;
            return 1;
        }

        for (const auto& run : runs) {
            const auto info = index.get_path_info(run.path_id);
            const auto base_name = (info.record_type == 'W') ? info.seq_id : info.name;
            const std::string subpath_name = std::string(base_name) + "#subpath_" +
                std::to_string(run.start_step) + "_" +
                std::to_string(run.start_step + run.step_count - 1);
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

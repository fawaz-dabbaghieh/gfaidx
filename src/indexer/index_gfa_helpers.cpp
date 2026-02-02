#include "index_gfa_helpers.h"

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <map>

// #include <sys/wait.h>

#include <community.h>
#include <graph.h>

#include "fs/Reader.h"
#include "fs/fs_helpers.h"
#include "fs/gfa_line_parsers.h"
#include "utils/Timer.h"

#define WEIGHTED     0
#define UNWEIGHTED   1

namespace gfaidx::indexer {

int nb_pass = 0;
double precision = 0.000001;
int display_level = -1;
int k1 = 16;
unsigned int N_NODES = 0;
unsigned int N_EDGES = 0;


bool command_exists(const std::string& command = "sort") {
    const std::string shell_command = "command -v " + command + " >/dev/null 2>&1";
    int code = std::system(shell_command.c_str());
    if (WEXITSTATUS(code) == 0) return true;
    return false;
}

 void get_int_node_id(std::unordered_map<std::string, unsigned int>& node_id_map,
                            const std::string& node_id,
                            unsigned int& int_id) {
    if (node_id_map.find(node_id) == node_id_map.end()) {
        node_id_map[node_id] = N_NODES;
        int_id = N_NODES;
        N_NODES++;
    } else {
        int_id = node_id_map[node_id];
    }
}

void print_c_stats(const Community& c, int level) {
    std::cout << get_time() << ": level " << level
              << ": network size: "
              << c.g.nb_nodes << " nodes, "
              << c.g.nb_links << " edges" << std::endl;
}


void configure_index_gfa_parser(argparse::ArgumentParser& parser) {
    parser.add_argument("in_gfa")
      .help("input GFA graph");

    parser.add_argument("out_gz")
      .help("output node communities");

    parser.add_argument("--keep_tmp").default_value(false)
      .implicit_value(true)
      .help("keep temporary files");

    parser.add_argument("--tmp_dir").default_value(std::string(""))
      .nargs(1)
      .help("temporary directory base (default: create a unique temp dir)");

    parser.add_argument("--progress_every").default_value(std::string("1000000"))
      .nargs(1)
      .help("print progress every N lines (default: 1000000), give 0 to disable");

    parser.add_argument("--gzip_level").default_value(std::string("6"))
      .nargs(1)
      .help("gzip compression level 1-9 (default: 6)");

    parser.add_argument("--gzip_mem_level").default_value(std::string("8"))
      .nargs(1)
      .help("gzip mem level 1-9 (default: 8)");

    parser.add_argument("--recursive_chunking").default_value(false)
      .implicit_value(true)
      .help("recursively split oversized communities (one extra pass)");

    parser.add_argument("--recursive_max_nodes").default_value(std::string("30000"))
      .nargs(1)
      .help("soft cap for nodes in a community before recursive splitting");

    parser.add_argument("--recursive_max_seq_bp").default_value(std::string("50000000"))
      .nargs(1)
      .help("soft cap for total sequence bp before recursive splitting");

    parser.add_argument("--recursive_max_edges").default_value(std::string("70000"))
      .nargs(1)
      .help("soft cap for intra-community edges before recursive splitting");

    parser.add_argument("--recursive_hard_max_nodes").default_value(std::string("100000"))
      .nargs(1)
      .help("hard cap for nodes in a community (always split)");

    parser.add_argument("--recursive_hard_max_seq_bp").default_value(std::string("300000000"))
      .nargs(1)
      .help("hard cap for total sequence bp (always split)");

    parser.add_argument("--community_stats_tsv").default_value(std::string(""))
      .nargs(1)
      .help("write per-community stats to a TSV file (optional)");
}

bool run_sort(const std::string& input_edges,
              const std::string& output_edges,
              const std::string& tmpdir,
              const std::string& mem,
              bool unique,
              int threads) {

    if (!command_exists("sort")) {
        std::cout << get_time() << ": the command 'sort' does not exist";
        return false;
    }

    if (!dir_exists(tmpdir.c_str())) {
        std::cerr << "Temporary directory does not exist: " << tmpdir << std::endl;
        return false;
    }

    std::string command = "sort -k1,1 -k2,2 -n --parallel " + std::to_string(threads) + " -S " + mem;
    if (unique) command += " -u";
    command += " -T " + tmpdir;
    command += " -o " + output_edges + " " + input_edges;
    std::cout << get_time() << ": Running command: " << command << std::endl;
    int code = std::system(command.c_str());
    return (WEXITSTATUS(code) == 0);
}

void generate_edgelist(const std::string& input_gfa,
                       const std::string& tmp_edgelist,
                       std::unordered_map<std::string, unsigned int>& node_id_map,
                       const Reader::Options& reader_options) {
    std::string_view line;

    Reader file_reader(reader_options);
    if (!file_reader.open(input_gfa)) {
        std::cerr << "Could not open file: " << input_gfa << std::endl;
        exit(1);
    }

    std::ofstream out;
    out.open(tmp_edgelist);
    bool first_line = true;
    std::cout << get_time() << ": Reading the GFA file " << input_gfa << std::endl;

    while (file_reader.read_line(line)) {
        if (line[0] == 'L') {
            N_EDGES++;
            auto [fst, snd] = extract_L_nodes(line);
            unsigned int src, dest;
            get_int_node_id(node_id_map, fst, src);
            get_int_node_id(node_id_map, snd, dest);
            if (first_line) {
                if (src > dest) out << dest << " " << src;
                else out << src << " " << dest;
                first_line = false;
            } else {
                if (src > dest) out << "\n" << dest << " " << src;
                else out << "\n" << src << " " << dest;
            }
        }
    }
    out.close();
}

void output_communities(const BGraph& g,
                        const std::string& out_file,
                        const std::unordered_map<std::string, unsigned int>& node_id_map) {
    std::vector<std::string> id_to_node(node_id_map.size());

    for (const auto& p : node_id_map) {
        id_to_node[p.second] = p.first;
    }

    if (!file_writable(out_file.c_str())) {
        std::cerr << "Output file is not writable: " << out_file << std::endl;
        exit(1);
    }

    ofstream out;
    out.open(out_file);
    for (size_t i = 0; i < g.nodes.size(); i++) {
        out << "Community_" << i << ": ";
        for (int j : g.nodes[i]) {
            out << id_to_node[j] << " ";
        }
        out << std::endl;
    }
    out.close();
}

void generate_communities(const std::string &binary_graph,
                          BGraph &g) {
    Community c(binary_graph.c_str(), nullptr, UNWEIGHTED, -1, precision);
    bool improvement = true;
    double mod = c.modularity();
    int level = 0;

    int iterations = 0;
    while ((iterations < 50) & improvement) {
        iterations++;
        print_c_stats(c, level);
        improvement = c.one_level();
        const double new_mod = c.modularity();
        level++;
        g = c.partition2graph_binary();
        c = Community(g, -1, precision);
        std::cout << get_time() << ": old modularity is " << mod << " and new modularity is " << new_mod << std::endl;
        mod = new_mod;
    }
}

void add_singleton_community(const std::string& input_gfa,
                             std::unordered_map<std::string, unsigned int>& node_id_map,
                             BGraph& g,
                             const Reader::Options& reader_options) {
    std::string_view line;
    Reader file_reader(reader_options);
    if (!file_reader.open(input_gfa)) {
        std::cerr << "Could not open file: " << input_gfa << std::endl;
        exit(1);
    }

    std::vector<int> singleton_nodes;
    std::string node_id;
    std::string node_seq;

    while (file_reader.read_line(line)) {
        if (line.empty() || line[0] != 'S') {
            continue;
        }
        extract_S_node(line, node_id, node_seq);
        if (node_id_map.find(node_id) != node_id_map.end()) {
            continue;
        }

        unsigned int int_id = 0;
        get_int_node_id(node_id_map, node_id, int_id);
        singleton_nodes.push_back(static_cast<int>(int_id));
    }

    if (!singleton_nodes.empty()) {
        g.nodes.push_back(singleton_nodes);
        g.nb_nodes = g.nodes.size();
        std::cout << get_time() << ": Added " << singleton_nodes.size()
                  << " singleton nodes to community " << (g.nodes.size() - 1) << std::endl;
    } else {
        std::cout << get_time() << ": No singleton nodes found" << std::endl;
    }
}

}  // namespace gfaidx::indexer

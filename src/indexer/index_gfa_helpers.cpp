#include "index_gfa_helpers.h"

#include <algorithm>
#include <charconv>
#include <chrono>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <map>
#include <utility>
#include <vector>

#include <sys/wait.h>
#include <unistd.h>

#include <community.h>
#include <graph.h>

#include "fs/Reader.h"
#include "fs/fs_helpers.h"
#include "fs/gfa_line_parsers.h"
#include "paths/path_coordinate_checkpoints.h"
#include "paths/path_index.h"
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
std::string in_gfa="in_gfa";
std::string out_graph="out_indexed_graph";

namespace {

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

}  // namespace

void configure_index_gfa_parser(argparse::ArgumentParser& parser) {
    parser.add_argument(in_gfa)
      .help("input GFA graph");

    parser.add_argument(out_graph)
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

    parser.add_argument("--no_paths").default_value(false)
      .implicit_value(true)
      .help("skip building the .pdx path index; still write .gz, .idx, .ndx, and .lnx");

    parser.add_argument("--checkpoint_steps")
      .default_value(std::to_string(
          paths::kDefaultPathCheckpointStride))
      .nargs(1)
      .help("store one .pcx coordinate checkpoint every N path steps; defaults to 4096");

    parser.add_argument("--max_chunk_nodes").default_value(std::string("0"))
      .nargs(1)
      .help("re-run Louvain inside communities at least this large and cap small-community merges at this size; 0 disables refinement and leaves merging uncapped");

    // Keep small-community coarsening opt-in so existing indexing runs preserve
    // their current partition unless the caller provides a minimum chunk size.
    parser.add_argument("--min_chunk_nodes").default_value(std::string("0"))
      .nargs(1)
      .help("merge communities smaller than this into their strongest eligible neighbor; 0 disables");

    // auto numbers nodes by community and then by a locality key so path
    // steps delta-code well; legacy_hash reproduces the pre-v2 hash-rank ids
    // and the v4 .pdx byte for byte, mainly for A/B comparisons.
    parser.add_argument("--node_order").default_value(std::string("auto"))
      .nargs(1)
      .choices("auto", "legacy_hash")
      .help("node id order: auto (community, then integer name or first appearance) or legacy_hash (pre-v2 .ndx and v4 .pdx)");

    parser.add_argument("--step_encoding").default_value(std::string("delta"))
      .nargs(1)
      .choices("delta", "packed32")
      .help(".pdx step table: delta (zigzag varint blocks) or packed32 (4 bytes per step); legacy_hash forces packed32");

    parser.add_argument("--posting_encoding").default_value(std::string("slots"))
      .nargs(1)
      .choices("slots", "groups")
      .help(".pdx postings: slots (predicted from neighbouring node ids) or groups (v4 per-path groups); legacy_hash forces groups");

    parser.add_argument("--posting_block_nodes")
      .default_value(std::to_string(paths::kDefaultPostingBlockNodes))
      .nargs(1)
      .help("node ids per independently decodable posting block for --posting_encoding slots");

    parser.add_argument("--threads")
      .default_value(std::string("1"))
      .nargs(1)
      .help("threads for building the path index; the output is the same for every thread count");
}

bool run_sort(const std::string& input_edges,
              const std::string& output_edges,
              const std::string& tmpdir,
              const std::string& mem,
              bool unique,
              int threads) {
    if (!dir_exists(tmpdir.c_str())) {
        std::cerr << "Temporary directory does not exist: " << tmpdir << std::endl;
        return false;
    }

    std::vector<std::string> args{
        "sort",
        "-k1,1",
        "-k2,2",
        "-n",
        "--parallel",
        std::to_string(threads),
        "-S",
        mem
    };
    if (unique) {
        args.emplace_back("-u");
    }
    args.emplace_back("-T");
    args.emplace_back(tmpdir);
    args.emplace_back("-o");
    args.emplace_back(output_edges);
    args.emplace_back(input_edges);

    std::cout << get_time() << ": Running command: sort -k1,1 -k2,2 -n --parallel "
              << threads << " -S " << mem;
    if (unique) {
        std::cout << " -u";
    }
    std::cout << " -T '" << tmpdir
              << "' -o '" << output_edges
              << "' '" << input_edges << "'" << std::endl;

    std::vector<char*> argv;
    argv.reserve(args.size() + 1);
    for (auto& arg : args) {
        argv.push_back(arg.data());
    }
    argv.push_back(nullptr);

    const pid_t pid = fork();
    if (pid < 0) {
        std::cerr << "Failed to start sort: " << std::strerror(errno) << std::endl;
        return false;
    }

    if (pid == 0) {
        execvp(argv[0], argv.data());
        std::cerr << "Failed to exec sort: " << std::strerror(errno) << std::endl;
        _exit(127);
    }

    int status = 0;
    while (waitpid(pid, &status, 0) == -1) {
        if (errno == EINTR) {
            continue;
        }
        std::cerr << "waitpid failed while waiting for sort: " << std::strerror(errno) << std::endl;
        return false;
    }

    if (WIFEXITED(status)) {
        if (WEXITSTATUS(status) == 0) {
            return true;
        }
        std::cerr << "sort exited with code " << WEXITSTATUS(status) << std::endl;
        return false;
    }

    if (WIFSIGNALED(status)) {
        std::cerr << "sort was terminated by signal " << WTERMSIG(status) << std::endl;
        return false;
    }

    std::cerr << "sort ended in an unexpected state" << std::endl;
    return false;
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
        if (line.empty()) {
            continue;
        }

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

std::vector<double> estimate_edge_step_bytes(
    const std::string& sorted_edgelist,
    const std::vector<const std::vector<std::uint32_t>*>& candidate_node_ids) {
    std::vector<std::uint64_t> total_bytes(candidate_node_ids.size(), 0);
    std::uint64_t edges = 0;

    Reader reader;
    if (!reader.open(sorted_edgelist)) {
        throw std::runtime_error("Failed to open sorted edge list: " + sorted_edgelist);
    }
    std::string_view line;
    while (reader.read_line(line)) {
        const auto space = line.find(' ');
        if (space == std::string_view::npos) continue;
        std::uint32_t u = 0;
        std::uint32_t v = 0;
        const auto ru = std::from_chars(line.data(), line.data() + space, u);
        const auto rv = std::from_chars(line.data() + space + 1, line.data() + line.size(), v);
        if (ru.ec != std::errc() || rv.ec != std::errc()) {
            throw std::runtime_error("Malformed sorted edge list line while scoring node orders");
        }
        for (std::size_t c = 0; c < candidate_node_ids.size(); ++c) {
            const auto& ids = *candidate_node_ids[c];
            if (u >= ids.size() || v >= ids.size()) {
                throw std::runtime_error("Edge list node id out of range while scoring node orders");
            }
            // Same value layout as the delta step encoder: zigzag delta, then
            // one strand bit, as LEB128.
            const std::int64_t delta = static_cast<std::int64_t>(ids[v]) - static_cast<std::int64_t>(ids[u]);
            std::uint64_t value = ((static_cast<std::uint64_t>(delta) << 1) ^
                                   static_cast<std::uint64_t>(delta >> 63)) << 1;
            std::uint64_t bytes = 1;
            while (value >= 0x80) {
                value >>= 7;
                ++bytes;
            }
            total_bytes[c] += bytes;
        }
        ++edges;
    }

    std::vector<double> out(candidate_node_ids.size(), 0.0);
    for (std::size_t c = 0; c < out.size(); ++c) {
        out[c] = edges == 0 ? 0.0 : static_cast<double>(total_bytes[c]) / static_cast<double>(edges);
    }
    return out;
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

void generate_communities(const std::string& binary_graph,
                          BGraph& g,
                          int display_level,
                          bool verbose,
                          std::uint32_t max_piece_nodes) {
    Community c(binary_graph.c_str(), nullptr, UNWEIGHTED, -1, precision);
    bool improvement = true;
    double mod = c.modularity();
    int level = 0;

    int iterations = 0;
    while ((iterations < 50) & improvement) {
        iterations++;
        print_c_stats(c, level);

        // Time the local-moving phase independently from graph contraction and
        // construction of the next level so later optimizations can be measured
        // against a clear per-level baseline.
        Timer one_level_timer;
        improvement = c.one_level();
        const double one_level_seconds = one_level_timer.elapsed();
        std::cout << get_time() << ": Louvain level " << level
                  << " local moving finished in " << one_level_seconds
                  << " seconds; nodes moved: " << (improvement ? "yes" : "no")
                  << std::endl;

        const double new_mod = c.modularity();
        level++;
        BGraph next = c.partition2graph_binary();
        if (max_piece_nodes > 0 && iterations > 1) {
            std::size_t largest = 0;
            for (const auto& members : next.nodes) largest = std::max(largest, members.size());
            if (largest > max_piece_nodes) {
                std::cout << get_time() << ": Keeping Louvain level " << level - 1
                          << " because level " << level << " would create a community of "
                          << largest << " nodes, above the limit of " << max_piece_nodes
                          << std::endl;
                break;
            }
        }
        g = std::move(next);
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

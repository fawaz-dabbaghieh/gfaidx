//
// Created by Fawaz Dabbaghie on 05/12/2025.
//

#include <chrono>
#include <fstream>
#include <iostream>
#include <map>

#include <argparse/argparse.hpp>
#include <community.h>
#include <graph.h>
#include <graph_binary.h>

#include "fs/fs_helpers.h"
#include "fs/Reader.h"
#include "fs/gfa_line_parsers.h"
#include "utils/Timer.h"


#define WEIGHTED     0
#define UNWEIGHTED   1

int nb_pass    = 0;
double precision = 0.000001;
int display_level = -1;
int k1 = 16;
unsigned int N_NODES = 0;
unsigned int N_EDGES = 0;


bool command_exists(const std::string& command="sort") {
    const std::string shell_command = "command -v " + command + " >/dev/null 2>&1";
    int code = std::system(shell_command.c_str());
    if (WEXITSTATUS(code) == 0) return true; // Command was found
    return false;
}

bool run_sort(const std::string& input_edges, const std::string& output_edges, const std::string& tmpdir = ".",
    const std::string& mem="50%", const bool unique = true, const int threads = 1) {

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

// std::pair<std::string, std::string> split_string(const std::string& line, char delimiter = '\t') {
//     std::vector<std::string> tokens;
//     std::string currentToken;
//     for (const char c : line) {
//         if (c == delimiter) {
//             tokens.push_back(currentToken);
//             currentToken.clear();
//         } else {
//             currentToken += c;
//         }
//     }
//     tokens.push_back(currentToken); // Add the last token
//     // an L line should be at least 5 items
//     if (tokens.size() < 5) {
//         std::cerr << "Offending L line: " << line << std::endl;
//         exit(1);
//     }
//     return {tokens[1], tokens[3]};
// }


inline void get_int_node_id(std::unordered_map<std::string, unsigned int>& node_id_map, const std::string& node_id, unsigned int &int_id) {
    if (node_id_map.find(node_id) == node_id_map.end()) { // new node
        node_id_map[node_id] = N_NODES;
        int_id = N_NODES;
        N_NODES++;
    } else {
        int_id = node_id_map[node_id];
    }
}

void parse_args(int argc, char** argv, argparse::ArgumentParser& parser) {
    // std::string input_gfa;
    parser.add_argument("in_gfa")
      .help("input GFA graph");

    // std::string output_bin;
    parser.add_argument("out_communities")
      .help("output node communities");

    // bool keep_tmp;
    parser.add_argument("--keep_tmp").default_value(false)
      .implicit_value(true)
      .help("keep temporary files");

    try {
        parser.parse_args(argc, argv);
    }
    catch (const std::exception& err) {
        std::cerr << err.what() << std::endl;
        std::cerr << parser;
        exit(1);
    }
}


void generate_edgelist(const std::string& input_gfa, const std::string& tmp_edgelist,
    std::unordered_map<std::string, unsigned int>& node_id_map) {
    std::string_view line;

    Reader file_reader;
    if (!file_reader.open(input_gfa)) {
        std::cerr << "Could not open file: " << input_gfa << std::endl;
        exit(1);
    }

    // std::ifstream in(input_gfa);
    // if (!in.good()) {
        // std::cerr << "Could not open input file: " << input_gfa << std::endl; exit(1);
    // }

    std::ofstream out;
    out.open(tmp_edgelist);
    // Graph graph;
    bool first_line = true;
    unsigned int line_counter = 0;
    std::cout << get_time() << ": Reading the GFA file " << input_gfa << std::endl;

    // while (std::getline(in, line)) {
    std::unordered_map<std::string, std::vector<std::string>> paths_map;

    while (file_reader.read_line(line)) {
        if (line_counter % 500000 == 0) std::cout << get_time() << ": Read " << line_counter << " lines" << std::endl;
        line_counter++;
        if (line[0] == 'L') {
            N_EDGES++;
            auto [fst, snd] = extract_L_nodes(line);
            unsigned int src, dest;
            get_int_node_id(node_id_map, fst, src);
            get_int_node_id(node_id_map, snd, dest);

            // to avoid the last empty line
            if (first_line) {
                if (src > dest) out << dest << " " << src;
                else out << src << " " << dest;
                first_line = false;
            } else {
                if (src > dest) out << "\n" << dest << " " << src;
                else out << "\n" << src << " " << dest;
            }
        }
        else if (line[0] == 'P'){
            std::string path_name;
            std::vector<std::string> node_list;
            extract_P_nodes(line, path_name, node_list);
            paths_map[path_name] = node_list;
        }
    }
    out.close();
    // in.close();
}


inline void print_c_stats(const Community& c, const int level) {
    std::cout << get_time () << ": level " << level
    << ": network size: "
    << c.g.nb_nodes << " nodes, "
    << c.g.nb_links << " edges" << std::endl;
}


void output_communities(const BGraph& g, const std::string& out_file, const std::unordered_map<std::string, unsigned int>& node_id_map) {
    std::vector<std::string> id_to_node(node_id_map.size());
    for (const auto& p : node_id_map) id_to_node[p.second] = p.first;

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
            // out<< j << " ";
        }
        out << std::endl;
    }
    out.close();
}

void generate_communities(const std::string& binary_graph, BGraph& g, int display_lever=-1, bool verbose=false) {
    Community c(binary_graph.c_str(), nullptr, UNWEIGHTED, -1, precision);
    // BGraph g;
    bool improvement = true;
    double mod = c.modularity();
    int level=0;

    int iterations = 0;
    // added the upper bound on iteration just to not get stuck in an endless loop during bugs
    while ((iterations < 50) & improvement) {
        iterations++;
        print_c_stats(c, level);
        improvement = c.one_level();
        const double new_mod = c.modularity();
        level++;
        g = c.partition2graph_binary();
        c = Community(g, -1, precision);
        std::cout << get_time() << ": old modularity is " << mod << " and new modularity is " << new_mod<< std::endl;
        mod=new_mod;
    }
}


int main(int argc, char** argv) {

    /*
     * parse arguments and check inputs/outputs
     */
    Timer total_time;
    argparse::ArgumentParser program("gfaidx", "0.1.0");
    parse_args(argc, argv, program);

    auto input_gfa = program.get<std::string>("in_gfa");
    if (!file_exists(input_gfa.c_str())) {
        std::cerr << "Input file does not exist: " << input_gfa << std::endl; return 1;
    }

    auto out_comms = program.get<std::string>("out_communities");
    if (!file_writable(out_comms.c_str())) {
        std::cerr << "Output file is not writable: " << out_comms << std::endl; return 1;
    }

    bool keep_tmp = program.get<bool>("keep_tmp");

    // const std::string input_gfa = argv[1];
    std::ifstream in(input_gfa);
    if (!in.good()) {
        std::cerr << "Could not open input file: " << input_gfa << std::endl; return 1;
    }

    Timer timer;
    /*
     * creating the edge list from the GFA file
     */
    // todo: I probably want to change this
    //     also change the hard-coded tmp locations to be in the specified tmp file
    std::unordered_map<std::string, unsigned int> node_id_map;
    std::string tmp_edgelist = "../test_graphs/tmp_edgelist.txt";
    // generates the edge list from the GFA with integer node IDs
    std::cout << get_time() << ": Generating the edges list" << std::endl;
    timer.reset();
    generate_edgelist(input_gfa, tmp_edgelist, node_id_map);
    std::cout << get_time() << ": Finished generating the edges list in " << timer.elapsed() << " seconds" << std::endl;
    std::cout << get_time() << ": The GFA has " << N_NODES << " S lines, and " << N_EDGES << " L lines"<< std::endl;

    /*
     * sorting the edge list with linux sort
     */
    std::string sorted_tmp_edgelist = "../test_graphs/tmp_edgelist_sorted.txt";
    std::string tmp_dir = "../test_graphs/";
    timer.reset();
    std::cout << get_time() << ": Sorting the edges" << std::endl;
    run_sort(tmp_edgelist, sorted_tmp_edgelist, tmp_dir);
    std::cout << get_time() << ": Finished sorting the edges in " << timer.elapsed() << " seconds" << std::endl;

    /*
     * loading the graph from the edge list with the louvain graph class
     */
    std::cout << get_time() << ": Loading the edge list from disk" << std::endl;
    timer.reset();
    Graph graph(sorted_tmp_edgelist.c_str(), UNWEIGHTED);
    std::cout << get_time() << ": Finished loading the edge list from disk in " << timer.elapsed() << " seconds" << std::endl;

    /*
     * converting the graph to binary format for faster processing
     */
    // graph.renumber(UNWEIGHTED);
    std::string tmp_binary = "../test_graphs/tmp_binary.bin";
    std::cout <<  get_time() << ": Saving the graph as compressed a binary to disk to: " << out_comms << std::endl;
    timer.reset();
    graph.display_binary(tmp_binary.c_str(), nullptr, UNWEIGHTED);
    std::cout <<  get_time() << ": Finished saving the binary graph to disk in " << timer.elapsed() << " seconds" << std::endl;

    /*
     * performing community detection on the binary graph
     */
    timer.reset();
    std::cout << get_time () << ": Starting community detection" << std::endl;
    BGraph final_graph;
    generate_communities(tmp_binary, final_graph, display_level);
    std::cout << get_time () << ": Finished community detection in " << timer.elapsed() << " seconds" << std::endl;

    std::cout << get_time () << ": Outputting the communities" << std::endl;
    output_communities(final_graph, out_comms, node_id_map);

    if (!keep_tmp) {
        std::cout << get_time () << ": Removing the temporary files" << std::endl;
        remove_file(tmp_edgelist.c_str());
        remove_file(sorted_tmp_edgelist.c_str());
        remove_file(tmp_binary.c_str());
    }

    std::cout << get_time () << ": Finished in total time of " << total_time.elapsed() << " seconds" << std::endl;

    return 0;
}

// the groupings are in the final graph
// final_graph.nodes is a vector of vectors of ints, the index is the community number
// and the inside vector has the node IDs groupings basically
// so I now have the association between the int node IDs, and the community ID
// I can fill this information in the map maybe
// so I have string_id: <int_id, community_id>
// I can now loop through the original GFA file and start sorting the lines
// i.e., for each line in the GFA, write it in its corresponding community file
//    if an edge belong to two different communities, write it in its own file
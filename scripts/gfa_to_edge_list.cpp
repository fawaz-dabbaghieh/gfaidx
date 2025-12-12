//
// Created by Fawaz Dabbaghie on 05/12/2025.
//

#include <chrono>
#include <fstream>
#include <iostream>
#include <map>

#include <argparse/argparse.hpp>
#include <sys/stat.h>

#include "../src/graph.h"
#include "../src/graph_binary.h"


#define WEIGHTED     0
#define UNWEIGHTED   1

unsigned int N_LINKS = 0;


bool file_exists(const char* file_name) {
    struct stat st{};
    return stat(file_name, &st) == 0;
}

bool dir_exists(const char* dir_name) {
    struct stat st{};
    return stat(dir_name, &st) == 0 && S_ISDIR(st.st_mode);
}

bool file_writable(const char* file_name) {
    std::ofstream f(file_name);
    return f.good();
}

bool remove_file(const char * file ) {
    if (std::remove(file) != 0){
        std::cerr << "Error: could not remove file " << file << std::endl;
        return false;
    }
    return true;
}

inline std::string get_time() {
    std::chrono::time_point now = std::chrono::system_clock::now();
    time_t in_time_t = std::chrono::system_clock::to_time_t(now);
    std::string time_str = std::ctime(&in_time_t);
    time_str.pop_back();
    return time_str;
}


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

    // if (!dir_exists(tmpdir))
    // todo add checks for input, output and tmp directory
    std::string command = "sort -k1,1 -k2,2 -n --parallel " + std::to_string(threads) + " -S " + mem;
    if (unique) command += " -u";
    command += " -T " + tmpdir;
    command += " -o " + output_edges + " " + input_edges;
    std::cout << get_time() << ": Running command: " << command << std::endl;
    int code = std::system(command.c_str());

    return (WEXITSTATUS(code) == 0);
}

std::pair<std::string, std::string> split_string(const std::string& line, char delimiter = '\t') {
    std::vector<std::string> tokens;
    std::string currentToken;
    for (char c : line) {
        if (c == delimiter) {
            tokens.push_back(currentToken);
            currentToken.clear();
        } else {
            currentToken += c;
        }
    }
    tokens.push_back(currentToken); // Add the last token
    // an L line should be at least 5 items
    if (tokens.size() < 5) {
        std::cerr << "Offending L line: " << line << std::endl;
        exit(1);
    }
    return {tokens[1], tokens[3]};
}


inline void get_int_node_id(std::map<std::string, unsigned int>& node_id_map, const std::string& node_id, unsigned int &int_id) {
    if (node_id_map.find(node_id) == node_id_map.end()) { // new node
        node_id_map[node_id] = N_LINKS;
        int_id = N_LINKS;
        N_LINKS++;
        // return node_id_map.size();
    } else {
        int_id = node_id_map[node_id];
    }
    // return node_id_map[node_id];
}

// I can start filling the graph from louvain method here by adding one connection at a time
// convert to binary graph.display_binary()
// clean everything to save memory
// output the node_str_id -> int_id map into a file
// sort it on disk with sort to save memory
// do the community detection on the binary graph file
// output the highest level to disk (or maybe I can access it internally easily, not sure yet)
// read the sorted node_str_ids but only the first column and put that in a vector
// I can put that in a vector as well I think because the node IDs are ints and sorted
int main(int argc, char** argv) {
    // if (argc < 3) {
    //     std::cerr << "usage: " << argv[0] << " <input_gfa> <output_edges>\n"; return 1;
    // }

    argparse::ArgumentParser program("program_name");

    std::string input_gfa;
    program.add_argument("in_gfa")
      .help("input GFA graph").store_into(input_gfa);

    std::string output_bin;
    program.add_argument("out_bin")
      .help("output binary graph").store_into(output_bin);

    try {
        program.parse_args(argc, argv);
    }
    catch (const std::exception& err) {
        std::cerr << err.what() << std::endl;
        std::cerr << program;
        return 1;
    }

    std::cout << input_gfa << " " << output_bin << std::endl;

    if (!file_exists(input_gfa.c_str())) {
        std::cerr << "Input file does not exist: " << argv[1] << std::endl; return 1;
    }

    // const std::string input_gfa = argv[1];
    std::string line;
    std::ifstream in(input_gfa);
    if (!in.good()) {
        std::cerr << "Could not open input file: " << input_gfa << std::endl; return 1;
    }

    std::map<std::string, unsigned int> node_id_map;

    ofstream out;
    out.open("../test_graphs/tmp_edgelist.txt");
    // Graph graph;
    bool first_line = true;
    unsigned int line_counter = 0;
    std::cout << get_time() << ": Reading the edges from the GFA file and writing out" << std::endl;
    while (std::getline(in, line)) {
        if (line_counter % 500000 == 0) std::cout << get_time() << ": Read " << line_counter << " lines" << std::endl;
        line_counter++;
        // todo: need to change this later to better way of reading the big GFA files, buffer or mmap
        if (line[0] == 'L') {
            std::pair<std::string, std::string> edge = split_string(line);
            unsigned int src, dest;
            get_int_node_id(node_id_map, edge.first, src);
            get_int_node_id(node_id_map, edge.second, dest);

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
    }
    out.close();
    in.close();

    // uint32_t n_nodes = node_id_map.size();
    // graph.nodes.resize(n_nodes);
    // for (auto& pair : node_id_map) {
        // graph.nodes[pair.second - 1] = pair.second;
    // }

    std::string edge_list = "../test_graphs/tmp_edgelist.txt";
    std::string sorted_edge_list = "../test_graphs/tmp_edgelist_sorted.txt";
    std::string temp_dir = "../test_graphs/";

    std::cout << get_time() << ": Sorting the edges" << std::endl;
    run_sort(edge_list, sorted_edge_list, temp_dir);
    std::cout << get_time() << ": Finished sorting the edges: " << std::endl;

    std::cout << get_time() << ": Loading the graph from disk" << std::endl;
    // std::string x = "../test_graphs/tmp_edgelist_sorted.txt";
    size_t x_size = sorted_edge_list.length() + 1;
    char x_array[x_size];
    // Copy the content using strcpy
    strcpy(x_array, sorted_edge_list.c_str());
    // Graph graph(x_array, UNWEIGHTED);
    Graph graph(x_array, UNWEIGHTED);
    std::cout << get_time() << ": Finished loading the graph from disk" << std::endl;

    // graph.renumber(UNWEIGHTED);
    std::cout <<  get_time() << ": Saving the graph as a binary to disk to: " << output_bin << std::endl;
    graph.display_binary(output_bin.c_str(), nullptr, UNWEIGHTED);
    std::cout <<  get_time() << ": Finished saving the binary graph to disk" << std::endl;

    std::cout << get_time () << ": Loading the binary graph from disk" << std::endl;
    BGraph bgraph(output_bin.c_str(), nullptr, UNWEIGHTED);
    // bgraph.display();
    // Graph g("../test_graphs/", 1);
    // std::cout << n_nodes << std::endl;
    return 0;
}
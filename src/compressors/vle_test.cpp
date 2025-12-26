//
// Created by Fawaz Dabbaghie on 19/12/2025.
//

#include <fstream>
#include "compressors.h"
#include "../fs/Reader.h"
#include "../fs/gfa_line_parsers.h"


unsigned int N_NODES = 0;


inline void get_int_node_id(std::unordered_map<std::string, unsigned int>& node_id_map, const std::string& node_id, unsigned int &int_id) {
    if (node_id_map.find(node_id) == node_id_map.end()) { // new node
        node_id_map[node_id] = N_NODES;
        int_id = N_NODES;
        N_NODES++;
    } else {
        int_id = node_id_map[node_id];
    }
}


int main(int argc, char** argv) {
    if (argc < 3) {
        std::cerr << "usage: " << argv[0] << " <input_gfa> <out_compressed_paths>\n";
        return 1;
    }
    Reader r;
    r.open(argv[1]);
    std::string_view line;
    std::unordered_map<std::string, unsigned int> node_id_map;
    long int progress_counter = 0;
    while (r.read_line(line)) {
        // I know that all the L lines will be first
        if (line[0] == 'L') {
            progress_counter++;
            if (progress_counter % 5000000 == 0) {
                std::cout << "Number of L lines processed: " << progress_counter << std::endl;
            }

            auto [first, second] = extract_L_nodes(line);
            unsigned int src, dest;
            get_int_node_id(node_id_map, first, src);
            get_int_node_id(node_id_map, second, dest);
        }
    }
    r.close();

    int n_paths = 0;
    std::ofstream out(argv[2], std::ios::binary);
    r.open(argv[1]);
    while (r.read_line(line)) {
        if (line[0] == 'P') {
            n_paths++;
            std::string path_name;
            std::vector<std::string> node_list;

            extract_P_nodes(line, path_name, node_list);
            std::vector<uint8_t> encoded_path;
            encode_path_string_ids_u32(node_list, node_id_map, encoded_path);
            // I want to write the path name with a separating character at the end so I know when I'm reading the path
            // name and when I'm reading the integers
            out.write(path_name.c_str(), path_name.size());
            constexpr char separator = '\0';
            out.write(&separator, 1);
            out.write(reinterpret_cast<const char*>(encoded_path.data()), encoded_path.size());
            out.write(&separator, 1);
            if (n_paths % 1000 == 0) {
                std::cout << "Number of paths processed: " << n_paths << std::endl;
            }
        }
    }
    out.close();

    std::cout << "Number of paths compressed: " << n_paths << std::endl;

    return 0;
}

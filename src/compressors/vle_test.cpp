//
// Created by Fawaz Dabbaghie on 19/12/2025.
//

#include <fstream>
#include "compressors.h"
#include "../fs/Reader.h"



int main(int argc, char** argv) {
    if (argc < 3) {
        std::cerr << "usage: " << argv[0] << " <input_gfa> <out_compressed_paths>\n";
        return 1;
    }
    Reader r;
    r.open(argv[1]);
    std::string_view line;
    int n_paths = 0;

    while (r.read_line(line)) {
        if (line[0] == 'P') {
            n_paths++;

        }
    }

    std::cout << "Number of paths compressed: " << n_paths << std::endl;

    return 0;
}
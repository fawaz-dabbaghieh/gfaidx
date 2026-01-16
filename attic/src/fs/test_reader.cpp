//
// Created by Fawaz Dabbaghie on 17/12/2025.
//
#include <iostream>
#include <fstream>

#include "Reader.h"


int main(int argc, char** argv) {
    if (argc < 2) { std::cerr << "usage: " << argv[0] << " <gfa_file>\n"; return 1; }
    std::string test_out = "../test_graphs/test_reader_out.gfa";

    Reader r;
    if (!r.open(argv[1])) {
        std::cerr << "Could not open file: " << argv[1] << std::endl;
        return 1;
    }

    std::string_view line;
    uint s_number = 0;
    uint l_number = 0;
    int counter = 0;

    std::ofstream out(test_out);
    while (r.read_line(line)) {
        if (line.empty()) continue;
        counter ++;
	// if (counter % 1000000 == 0) std::cout << "Lines processed: " << counter << std::endl;
        // if (counter < 200) std::cout << line     << std::endl;
        // out << line << std::endl;
        if (line[0] == 'S') {
            s_number++;
            // parse segment fields by scanning for '\t'
        } else if (line[0] == 'L') {
            l_number++;
            // parse link fields by scanning for '\t'
        }
    }
    // out.close();
    std::cout << "Number of segments: " << s_number << std::endl;
    std::cout << "Number of links: " << l_number << std::endl;
    std::cout << "Number of total lines: " << counter << std::endl;
}

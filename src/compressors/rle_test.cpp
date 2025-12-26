//
// Created by Fawaz Dabbaghie on 19/12/2025.
//

#include <fstream>
#include "compressors.h"
#include "../fs/Reader.h"


int main(int argc, char** argv) {
    if (argc < 4) { std::cerr << "usage: " << argv[0] << " <gfa_file> <output_seqs> <output_seqs_rle>\n"; return 1; }
    const std::string gfa_file = argv[1];
    std::ifstream in(gfa_file);
    if (!in.good()) {
        std::cerr << "Could not open input file: " << gfa_file << std::endl; return 1;
    }

    std::string_view line;
    Reader file_reader;
    if (!file_reader.open(argv[1])) {
        std::cerr << "Could not open file: " << argv[1] << std::endl;
        exit(1);
    }

    std::ofstream out_seqs;
    out_seqs.open(argv[2]);

    std::ofstream out_rle;
    out_rle.open(argv[3]);

    while (file_reader.read_line(line)) {
        if (line[0] == 'S') {

            // find the sequence start and end
            std::size_t seq_start = 0;
            std::size_t seq_end = 0;
            std::size_t tab_count = 0;
            for (std::size_t i = 0; i < line.size(); ++i) {
                if (line[i] == '\t') {
                    tab_count++;
                    if (tab_count == 2) {
                        seq_start = i + 1;
                    } else if (tab_count == 3) {
                        seq_end = i;
                        break;
                    }
                }
            }
            if (seq_end == 0) seq_end = line.size();
            std::string rle_seq = rle_encode(line, seq_start, seq_end);
            out_seqs << line.substr(seq_start, seq_end - seq_start) << "\n";
            out_rle << rle_seq << "\n";
            // std::cout << "Original sequence: " << line.substr(seq_start, seq_end - seq_start) << "\n";
            // std::cout << "Runlength encoded sequence: " << rle_seq << "\n";
        }
    }
}

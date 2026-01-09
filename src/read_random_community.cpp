//
// Created by Fawaz Dabbaghie on 09/01/2026.
//

#include <iostream>
#include <filesystem>
#include <fstream>
#include <functional>
#include <graph_binary.h>

#include "split_gfa_to_comms.h"


namespace fs = std::filesystem;

static void validate_span(const std::string& gz_path, uint64_t off, uint64_t sz) {
    uint64_t fsz = fs::file_size(gz_path);
    std::cerr << "file_size=" << fsz << " off=" << off << " sz=" << sz
              << " off+sz=" << (off + sz) << "\n";
    if (off > fsz) throw std::runtime_error("Offset beyond EOF");
    if (off + sz > fsz) throw std::runtime_error("Range exceeds EOF (bad index / overflow)");
}


int main(int argc, char *argv[]) {
    if (argc < 4) {
        std::cerr << "Usage:\n" << argv[0] << " <input.indexed.gz> <input.idx> <communit_id>\n";
        return 2;
    }
    const std::string index_path = argv[2];
    const std::string gz_path    = argv[1];
    const std::uint32_t community_id = static_cast<std::uint32_t>(std::stoul(argv[3]));

    // CommunitySpan s = lookup_community_span_tsv(index_path, community_id);
    // validate_span(gz_path, s.gz_offset, s.gz_size);
    // std::string text = read_gzip_member(gz_path, s.gz_offset, s.gz_size);
    stream_community_lines(index_path, gz_path, community_id,
    [](const std::string& line) -> bool {
    std::cout << line << "\n";  // each line has no trailing '\n'
    return true;                // keep going
    });
}
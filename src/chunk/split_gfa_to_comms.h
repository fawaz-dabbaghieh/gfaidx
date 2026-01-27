//
// Created by Fawaz Dabbaghie on 09/01/2026.
//

#ifndef GFAIDX_SPLIT_GFA_TO_COMMS_H
#define GFAIDX_SPLIT_GFA_TO_COMMS_H

#include <cstddef>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

#include <graph_binary.h>

#include "fs/Reader.h"

struct IndexEntry {
    std::uint32_t community_id{};
    std::uint64_t gz_offset{};
    std::uint64_t gz_size{};
};

// struct SplitStats {
//     std::vector<std::uint64_t> uncompressed_sizes;
//     std::vector<std::uint32_t> line_counts;
// };

// struct IdMaps {
//     std::vector<std::string> id_to_node;
//     std::vector<std::uint32_t> id_to_comm;
// };


void split_gzip_gfa(const std::string& in_gfa,
                    const std::string& out_gz,
                    const std::string& out_dir,
                    const std::uint32_t ncom,
                    std::size_t max_open_text,
                    const std::unordered_map<std::string, unsigned int>& node_id_map,
                    const std::vector<std::uint32_t>& id_to_comm,
                    const Reader::Options& reader_options = Reader::Options{},
                    int gzip_level = 6,
                    int gzip_mem_level = 8);

#endif //GFAIDX_SPLIT_GFA_TO_COMMS_H

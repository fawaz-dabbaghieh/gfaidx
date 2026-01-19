//
// Created by Fawaz Dabbaghie on 09/01/2026.
//

#ifndef GFAIDX_SPLIT_GFA_TO_COMMS_H
#define GFAIDX_SPLIT_GFA_TO_COMMS_H

#include <cstdint>
#include <functional>
#include <string>
#include <unordered_map>
#include <fstream>
#include <filesystem>
#include <vector>

#include <graph_binary.h>

#include "fs/Reader.h"


namespace fs = std::filesystem;


struct CommunitySpan {
    std::uint64_t gz_offset = 0;
    std::uint64_t gz_size   = 0;
};

struct IndexEntry {
    std::uint32_t community_id{};
    std::uint64_t gz_offset{};
    std::uint64_t gz_size{};
    std::uint64_t uncompressed_size{};
    std::uint32_t line_count{};
};

CommunitySpan lookup_community_span_tsv(const std::string& index_path,
                                        const std::uint32_t community_id);

void stream_community_lines_from_gz_range(
    const std::string& gz_path,
    std::uint64_t offset,
    std::uint64_t gz_size,
    const std::function<bool(const std::string&)>& on_line);

void stream_community_lines(
    const std::string& index_path,
    const std::string& gz_path,
    std::uint32_t community_id,
    const std::function<bool(const std::string&)>& on_line);


void split_gzip_gfa(const std::string& in_gfa,
                    const std::string& out_gz,
                    const std::string& out_dir,
                    const BGraph& g,
                    std::size_t max_open_text,
                    const std::unordered_map<std::string, unsigned int>& node_id_map,
                    const Reader::Options& reader_options = Reader::Options{},
                    int gzip_level = 6,
                    int gzip_mem_level = 8);

#endif //GFAIDX_SPLIT_GFA_TO_COMMS_H

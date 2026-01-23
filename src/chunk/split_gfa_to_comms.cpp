//
// Created by Fawaz Dabbaghie on 09/01/2026.
//

#include "chunk/split_gfa_to_comms.h"

#include <zlib.h>

#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <list>
#include <random>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>
#include <graph_binary.h>

#include "chunk/text_handle_cache.h"
#include "fs/gfa_line_parsers.h"
#include "utils/Timer.h"

namespace fs = std::filesystem;

static void throw_zlib(const char* where, int zret) {
    throw std::runtime_error(std::string(where) + " (zlib ret=" + std::to_string(zret) + ")");
}

// Stream-compress a whole text file into ONE gzip member appended to `out`.
static void append_one_gzip_member_from_file(std::ofstream& out,
                                            const fs::path& in_path,
                                            int level = 6,
                                            int memLevel = 8) {

    std::ifstream in(in_path, std::ios::binary);
    if (!in) throw std::runtime_error("Failed to open temp text for read: " + in_path.string());

    z_stream strm;
    std::memset(&strm, 0, sizeof(strm));

    // 15+16 => gzip wrapper; produces header+trailer => one gzip member.
    int ret = deflateInit2(&strm, level, Z_DEFLATED, 15 + 16, memLevel, Z_DEFAULT_STRATEGY);
    if (ret != Z_OK) throw_zlib("deflateInit2", ret);

    std::vector<unsigned char> inbuf(1 << 20);
    unsigned char outbuf[1u << 16];

    while (true) {
        in.read(reinterpret_cast<char*>(inbuf.data()), static_cast<std::streamsize>(inbuf.size()));
        std::streamsize got = in.gcount();
        if (got < 0) got = 0;

        strm.next_in = inbuf.data();
        strm.avail_in = static_cast<uInt>(got);

        int flush = in.eof() ? Z_FINISH : Z_NO_FLUSH;

        do {
            strm.next_out = outbuf;
            strm.avail_out = sizeof(outbuf);

            ret = deflate(&strm, flush);
            if (ret == Z_STREAM_ERROR) {
                deflateEnd(&strm);
                throw_zlib("deflate", ret);
            }

            std::size_t have = sizeof(outbuf) - strm.avail_out;
            if (have) out.write(reinterpret_cast<const char*>(outbuf), static_cast<std::streamsize>(have));

        } while (strm.avail_out == 0);

        if (flush == Z_FINISH) break;
    }

    if (ret != Z_STREAM_END) {
        deflateEnd(&strm);
        throw std::runtime_error("deflate did not reach Z_STREAM_END");
    }

    deflateEnd(&strm);
}


void debug_print_node_to_comm(const std::unordered_map<std::string, unsigned int>& node_to_id,
                             const std::vector<std::uint32_t>& id_to_comm) {
    for (const auto& [node_id, node_int_id] : node_to_id) {
        std::cout << node_id << " -> " << id_to_comm[node_int_id] << std::endl;
    }
}


static std::vector<fs::path> build_part_paths(const std::string& out_dir, std::uint32_t n_communities) {
    std::vector<fs::path> part_txt;
    part_txt.reserve(n_communities);
    for (std::uint32_t c = 0; c < n_communities; ++c) {
        part_txt.emplace_back(out_dir + "/comm_" + std::to_string(c) + ".gfa");
        if (fs::exists(part_txt.back())) fs::remove(part_txt.back());
    }
    return part_txt;
}


inline unsigned int get_node_comm(const std::string& node_id, const std::unordered_map<std::string,
    unsigned int>& node_to_id,
    const std::vector<std::uint32_t>& id_to_comm) {

    if (node_id.empty()) {
        std::cerr << "Empty node ID encountered" << std::endl;
        exit(1);
    }

    unsigned int node_int_id;
    try {
        node_int_id = node_to_id.at(node_id);
    } catch (const std::out_of_range& e) {
        std::cerr << "Node " << node_id << " not found in the map" << std::endl;
        std::cerr << e.what() << std::endl;
        exit(1);
    }
    return id_to_comm[node_int_id];

}
// for line processing, should split it to separate functions maybe
// a bit messy, but it's fine for now
static void split_gfa_to_parts(const std::string& in_gfa,
                               const std::unordered_map<std::string, unsigned int>& node_to_id,
                               const std::vector<std::uint32_t>& id_to_comm,
                               const std::vector<fs::path>& part_txt,
                               std::size_t max_open_text,
                               const Reader::Options& reader_options) {

    // because I'll be opening a lot of files and the system limits the numer of open file
    // I am using LRU cache to loop through the open files
    TextHandleCache cache(part_txt, max_open_text);

    std::string_view line;
    Reader file_reader(reader_options);
    if (!file_reader.open(in_gfa)) {
        std::cerr << "Could not open file: " << in_gfa << std::endl;
        exit(1);
    }

    std::cout << get_time() << ": Starting splitting the GFA into communities" << std::endl;
    std::uint32_t last_comm = part_txt.size() - 1;

    // debug_print_node_to_comm(node_to_id, id_to_comm);

    while (file_reader.read_line(line)) {

        if (line[0] == 'H') {
            cache.write_line(0, line);
            continue;
        }
        // here I need to check if the edge belongs to two chunks
        // separate into its own chunk with the other in-between edges
        if (line[0] == 'L') {
            auto [source, destination] = extract_L_nodes(line);
            auto src_comm_id = get_node_comm(source, node_to_id, id_to_comm);
            auto dest_comm_id = get_node_comm(destination, node_to_id, id_to_comm);
            if (src_comm_id == dest_comm_id) {
                cache.write_line(src_comm_id, line);
            } else {  // in-between edges
                cache.write_line(last_comm, line);
            }
        // using extract_S_node I think was wasteful, I only need the node ID
        // so I'll only copy the parts that find the node ID
        } else if (line[0] == 'S') {
            std::string node_id;
            // as for the GFA format, the node ID comes after S\tnode_id\t
            // so between the first and second tabs
            const size_t t1 = line.find('\t');
            if (t1 == npos) offending_line(line);
            const size_t t2 = line.find('\t', t1 + 1);
            if (t2 == npos) offending_line(line);
            node_id = std::string(line.substr(t1 + 1, t2 - (t1 + 1)));
            auto node_comm_id = get_node_comm(node_id, node_to_id, id_to_comm);
            // std::uint32_t c = id_to_comm[node_int_id];
            cache.write_line(node_comm_id, line);

        }
    }
    cache.close_all();
}

static void compress_parts_to_gzip(const std::string& out_gz,
                                   const std::vector<fs::path>& part_txt,
                                   int gzip_level,
                                   int gzip_mem_level) {
    std::ofstream out(out_gz, std::ios::binary);
    if (!out) throw std::runtime_error("Failed to open " + out_gz);
    std::string out_idx = out_gz + ".idx";

    std::ofstream idx(out_idx);
    if (!idx) throw std::runtime_error("Failed to open " + out_idx);
    idx << "#community_id\tgz_offset\tgz_size\n";

    std::cout << get_time() << ": Starting to compress and add to final file" << std::endl;
    for (std::uint32_t c = 0; c < part_txt.size(); ++c) {
        IndexEntry offsets_idx;
        offsets_idx.community_id = c;
        offsets_idx.gz_offset = static_cast<std::uint64_t>(out.tellp());

        if (fs::exists(part_txt[c]) && fs::file_size(part_txt[c]) > 0) {
            Timer member_timer;
            std::cout << get_time() << ": Compressing community " << c << std::endl;
            append_one_gzip_member_from_file(out, part_txt[c], gzip_level, gzip_mem_level);
            offsets_idx.gz_size = static_cast<std::uint64_t>(out.tellp()) - offsets_idx.gz_offset;
            std::cout << get_time() << ": Finished community " << c
                      << " in " << member_timer.elapsed() << " seconds" << std::endl;
        } else {
            offsets_idx.gz_size = 0;
        }

        idx << offsets_idx.community_id << '\t' << offsets_idx.gz_offset << '\t' << offsets_idx.gz_size << "\n";
    }
}

void split_gzip_gfa(const std::string& in_gfa,
                    const std::string& out_gz,
                    const std::string& out_dir,
                    const BGraph& g,
                    std::size_t max_open_text,
                    const std::unordered_map<std::string, unsigned int>& node_to_id,
                    const std::vector<std::uint32_t>& id_to_comm,
                    const Reader::Options& reader_options,
                    int gzip_level,
                    int gzip_mem_level) {

    const auto ncom = static_cast<std::uint32_t>(g.nodes.size());

    // generate a list of paths for the separate chunks
    const auto part_txt = build_part_paths(out_dir, ncom + 1);

    // splits the GFA file to separate communities on disk
    split_gfa_to_parts(in_gfa,
                       node_to_id,
                       id_to_comm,
                       part_txt,
                       max_open_text,
                       reader_options);

    // compresses each community to the final graph and builds the offsets index
    compress_parts_to_gzip(out_gz,
                           part_txt,
                           gzip_level,
                           gzip_mem_level);
}

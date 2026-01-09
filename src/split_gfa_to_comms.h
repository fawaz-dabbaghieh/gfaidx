//
// Created by Fawaz Dabbaghie on 09/01/2026.
//

#ifndef GFAIDX_SPLIT_GFA_TO_COMMS_H
#define GFAIDX_SPLIT_GFA_TO_COMMS_H
#include <zlib.h>

#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <list>
#include <random>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>
#include <graph_binary.h>

#include "fs/Reader.h"
#include "fs/gfa_line_parsers.h"


namespace fs = std::filesystem;

struct IndexEntry {
    std::uint32_t community_id{};
    std::uint64_t gz_offset{};
    std::uint64_t gz_size{};
    std::uint64_t uncompressed_size{};
    std::uint32_t line_count{};
};

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

// LRU cache for raw text temp files (safe to close/reopen: no compression state).
// we need this due to the limit of open file descriptors or open files a process can have
// this limit can be changed, but using this LRU cache to cycle through them, as I might be writing to many files
class TextHandleCache {
public:
    TextHandleCache(std::vector<fs::path> paths, std::size_t max_open)
        : paths_(std::move(paths)), max_open_(max_open) {}

    void write_line(std::uint32_t cid, std::string_view line_no_nl) {
        std::ofstream& f = get_handle_(cid);
        f.write(line_no_nl.data(), static_cast<std::streamsize>(line_no_nl.size()));
        f.put('\n');
        if (!f) throw std::runtime_error("write failed for " + paths_.at(cid).string());
    }

    void close_all() {
        for (auto& kv : open_) {
            kv.second.file.close();
        }
        open_.clear();
        lru_.clear();
    }

private:
    struct OpenRec {
        std::ofstream file;
        std::list<std::uint32_t>::iterator it;
    };

    std::ofstream& get_handle_(std::uint32_t cid) {
        auto it = open_.find(cid);
        if (it != open_.end()) {
            lru_.erase(it->second.it);
            lru_.push_front(cid);
            it->second.it = lru_.begin();
            return it->second.file;
        }

        // Evict if needed
        if (open_.size() >= max_open_) {
            std::cout << "Evicting file " << paths_.at(lru_.back()) << std::endl;
            std::uint32_t evict = lru_.back();
            lru_.pop_back();
            auto eit = open_.find(evict);
            if (eit != open_.end()) {
                eit->second.file.close();
                open_.erase(eit);
            }
        }

        const fs::path& p = paths_.at(cid);
        fs::create_directories(p.parent_path());

        // Append mode; create if missing.
        std::ofstream f(p, std::ios::binary | std::ios::out | std::ios::app);
        if (!f) throw std::runtime_error("Failed to open temp text file: " + p.string());

        lru_.push_front(cid);
        OpenRec rec{std::move(f), lru_.begin()};
        auto [ins_it, ok] = open_.emplace(cid, std::move(rec));
        return ins_it->second.file;
    }

    std::vector<fs::path> paths_;
    std::size_t max_open_;
    std::unordered_map<std::uint32_t, OpenRec> open_;
    std::list<std::uint32_t> lru_;
};


inline std::string process_lines(const std::string_view line) {
    if (line[0] == 'L') {
        auto [src, dest] = extract_L_nodes(line);
        return src;

    } if (line[0] == 'S') {
        std::string node_id;
        std::string node_seq;
        extract_S_node(line, node_id, node_seq);
        return node_id;
    }
    return "";
}


inline void split_gzip_gfa(const std::string& in_gfa,
                      const std::string& out_gz,
                      const std::string& out_dir,
                      const BGraph& g,
                      std::size_t max_open_text,
                      const std::unordered_map<std::string, unsigned int>& node_id_map) {

    // a vector with [node1, node2, node3...] and can be accessed with the int ID. So, int to string ID mapping
    std::vector<std::string> id_to_node(node_id_map.size());
    for (const auto& p : node_id_map) {
        id_to_node[p.second] = p.first;
    }

    // the node_id_map maps string id to int

    // now we need to map int ID to community ID
    std::vector<uint32_t> node_to_comm(node_id_map.size());
    for (int c=0 ; c < g.nodes.size() ; c++) {
        for (const auto n : g.nodes[c]) {
            node_to_comm[n] = c;
        }
    }

    int n_communities = g.nodes.size();

    // now when we loop through the file, if it's an S line, easy, get the node ID and map to community
    // then write in that community file
    // L lines a bit tricky
    // Temp directory next to output.
    // fs::path out_path(out_gz);
    const fs::path tmp_dir = out_dir;
    // tmp_dir += ".parts_text";
    // fs::create_directories(tmp_dir);

    // Per-community raw temp files.
    std::vector<fs::path> part_txt;
    part_txt.reserve(n_communities);
    for (std::uint32_t c = 0; c < n_communities; ++c) {
        part_txt.emplace_back(out_dir + "/comm_" + std::to_string(c) + ".gfa");
        // part_txt.push_back(tmp_dir / ("comm_" + std::to_string(c) + ".gfa"));
        // remove if exists from previous runs
        if (fs::exists(part_txt.back())) fs::remove(part_txt.back());
    }

    // working on the input file

    std::ifstream in(in_gfa);
    if (!in) throw std::runtime_error("Failed to open " + in_gfa);

    TextHandleCache cache(part_txt, max_open_text);


    std::string_view line;
    Reader file_reader;
    if (!file_reader.open(in_gfa)) {
        std::cerr << "Could not open file: " << in_gfa << std::endl;
        exit(1);
    }

    std::uint32_t ncom = g.nodes.size();
    std::vector<std::uint64_t> uncomp(ncom, 0);
    std::vector<std::uint32_t> lines(ncom, 0);

    while (file_reader.read_line(line)) {
        std::string node_id;
        node_id = process_lines(line);
        if (node_id.empty()) continue;
        // todo: add check if something went wrong and the node is not in the map
        unsigned int node_int_id = node_id_map.at(node_id);
        uint32_t c = node_to_comm[node_int_id];
        cache.write_line(c, line);
        uncomp[c] += (line.size() + 1);
        lines[c] += 1;
    }
    cache.close_all();


    // PASS 2: compress each comm_X.gfa into exactly ONE gzip member, append to final .gz
    std::ofstream out(out_gz, std::ios::binary);
    if (!out) throw std::runtime_error("Failed to open " + out_gz);
    std::string out_idx = out_gz + ".idx";

    std::ofstream idx(out_idx);
    if (!idx) throw std::runtime_error("Failed to open " + out_idx);
    idx << "#community_id\tgz_offset\tgz_size\tuncompressed_size\tline_count\n";

    // Choose compression level: 6 ~ gzip default, 9 best compression
    const int level = 9;
    const int memLevel = 9; // 8 is typical; 9 may slightly improve, uses more memory during compression

    std::vector<IndexEntry> entries;
    entries.reserve(ncom);

    for (std::uint32_t c = 0; c < ncom; ++c) {
        IndexEntry e;
        e.community_id = c;
        e.gz_offset = static_cast<std::uint64_t>(out.tellp());
        e.uncompressed_size = uncomp[c];
        e.line_count = lines[c];

        if (fs::exists(part_txt[c]) && fs::file_size(part_txt[c]) > 0) {
            append_one_gzip_member_from_file(out, part_txt[c], level, memLevel);
            e.gz_size = static_cast<std::uint64_t>(out.tellp()) - e.gz_offset;
        } else {
            e.gz_size = 0;
        }

        idx << e.community_id << '\t' << e.gz_offset << '\t' << e.gz_size << '\t'
            << e.uncompressed_size << '\t' << e.line_count << "\n";
        entries.push_back(e);
    }

}

#endif //GFAIDX_SPLIT_GFA_TO_COMMS_H
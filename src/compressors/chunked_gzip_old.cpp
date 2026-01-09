//
// Created by Fawaz Dabbaghie on 31/12/2025.
//

#include <zlib.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>
// #include "../fs/Reader.h"


struct ChunkIndexEntry {
    std::uint32_t community_id{};
    std::uint64_t gz_offset{};
    std::uint64_t gz_size{};
    std::uint64_t uncompressed_size{};
    std::uint32_t line_count{};
};

static void throw_zlib(const char* where, int zret) {
    throw std::runtime_error(std::string(where) + " zlib ret=" + std::to_string(zret));
}

// Write ONE gzip "member" (independent stream) to `out`.
// This makes output.gfa.gz a concatenation of gzip members.
static void write_gzip_member(std::ofstream& out, const std::string& payload, int level = Z_BEST_COMPRESSION) {
    z_stream strm;
    std::memset(&strm, 0, sizeof(strm));

    // windowBits = 15 + 16 => gzip wrapper
    int ret = deflateInit2(&strm, level, Z_DEFLATED, 15 + 16, 8, Z_DEFAULT_STRATEGY);
    if (ret != Z_OK) throw_zlib("deflateInit2", ret);

    strm.next_in = reinterpret_cast<Bytef*>(const_cast<char*>(payload.data()));
    strm.avail_in = static_cast<uInt>(payload.size());

    unsigned char outbuf[1u << 16];

    // Z_FINISH because payload already complete in memory.
    do {
        strm.next_out = outbuf;
        strm.avail_out = sizeof(outbuf);

        ret = deflate(&strm, Z_FINISH);
        if (ret == Z_STREAM_ERROR) {
            deflateEnd(&strm);
            throw_zlib("deflate", ret);
        }

        std::size_t have = sizeof(outbuf) - strm.avail_out;
        if (have) out.write(reinterpret_cast<const char*>(outbuf), static_cast<std::streamsize>(have));

    } while (ret != Z_STREAM_END);

    deflateEnd(&strm);
}

// Read exactly one gzip member from (offset, gz_size) and return decompressed text.
static std::string read_gzip_member(const std::string& gz_path, std::uint64_t offset, std::uint64_t gz_size) {
    std::ifstream in(gz_path, std::ios::binary);
    if (!in) throw std::runtime_error("Failed to open " + gz_path);

    in.seekg(static_cast<std::streamoff>(offset), std::ios::beg);
    if (!in) throw std::runtime_error("seekg failed");

    z_stream strm;
    std::memset(&strm, 0, sizeof(strm));

    // windowBits = 15 + 16 => gzip wrapper
    int ret = inflateInit2(&strm, 15 + 16);
    if (ret != Z_OK) throw_zlib("inflateInit2", ret);

    unsigned char inbuf[1u << 16];
    unsigned char outbuf[1u << 16];

    std::string out;
    out.reserve(std::min<std::uint64_t>(gz_size * 3, 1ull << 20)); // rough guess

    std::uint64_t remaining = gz_size;

    while (true) {
        if (remaining == 0) {
            inflateEnd(&strm);
            throw std::runtime_error("Ran out of compressed bytes before reaching Z_STREAM_END (bad index?)");
        }

        std::size_t to_read = std::min<std::uint64_t>(remaining, sizeof(inbuf));
        in.read(reinterpret_cast<char*>(inbuf), static_cast<std::streamsize>(to_read));
        const auto got = static_cast<std::size_t>(in.gcount());
        if (got == 0) {
            inflateEnd(&strm);
            throw std::runtime_error("Unexpected EOF while reading gzip member");
        }
        remaining -= got;

        strm.next_in = inbuf;
        strm.avail_in = static_cast<uInt>(got);

        while (strm.avail_in > 0) {
            strm.next_out = outbuf;
            strm.avail_out = sizeof(outbuf);

            ret = inflate(&strm, Z_NO_FLUSH);
            if (ret == Z_STREAM_END) {
                std::size_t have = sizeof(outbuf) - strm.avail_out;
                if (have) out.append(reinterpret_cast<const char*>(outbuf), have);
                inflateEnd(&strm);
                return out;
            }
            if (ret != Z_OK) {
                inflateEnd(&strm);
                throw_zlib("inflate", ret);
            }

            std::size_t have = sizeof(outbuf) - strm.avail_out;
            if (have) out.append(reinterpret_cast<const char*>(outbuf), have);
        }
    }
}

static std::vector<ChunkIndexEntry> build_chunked_gz(
        const std::string& in_txt,
        const std::string& out_gz,
        const std::string& out_index_tsv,
        std::size_t lines_per_chunk) {

    std::ifstream in(in_txt);
    if (!in) throw std::runtime_error("Failed to open " + in_txt);

    std::ofstream out(out_gz, std::ios::binary);
    if (!out) throw std::runtime_error("Failed to open " + out_gz);

    std::ofstream idx(out_index_tsv);
    if (!idx) throw std::runtime_error("Failed to open " + out_index_tsv);

    idx << "#community_id\tgz_offset\tgz_size\tuncompressed_size\tline_count\n";

    std::vector<ChunkIndexEntry> entries;
    entries.reserve(1024);

    std::uint32_t community_id = 0;
    std::uint32_t line_count = 0;
    std::string chunk;
    chunk.reserve(1 << 20);

    auto flush_chunk = [&]() {
        if (line_count == 0) return;

        const std::uint64_t start = out.tellp();  // position in output
        const auto uncomp = static_cast<std::uint64_t>(chunk.size());

        write_gzip_member(out, chunk);

        const std::uint64_t end = out.tellp();
        const std::uint64_t gzs = end - start;

        ChunkIndexEntry e;
        e.community_id = community_id;
        e.gz_offset = start;
        e.gz_size = gzs;
        e.uncompressed_size = uncomp;
        e.line_count = line_count;

        entries.push_back(e);

        idx << e.community_id << '\t' << e.gz_offset << '\t' << e.gz_size << '\t'
                << e.uncompressed_size << '\t' << e.line_count << "\n";

        // reset
        community_id++;
        line_count = 0;
        chunk.clear();
    };

    std::string line;
    // this part can be replaced with my reader
    while (std::getline(in, line)) {
        chunk.append(line);
        chunk.push_back('\n');
        line_count++;

        if (line_count == lines_per_chunk) {
            flush_chunk();
        }
    }
    flush_chunk();

    return entries;
}

static ChunkIndexEntry find_entry(const std::vector<ChunkIndexEntry>& entries, std::uint32_t community_id) {
    auto it = std::find_if(entries.begin(), entries.end(),
                                                 [&](const ChunkIndexEntry& e) { return e.community_id == community_id; });
    if (it == entries.end()) throw std::runtime_error("community_id not found in index");
    return *it;
}

int main(int argc, char** argv) {
    try {
        if (argc < 4) {
            std::cerr << "Usage:\n"
                                << "    " << argv[0] << " <input.gfa> <output.gfa.gz> <index.tsv> [community_to_test]\n";
            return 2;
        }

        const std::string in_gfa = argv[1];
        const std::string out_gz = argv[2];
        const std::string out_idx = argv[3];
        const std::uint32_t test_comm = (argc >= 5) ? static_cast<std::uint32_t>(std::stoul(argv[4])) : 0;

        constexpr std::size_t lines_per_community = 395;

        const auto entries = build_chunked_gz(in_gfa, out_gz, out_idx, lines_per_community);

        std::cerr << "Wrote " << entries.size() << " gzip members to " << out_gz << "\n";
        std::cerr << "Index: " << out_idx << "\n";

        // Test: decompress one community chunk.
        auto e = find_entry(entries, test_comm);
        std::string text = read_gzip_member(out_gz, e.gz_offset, e.gz_size);

        std::cout << "=== community " << test_comm << " (expected lines=" << e.line_count << ") ===\n";
        // Print first ~10 lines
        std::size_t printed = 0;
        for (std::size_t i = 0; i < text.size() && printed < 10; ) {
            std::size_t j = text.find('\n', i);
            if (j == std::string::npos) j = text.size();
            std::cout << text.substr(i, j - i) << "\n";
            printed++;
            i = (j < text.size()) ? (j + 1) : j;
        }

        return 0;
    } catch (const std::exception& e) {
        std::cerr << "ERROR: " << e.what() << "\n";
        return 1;
    }
}

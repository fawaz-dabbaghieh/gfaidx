#include "chunk/chunk_reader.h"

#include <zlib.h>

#include <cstring>
#include <fstream>
#include <stdexcept>
#include <string_view>
#include <vector>

static void throw_zlib(const char* where, int zret) {
    throw std::runtime_error(std::string(where) + " (zlib ret=" + std::to_string(zret) + ")");
}

CommunitySpan lookup_community_span_tsv(const std::string& index_path,
                                        std::uint32_t community_id) {
    std::ifstream idx(index_path);
    if (!idx) throw std::runtime_error("Failed to open index file: " + index_path);

    std::string line;
    while (std::getline(idx, line)) {
        if (line.empty() || line[0] == '#') continue;

        std::size_t p0 = line.find('\t');
        if (p0 == std::string::npos) continue;
        std::size_t p1 = line.find('\t', p0 + 1);
        if (p1 == std::string::npos) continue;
        std::size_t p2 = line.find('\t', p1 + 1);

        auto col0 = std::string_view(line).substr(0, p0);
        auto col1 = std::string_view(line).substr(p0 + 1, p1 - (p0 + 1));
        auto col2 = (p2 == std::string::npos)
                      ? std::string_view(line).substr(p1 + 1)
                      : std::string_view(line).substr(p1 + 1, p2 - (p1 + 1));

        std::uint32_t cid = static_cast<std::uint32_t>(std::stoul(std::string(col0)));
        if (cid != community_id) continue;

        CommunitySpan s;
        s.gz_offset = static_cast<std::uint64_t>(std::stoull(std::string(col1)));
        s.gz_size   = static_cast<std::uint64_t>(std::stoull(std::string(col2)));
        return s;
    }

    throw std::runtime_error("Community id not found in index: " + std::to_string(community_id));
}

void stream_community_lines_from_gz_range(
    const std::string& gz_path,
    std::uint64_t offset,
    std::uint64_t gz_size,
    const std::function<bool(const std::string&)>& on_line) {

    std::ifstream in(gz_path, std::ios::binary);
    if (!in) throw std::runtime_error("Failed to open " + gz_path);

    in.seekg(static_cast<std::streamoff>(offset), std::ios::beg);
    if (!in) throw std::runtime_error("seekg failed");

    z_stream strm;
    std::memset(&strm, 0, sizeof(strm));

    int ret = inflateInit2(&strm, 15 + 16);
    if (ret != Z_OK) throw_zlib("inflateInit2", ret);

    std::vector<unsigned char> inbuf(1 << 16);
    std::vector<unsigned char> outbuf(1 << 16);
    std::string pending;
    pending.reserve(1 << 20);

    std::uint64_t remaining = gz_size;
    bool stop = false;

    auto inflate_init = [](z_stream& s) {
        std::memset(&s, 0, sizeof(s));
        int ret = inflateInit2(&s, 15 + 16);
        if (ret != Z_OK) throw_zlib("inflateInit2", ret);
    };

    while (!stop) {
        if (remaining == 0) break;

        std::size_t want = static_cast<std::size_t>(std::min<std::uint64_t>(remaining, inbuf.size()));
        in.read(reinterpret_cast<char*>(inbuf.data()), static_cast<std::streamsize>(want));
        std::streamsize got = in.gcount();
        if (got <= 0) break;
        remaining -= static_cast<std::uint64_t>(got);

        strm.next_in = inbuf.data();
        strm.avail_in = static_cast<uInt>(got);

        while (strm.avail_in > 0) {
            strm.next_out = outbuf.data();
            strm.avail_out = static_cast<uInt>(outbuf.size());

            ret = inflate(&strm, Z_NO_FLUSH);

            std::size_t have = outbuf.size() - strm.avail_out;
            if (have) {
                pending.append(reinterpret_cast<char*>(outbuf.data()), have);
                std::size_t pos = 0;
                while (true) {
                    std::size_t nl = pending.find('\n', pos);
                    if (nl == std::string::npos) break;
                    std::string line = pending.substr(pos, nl - pos);
                    if (!on_line(line)) {
                        stop = true;
                        break;
                    }
                    pos = nl + 1;
                }
                if (pos > 0) pending.erase(0, pos);
            }

            if (ret == Z_STREAM_END) {
                Bytef* leftover_ptr = strm.next_in;
                uInt leftover_len = strm.avail_in;

                inflateEnd(&strm);

                if (stop) break;

                if (remaining == 0 && leftover_len == 0) break;

                inflate_init(strm);
                strm.next_in = leftover_ptr;
                strm.avail_in = leftover_len;
                continue;
            }

            if (ret != Z_OK) {
                inflateEnd(&strm);
                throw std::runtime_error("inflate failed ret=" + std::to_string(ret));
            }
        }
    }

    inflateEnd(&strm);
}

void stream_community_lines(
    const std::string& index_path,
    const std::string& gz_path,
    std::uint32_t community_id,
    const std::function<bool(const std::string&)>& on_line) {

    CommunitySpan s = lookup_community_span_tsv(index_path, community_id);
    stream_community_lines_from_gz_range(gz_path, s.gz_offset, s.gz_size, on_line);
}

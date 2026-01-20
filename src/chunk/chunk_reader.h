#ifndef GFAIDX_CHUNK_READER_H
#define GFAIDX_CHUNK_READER_H

#include <cstdint>
#include <functional>
#include <string>

struct CommunitySpan {
    std::uint64_t gz_offset = 0;
    std::uint64_t gz_size   = 0;
};

CommunitySpan lookup_community_span_tsv(const std::string& index_path,
                                        std::uint32_t community_id);

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

#endif //GFAIDX_CHUNK_READER_H

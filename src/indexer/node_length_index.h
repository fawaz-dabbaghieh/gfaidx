#ifndef GFAIDX_NODE_LENGTH_INDEX_H
#define GFAIDX_NODE_LENGTH_INDEX_H

#include <cstddef>
#include <cstdint>
#include <string>

#include "fs/Reader.h"

namespace gfaidx::indexer {

// Build a rank-aligned node-length sidecar. Entry i stores the uint32 segment
// length for the same node rank i used by .ndx, .pdx, and .cdx.
void build_node_length_index(const std::string& input_gfa,
                             const std::string& node_index_path,
                             const std::string& output_path,
                             const Reader::Options& reader_options = Reader::Options{});

// Mmap-backed reader for the .lnx sidecar. Keeping the length table mapped
// avoids rebuilding a large heap vector during coordinate-bearing subwalk output.
class NodeLengthIndexReader {
public:
    explicit NodeLengthIndexReader(const std::string& path);
    ~NodeLengthIndexReader();

    NodeLengthIndexReader(const NodeLengthIndexReader&) = delete;
    NodeLengthIndexReader& operator=(const NodeLengthIndexReader&) = delete;

    [[nodiscard]] std::uint64_t node_count() const { return node_count_; }
    [[nodiscard]] std::uint32_t length(std::uint32_t rank) const;

private:
    void close_mapping();

    int fd_{-1};
    void* mapping_{nullptr};
    std::size_t file_size_{0};
    std::uint64_t node_count_{0};
    const std::uint32_t* lengths_{nullptr};
};

}  // namespace gfaidx::indexer

#endif  // GFAIDX_NODE_LENGTH_INDEX_H

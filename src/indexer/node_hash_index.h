#ifndef GFAIDX_NODE_HASH_INDEX_H
#define GFAIDX_NODE_HASH_INDEX_H

#include <cstdint>
#include <fstream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace gfaidx::indexer {

// On-disk entry format for the node hash index (.ndx).
struct NodeHashEntry {
    std::uint64_t hash;
    std::uint32_t community_id;
};

// FNV-1a hash used for stable node-id hashing.
std::uint64_t fnv1a_hash(std::string_view s);

// Build and write the binary node hash index from node->id and id->community maps.
void write_node_hash_index(const std::unordered_map<std::string, unsigned int>& node_to_id,
                           const std::vector<std::uint32_t>& id_to_comm,
                           const std::string& out_path);

// Streaming on-disk lookup for node->community via binary search.
class NodeHashIndex {
public:
    explicit NodeHashIndex(const std::string& path);
    ~NodeHashIndex();

    NodeHashIndex(const NodeHashIndex&) = delete;
    NodeHashIndex& operator=(const NodeHashIndex&) = delete;

    bool lookup(std::string_view node_id, std::uint32_t& out_com) const;

private:
// it mainly uses mmap but has a fallback to fstream
#if defined(__unix__) || defined(__APPLE__)
    int fd_ = -1;
    const NodeHashEntry* data_ = nullptr;
    std::size_t file_size_ = 0;
    std::size_t n_entries_ = 0;
#else
    mutable std::ifstream file_;
    std::size_t entry_size_ = sizeof(NodeHashEntry);
    std::size_t file_size_ = 0;
    std::size_t n_entries_ = 0;
#endif
};

}  // namespace gfaidx::indexer

#endif  // GFAIDX_NODE_HASH_INDEX_H

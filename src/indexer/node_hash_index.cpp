#include "indexer/node_hash_index.h"

#include <algorithm>
#include <fstream>
#include <stdexcept>

namespace gfaidx::indexer {

std::uint64_t fnv1a_hash(std::string_view s) {
    // 64 bit string hashing using FNV-1a
    constexpr std::uint64_t FNV_OFFSET = 1469598103934665603ULL;
    constexpr std::uint64_t FNV_PRIME  = 1099511628211ULL;
    std::uint64_t h = FNV_OFFSET;
    for (unsigned char c : s) {
        h ^= c;
        h *= FNV_PRIME;
    }
    return h;
}

void write_node_hash_index(const std::unordered_map<std::string, unsigned int>& node_id_map,
                           const std::vector<std::uint32_t>& id_to_comm,
                           const std::string& out_path) {

    std::vector<NodeHashEntry> entries;
    entries.reserve(node_id_map.size());

    // Convert each node id into a hash and pair it with its community id.
    for (const auto& p : node_id_map) {
        const unsigned int int_id = p.second;
        if (int_id >= id_to_comm.size()) {
            throw std::runtime_error("Node id out of range while building .ndx");
        }
        NodeHashEntry e{};
        e.hash = fnv1a_hash(p.first);
        e.community_id = id_to_comm[int_id];
        entries.push_back(e);
    }

    // Sort by hash for binary-search lookup on disk.
    std::sort(entries.begin(), entries.end(),
              [](const NodeHashEntry& a, const NodeHashEntry& b) {
                  return a.hash < b.hash;
              });

    std::ofstream out(out_path, std::ios::binary);
    if (!out) {
        throw std::runtime_error("Failed to open output file: " + out_path);
    }

    // Write the raw binary table for fast lookup.
    out.write(reinterpret_cast<const char*>(entries.data()),
              static_cast<std::streamsize>(entries.size() * sizeof(NodeHashEntry)));
    out.close();
}

NodeHashIndex::NodeHashIndex(const std::string& path) {

    file_.open(path, std::ios::binary);
    if (!file_) {
        throw std::runtime_error("Failed to open node index file: " + path);
    }

    // Determine table size to drive binary search bounds.
    file_.seekg(0, std::ios::end);
    file_size_ = static_cast<std::size_t>(file_.tellg());
    file_.seekg(0, std::ios::beg);

    if (file_size_ % entry_size_ != 0) {
        throw std::runtime_error("Node index file size is invalid: " + path);
    }

    n_entries_ = file_size_ / entry_size_;
}

NodeHashIndex::~NodeHashIndex() {
    if (file_.is_open()) {
        file_.close();
    }
}

bool NodeHashIndex::lookup(std::string_view node_id, std::uint32_t& out_com) const {
    const std::uint64_t query_hash = fnv1a_hash(node_id);

    // Binary search the sorted hash table on disk.
    std::size_t low_val = 0;
    std::size_t high_val = n_entries_;
    while (low_val < high_val) {
        const std::size_t mid_val = low_val + (high_val - low_val) / 2;
        file_.seekg(static_cast<std::streamoff>(mid_val * entry_size_), std::ios::beg);
        NodeHashEntry entry{};
        file_.read(reinterpret_cast<char*>(&entry), static_cast<std::streamsize>(entry_size_));

        if (!file_) {
            throw std::runtime_error("Failed to read from node index file");
        }

        const std::uint64_t mid_val_hash = entry.hash;

        if (mid_val_hash < query_hash) low_val = mid_val + 1;
        else if (mid_val_hash > query_hash) high_val = mid_val;
        else {
            out_com = entry.community_id;
            return true;
        }
    }

    return false;
}

}  // namespace gfaidx::indexer

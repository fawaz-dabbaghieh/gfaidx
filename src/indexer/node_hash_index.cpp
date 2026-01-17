#include "indexer/node_hash_index.h"

#include <algorithm>
#include <fstream>
#include <stdexcept>

#if defined(__unix__) || defined(__APPLE__)
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

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

void write_node_hash_index(const std::unordered_map<std::string, unsigned int>& node_to_id,
                           const std::vector<std::uint32_t>& id_to_comm,
                           const std::string& out_path) {

    std::vector<NodeHashEntry> entries;
    entries.reserve(node_to_id.size());

    // Convert each node id into a hash and pair it with its community id.
    for (const auto& p : node_to_id) {
        const unsigned int int_id = p.second;
        if (int_id >= id_to_comm.size()) {
            throw std::runtime_error("Node id out of range while building .ndx");
        }
        // todo I have to think about hash collisions at some point
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

    // Writing the output node hash index file
    std::ofstream out(out_path, std::ios::binary);
    if (!out) {
        throw std::runtime_error("Failed to open output file: " + out_path);
    }
    out.write(reinterpret_cast<const char*>(entries.data()),
              static_cast<std::streamsize>(entries.size() * sizeof(NodeHashEntry)));
    out.close();
}

#if defined(__unix__) || defined(__APPLE__)
NodeHashIndex::NodeHashIndex(const std::string& path) {
    fd_ = ::open(path.c_str(), O_RDONLY);
    if (fd_ == -1) {
        throw std::runtime_error("Failed to open node index file: " + path);
    }

    struct stat st{};
    if (fstat(fd_, &st) == -1) {
        ::close(fd_);
        fd_ = -1;
        throw std::runtime_error("Failed to get the stat of the node index file: " + path);
    }

    file_size_ = static_cast<std::size_t>(st.st_size);
    if (file_size_ % sizeof(NodeHashEntry) != 0) {
        ::close(fd_);
        fd_ = -1;
        throw std::runtime_error("Node index file size is invalid: " + path);
    }

    n_entries_ = file_size_ / sizeof(NodeHashEntry);

    data_ = static_cast<const NodeHashEntry*>(mmap(nullptr,
                                                   file_size_,
                                                   PROT_READ,
                                                   MAP_SHARED,
                                                   fd_,
                                                   0));

    if (data_ == MAP_FAILED) {
        data_ = nullptr;
        ::close(fd_);
        fd_ = -1;
        throw std::runtime_error("mmap failed for node index file: " + path);
    }
}

NodeHashIndex::~NodeHashIndex() {
    if (data_) {
        munmap(const_cast<NodeHashEntry*>(data_), file_size_);
        data_ = nullptr;
    }
    if (fd_ != -1) {
        ::close(fd_);
        fd_ = -1;
    }
}
#else
    // fall back to fstream on other systems, maybe not necessary, but already wrote it
NodeHashIndex::NodeHashIndex(const std::string& path) {
    file_.open(path, std::ios::binary);
    if (!file_) {
        throw std::runtime_error("Failed to open node index file: " + path);
    }

    // Getting the size of the table for the binary search
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
#endif

bool NodeHashIndex::lookup(std::string_view node_id, std::uint32_t& out_com) const {
    // binary search lookup through the on-disk hash table
    const std::uint64_t query_hash = fnv1a_hash(node_id);

#if defined(__unix__) || defined(__APPLE__)
    std::size_t low_val = 0;
    std::size_t high_val = n_entries_;
    while (low_val < high_val) {
        // COLLISIONS? Need to think about dealing with them at some point
        const std::size_t mid_val = low_val + (high_val - low_val) / 2;
        const std::uint64_t mid_val_hash = data_[mid_val].hash;

        if (mid_val_hash < query_hash) low_val = mid_val + 1;
        else if (mid_val_hash > query_hash) high_val = mid_val;
        else {
            out_com = data_[mid_val].community_id;
            return true;
        }
    }
    return false;
#else
    std::size_t low_val = 0;
    std::size_t high_val = n_entries_;
    while (low_val < high_val) {
        const std::size_t mid_val = low_val + (high_val - low_val) / 2;
        const auto offset = static_cast<std::streamoff>(mid_val * entry_size_);
        file_.seekg(offset, std::ios::beg);
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
#endif
}

}  // namespace gfaidx::indexer

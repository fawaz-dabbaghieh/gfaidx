#include "indexer/node_hash_index.h"

#include <algorithm>
#include <fstream>
#include <limits>
#include <sstream>
#include <stdexcept>

static_assert(sizeof(gfaidx::indexer::NodeHashEntry) == 16, "NodeHashEntry size must be 16 bytes");

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "utils/debug_trace.h"
#include "fs/fs_helpers.h"

namespace gfaidx::indexer {

std::uint64_t fnv1a_hash64(std::string_view s) {
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

std::uint32_t fnv1a_hash32(std::string_view s) {
    // 32 bit string hashing using FNV-1a
    constexpr std::uint32_t FNV_OFFSET = 2166136261U;
    constexpr std::uint32_t FNV_PRIME  = 16777619U;
    std::uint32_t h = FNV_OFFSET;
    for (unsigned char c : s) {
        h ^= c;
        h *= FNV_PRIME;
    }
    return h;
}

std::uint64_t NodeHashIndex::size() const {
    return static_cast<std::uint64_t>(n_entries_);
}

void write_node_hash_index(const std::unordered_map<std::string, unsigned int>& node_to_id,
                           const std::vector<std::uint32_t>& id_to_comm,
                           const std::string& out_path) {
    // Stage the .ndx beside its final destination so we never expose a half-written index.
    const std::string temp_out_path = make_temp_output_path(out_path);

    // maybe this is taking too much memory here, as I am generating the hash for each node in this list before
    // writing to disk
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
        e.hash = fnv1a_hash64(p.first);
        // basically got the second hash for free due to padding, and used to avoid collisions
        e.hash32 = fnv1a_hash32(p.first);
        e.community_id = id_to_comm[int_id];
        entries.push_back(e);
    }

    // Sort by hash for binary-search lookup on disk.
    std::sort(entries.begin(), entries.end(),
              [](const NodeHashEntry& a, const NodeHashEntry& b) {
                  if (a.hash != b.hash) return a.hash < b.hash;
                  return a.hash32 < b.hash32;
              });

    try {
        // Write the complete hash table to the staged file first.
        std::ofstream out(temp_out_path, std::ios::binary);
        if (!out) {
            throw std::runtime_error("Failed to open output file: " + temp_out_path);
        }
        out.write(reinterpret_cast<const char*>(entries.data()),
                  static_cast<std::streamsize>(entries.size() * sizeof(NodeHashEntry)));
        // Close explicitly so delayed I/O errors surface before the publish rename.
        out.close();
        if (!out) {
            throw std::runtime_error("Failed while writing output file: " + temp_out_path);
        }
        // Only replace the final path after the staged write has fully succeeded.
        rename_path_or_throw(temp_out_path, out_path);
    } catch (...) {
        // If anything failed, remove the staged file so callers do not see leftover partial output.
        remove_path_if_exists(temp_out_path);
        throw;
    }
}

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

bool NodeHashIndex::lookup_rank(std::string_view node_id, std::uint32_t& out_rank) const {
    // Binary search lookup through the on-disk hash table. The returned rank is
    // the sorted entry position inside the .ndx file; path indexing can reuse
    // that stable rank as a compact node id without building another on-disk
    // name->id side index.
    const std::uint64_t query_hash = fnv1a_hash64(node_id);
    const std::uint32_t query_hash32 = fnv1a_hash32(node_id);

    std::size_t low_val = 0;
    std::size_t high_val = n_entries_;
    while (low_val < high_val) {
        const std::size_t mid_val = low_val + (high_val - low_val) / 2;
        const std::uint64_t mid_val_hash = data_[mid_val].hash;

        if (mid_val_hash < query_hash) low_val = mid_val + 1;
        else if (mid_val_hash > query_hash) high_val = mid_val;
        else {
            // Walk left to the first matching hash, then scan right to resolve collisions.
            std::size_t left = mid_val;
            while (left > 0 && data_[left - 1].hash == query_hash) {
                left--;
            }
            for (std::size_t i = left; i < n_entries_ && data_[i].hash == query_hash; ++i) {
                if (data_[i].hash32 == query_hash32) {
                    if (i > static_cast<std::size_t>(std::numeric_limits<std::uint32_t>::max())) {
                        throw std::runtime_error("Node hash index rank does not fit into uint32_t");
                    }
                    out_rank = static_cast<std::uint32_t>(i);
                    return true;
                }
            }
            // Log failed collision resolution so the temporary get_subgraph
            // trace can distinguish a true miss from a later rank corruption.
            if (gfaidx::debug::subgraph_trace_enabled()) {
                std::ostringstream oss;
                oss << "lookup_rank miss after hash64 match for node '" << node_id
                    << "' hash64=" << query_hash
                    << " hash32=" << query_hash32;
                gfaidx::debug::log_subgraph_trace(oss.str());
            }
            return false;
        }
    }
    // Log the full miss path once when the query hash never appears in .ndx.
    if (gfaidx::debug::subgraph_trace_enabled()) {
        std::ostringstream oss;
        oss << "lookup_rank miss for node '" << node_id
            << "' hash64=" << query_hash
            << " hash32=" << query_hash32
            << " n_entries=" << n_entries_;
        gfaidx::debug::log_subgraph_trace(oss.str());
    }
    return false;
}

bool NodeHashIndex::lookup(std::string_view node_id, std::uint32_t& out_com) const {
    std::uint32_t rank = 0;
    if (!lookup_rank(node_id, rank)) {
        return false;
    }

    out_com = data_[rank].community_id;
    return true;
}

std::uint32_t NodeHashIndex::community_id_by_rank(std::uint32_t rank) const {
    if (rank >= n_entries_) {
        // Emit the exact bad rank and table size before throwing so external
        // repro logs keep the low-level values that triggered the abort.
        if (gfaidx::debug::subgraph_trace_enabled()) {
            std::ostringstream oss;
            oss << "community_id_by_rank out of range rank=" << rank
                << " n_entries=" << n_entries_;
            gfaidx::debug::log_subgraph_trace(oss.str());
        }
        throw std::runtime_error("Node hash index rank out of range");
    }
    return data_[rank].community_id;
}

}  // namespace gfaidx::indexer

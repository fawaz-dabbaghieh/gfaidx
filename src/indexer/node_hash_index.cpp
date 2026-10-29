#include "indexer/node_hash_index.h"

#include <algorithm>
#include <cstring>
#include <fstream>
#include <limits>
#include <numeric>
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
namespace {

// .ndx v2 layout:
// - this 64-byte header
// - entry_count NodeHashEntry records sorted by (hash, hash32), payload = node id
// - community_count + 1 uint32 community start ids
// - zero padding so the file size is 8 mod 16
// Legacy files are a bare entry array whose size is a multiple of 16, so the
// two formats are told apart by size alone and old readers reject v2 files.
constexpr char kNodeIndexMagic[8] = {'G', 'F', 'A', 'N', 'D', 'X', '0', '2'};
constexpr std::uint32_t kNodeIndexVersion = 2;
constexpr std::size_t kNodeIndexV2SizeResidue = 8;

struct NodeHashIndexHeaderDisk {
    char magic[8]{};
    std::uint32_t version{};
    std::uint32_t node_order_key{};
    std::uint64_t entry_count{};
    std::uint64_t community_count{};
    std::uint64_t entries_offset{};
    std::uint64_t community_starts_offset{};
    std::uint64_t node_id_space{};
    std::uint64_t reserved{};
};

static_assert(sizeof(NodeHashIndexHeaderDisk) == 64, "Unexpected .ndx v2 header size");

constexpr std::uint64_t kFnvOffset = 1469598103934665603ULL;
constexpr std::uint64_t kFnvPrime = 1099511628211ULL;

void hash_u32(std::uint64_t& hash, std::uint32_t value) {
    for (int i = 0; i < 4; ++i) {
        hash ^= (value >> (8 * i)) & 0xffu;
        hash *= kFnvPrime;
    }
}

bool parse_u64_digits(std::string_view digits, std::uint64_t& out) {
    if (digits.empty()) return false;
    std::uint64_t value = 0;
    for (const char c : digits) {
        if (c < '0' || c > '9') return false;
        const auto digit = static_cast<std::uint64_t>(c - '0');
        if (value > (std::numeric_limits<std::uint64_t>::max() - digit) / 10) return false;
        value = value * 10 + digit;
    }
    out = value;
    return true;
}

std::vector<NodeHashEntry> make_sorted_entries(
    const std::unordered_map<std::string, unsigned int>& node_to_id,
    const std::vector<std::uint32_t>& internal_to_payload) {
    std::vector<NodeHashEntry> entries;
    entries.reserve(node_to_id.size());

    // Convert each node id into a hash and pair it with its payload.
    for (const auto& p : node_to_id) {
        const unsigned int int_id = p.second;
        if (int_id >= internal_to_payload.size()) {
            throw std::runtime_error("Node id out of range while building .ndx");
        }
        // todo I have to think about hash collisions at some point
        NodeHashEntry e{};
        e.hash = fnv1a_hash64(p.first);
        // basically got the second hash for free due to padding, and used to avoid collisions
        e.hash32 = fnv1a_hash32(p.first);
        e.payload = internal_to_payload[int_id];
        entries.push_back(e);
    }

    // Sort by hash for binary-search lookup on disk.
    std::sort(entries.begin(), entries.end(),
              [](const NodeHashEntry& a, const NodeHashEntry& b) {
                  if (a.hash != b.hash) return a.hash < b.hash;
                  return a.hash32 < b.hash32;
              });
    return entries;
}

// Stage the .ndx beside its final destination so we never expose a half-written index.
template <typename WriteBody>
void write_staged(const std::string& out_path, WriteBody&& write_body) {
    const std::string temp_out_path = make_temp_output_path(out_path);
    try {
        std::ofstream out(temp_out_path, std::ios::binary);
        if (!out) {
            throw std::runtime_error("Failed to open output file: " + temp_out_path);
        }
        write_body(out);
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

}  // namespace

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

NodeIdAssignment assign_node_ids(const std::unordered_map<std::string, unsigned int>& node_to_id,
                                 const std::vector<std::uint32_t>& id_to_comm,
                                 std::uint32_t community_count,
                                 bool use_name_key) {
    const std::size_t n = node_to_id.size();
    if (id_to_comm.size() != n) {
        throw std::runtime_error("Community assignment does not cover every node");
    }
    if (n > static_cast<std::size_t>(std::numeric_limits<std::uint32_t>::max())) {
        throw std::runtime_error("Node count exceeds uint32_t node id range");
    }

    // Graph builders such as PGGB (after odgi sort) and Minigraph-Cactus number
    // segments along the paths, so an integer name is a free locality key. Any
    // name that does not fit the pattern switches the whole graph to the
    // first-appearance key; names are never used as identity here.
    std::vector<std::uint64_t> keys(use_name_key ? n : 0);
    bool all_integer = use_name_key;
    bool all_prefixed = use_name_key;
    std::string prefix;
    bool have_prefix = false;
    for (const auto& [name, int_id] : node_to_id) {
        if (!use_name_key) break;
        if (int_id >= n) {
            throw std::runtime_error("Node id out of range while assigning node ids");
        }
        if (all_integer && parse_u64_digits(name, keys[int_id])) continue;
        all_integer = false;
        if (!all_prefixed) break;
        if (!have_prefix) {
            const auto first_digit = name.find_first_of("0123456789");
            if (first_digit == 0 || first_digit == std::string::npos) {
                all_prefixed = false;
                break;
            }
            prefix = name.substr(0, first_digit);
            have_prefix = true;
        }
        if (name.compare(0, prefix.size(), prefix) != 0 ||
            !parse_u64_digits(std::string_view(name).substr(prefix.size()), keys[int_id])) {
            all_prefixed = false;
            break;
        }
    }
    if (!all_integer && all_prefixed) {
        // Integer names seen before the first prefixed name were parsed as
        // plain integers, which do not carry the prefix; reparse all of them.
        for (const auto& [name, int_id] : node_to_id) {
            if (name.compare(0, prefix.size(), prefix) != 0 ||
                !parse_u64_digits(std::string_view(name).substr(prefix.size()), keys[int_id])) {
                all_prefixed = false;
                break;
            }
        }
    }

    NodeIdAssignment out;
    out.key = all_integer ? NodeOrderKey::integer_name
                          : (all_prefixed ? NodeOrderKey::prefixed_integer
                                          : NodeOrderKey::first_appearance);

    // Counting sort by community keeps internal-id order inside each bucket,
    // which is already the final order for the first-appearance key.
    out.community_starts.assign(static_cast<std::size_t>(community_count) + 1, 0);
    for (const auto comm : id_to_comm) {
        if (comm >= community_count) {
            throw std::runtime_error("Community id out of range while assigning node ids");
        }
        ++out.community_starts[comm + 1];
    }
    for (std::size_t c = 0; c < community_count; ++c) {
        out.community_starts[c + 1] += out.community_starts[c];
    }

    std::vector<std::uint32_t> order(n);
    {
        std::vector<std::uint32_t> fill(out.community_starts.begin(), out.community_starts.end() - 1);
        for (std::uint32_t int_id = 0; int_id < n; ++int_id) {
            order[fill[id_to_comm[int_id]]++] = int_id;
        }
    }
    if (out.key != NodeOrderKey::first_appearance) {
        for (std::size_t c = 0; c < community_count; ++c) {
            std::stable_sort(order.begin() + out.community_starts[c],
                             order.begin() + out.community_starts[c + 1],
                             [&](std::uint32_t a, std::uint32_t b) { return keys[a] < keys[b]; });
        }
    }
    std::vector<std::uint64_t>().swap(keys);

    out.node_ids.resize(n);
    for (std::uint32_t pos = 0; pos < n; ++pos) {
        out.node_ids[order[pos]] = pos;
    }
    return out;
}

void write_node_hash_index(const std::unordered_map<std::string, unsigned int>& node_to_id,
                           const std::vector<std::uint32_t>& id_to_comm,
                           const std::string& out_path) {
    const auto entries = make_sorted_entries(node_to_id, id_to_comm);
    write_staged(out_path, [&](std::ofstream& out) {
        out.write(reinterpret_cast<const char*>(entries.data()),
                  static_cast<std::streamsize>(entries.size() * sizeof(NodeHashEntry)));
    });
}

void write_node_hash_index(const std::unordered_map<std::string, unsigned int>& node_to_id,
                           const NodeIdAssignment& assignment,
                           const std::string& out_path) {
    if (assignment.node_ids.size() != node_to_id.size() ||
        assignment.community_starts.empty() ||
        assignment.community_starts.back() != node_to_id.size()) {
        throw std::runtime_error("Node id assignment does not match the node set");
    }
    const auto entries = make_sorted_entries(node_to_id, assignment.node_ids);

    NodeHashIndexHeaderDisk header{};
    std::memcpy(header.magic, kNodeIndexMagic, sizeof(kNodeIndexMagic));
    header.version = kNodeIndexVersion;
    header.node_order_key = static_cast<std::uint32_t>(assignment.key);
    header.entry_count = entries.size();
    header.community_count = assignment.community_starts.size() - 1;
    header.entries_offset = sizeof(NodeHashIndexHeaderDisk);
    header.community_starts_offset = header.entries_offset + entries.size() * sizeof(NodeHashEntry);

    // Fingerprint the complete name -> id mapping (entries are in name-hash
    // order) so indexes from two different builds of one graph never mix.
    std::uint64_t space = kFnvOffset;
    hash_u32(space, static_cast<std::uint32_t>(header.entry_count));
    hash_u32(space, static_cast<std::uint32_t>(header.community_count));
    for (const auto start : assignment.community_starts) hash_u32(space, start);
    for (const auto& e : entries) hash_u32(space, e.payload);
    header.node_id_space = space == 0 ? 1 : space;

    const std::size_t starts_bytes = assignment.community_starts.size() * sizeof(std::uint32_t);
    const std::size_t body_end = header.community_starts_offset + starts_bytes;
    const std::size_t padding = (kNodeIndexV2SizeResidue + 16 - body_end % 16) % 16;
    const char zeros[16] = {};

    write_staged(out_path, [&](std::ofstream& out) {
        out.write(reinterpret_cast<const char*>(&header), sizeof(header));
        out.write(reinterpret_cast<const char*>(entries.data()),
                  static_cast<std::streamsize>(entries.size() * sizeof(NodeHashEntry)));
        out.write(reinterpret_cast<const char*>(assignment.community_starts.data()),
                  static_cast<std::streamsize>(starts_bytes));
        out.write(zeros, static_cast<std::streamsize>(padding));
    });
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
    const bool legacy = file_size_ % sizeof(NodeHashEntry) == 0;
    if (!legacy && (file_size_ % 16 != kNodeIndexV2SizeResidue ||
                    file_size_ < sizeof(NodeHashIndexHeaderDisk))) {
        ::close(fd_);
        fd_ = -1;
        throw std::runtime_error("Node index file size is invalid: " + path);
    }

    if (file_size_ > 0) {
        void* mapping = mmap(nullptr, file_size_, PROT_READ, MAP_SHARED, fd_, 0);
        if (mapping == MAP_FAILED) {
            ::close(fd_);
            fd_ = -1;
            throw std::runtime_error("mmap failed for node index file: " + path);
        }
        mapping_ = static_cast<const unsigned char*>(mapping);
    }

    if (legacy) {
        data_ = reinterpret_cast<const NodeHashEntry*>(mapping_);
        n_entries_ = file_size_ / sizeof(NodeHashEntry);
        return;
    }

    NodeHashIndexHeaderDisk header{};
    std::memcpy(&header, mapping_, sizeof(header));
    const std::uint64_t expected_starts_offset =
        sizeof(header) + header.entry_count * sizeof(NodeHashEntry);
    const bool valid =
        std::memcmp(header.magic, kNodeIndexMagic, sizeof(kNodeIndexMagic)) == 0 &&
        header.version == kNodeIndexVersion &&
        header.entry_count <= std::numeric_limits<std::uint32_t>::max() &&
        header.community_count < std::numeric_limits<std::uint32_t>::max() &&
        header.entries_offset == sizeof(header) &&
        header.community_starts_offset == expected_starts_offset &&
        file_size_ >= expected_starts_offset + (header.community_count + 1) * sizeof(std::uint32_t) &&
        file_size_ - (expected_starts_offset + (header.community_count + 1) * sizeof(std::uint32_t)) < 16;
    if (!valid) {
        munmap(const_cast<unsigned char*>(mapping_), file_size_);
        mapping_ = nullptr;
        ::close(fd_);
        fd_ = -1;
        throw std::runtime_error("Node index header is invalid or unsupported: " + path);
    }

    data_ = reinterpret_cast<const NodeHashEntry*>(mapping_ + header.entries_offset);
    n_entries_ = static_cast<std::size_t>(header.entry_count);
    community_starts_ = reinterpret_cast<const std::uint32_t*>(mapping_ + header.community_starts_offset);
    community_count_ = static_cast<std::size_t>(header.community_count);
    node_id_space_ = header.node_id_space;
    if (community_starts_[0] != 0 || community_starts_[community_count_] != n_entries_) {
        munmap(const_cast<unsigned char*>(mapping_), file_size_);
        mapping_ = nullptr;
        ::close(fd_);
        fd_ = -1;
        throw std::runtime_error("Node index community table is invalid: " + path);
    }
}

NodeHashIndex::~NodeHashIndex() {
    if (mapping_) {
        munmap(const_cast<unsigned char*>(mapping_), file_size_);
        mapping_ = nullptr;
    }
    if (fd_ != -1) {
        ::close(fd_);
        fd_ = -1;
    }
}

bool NodeHashIndex::find_entry(std::string_view node_id, std::size_t& out_index) const {
    // Binary search lookup through the on-disk hash table.
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
                    out_index = i;
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

bool NodeHashIndex::lookup_rank(std::string_view node_id, std::uint32_t& out_rank) const {
    std::size_t index = 0;
    if (!find_entry(node_id, index)) {
        return false;
    }
    if (!is_legacy()) {
        out_rank = data_[index].payload;
        return true;
    }
    // Legacy files reuse the sorted entry position as the compact node id.
    if (index > static_cast<std::size_t>(std::numeric_limits<std::uint32_t>::max())) {
        throw std::runtime_error("Node hash index rank does not fit into uint32_t");
    }
    out_rank = static_cast<std::uint32_t>(index);
    return true;
}

bool NodeHashIndex::lookup(std::string_view node_id, std::uint32_t& out_com) const {
    std::uint32_t rank = 0;
    if (!lookup_rank(node_id, rank)) {
        return false;
    }

    out_com = community_id_by_rank(rank);
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
    if (is_legacy()) {
        return data_[rank].payload;
    }
    // Node ids are grouped by community, so the owner is the last community
    // starting at or before this id (empty communities share a start).
    const auto* begin = community_starts_;
    const auto* end = community_starts_ + community_count_ + 1;
    return static_cast<std::uint32_t>(std::upper_bound(begin, end, rank) - begin - 1);
}

void require_matching_node_ids(const NodeHashIndex& node_index,
                               std::uint64_t node_count,
                               std::uint64_t node_id_space,
                               const char* other_index) {
    if (node_count != node_index.size()) {
        throw std::runtime_error(std::string(other_index) +
                                 " and .ndx node counts differ; rebuild aligned indexes");
    }
    if (node_id_space != node_index.node_id_space()) {
        throw std::runtime_error(std::string(other_index) +
                                 " was built against a different .ndx node numbering; "
                                 "rebuild it from this .ndx (or rebuild all indexes with index_gfa)");
    }
}

}  // namespace gfaidx::indexer

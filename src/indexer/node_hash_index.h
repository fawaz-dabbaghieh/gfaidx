#ifndef GFAIDX_NODE_HASH_INDEX_H
#define GFAIDX_NODE_HASH_INDEX_H

#include <cstdint>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace gfaidx::indexer {

// On-disk entry format for the node hash index (.ndx). Entries are sorted by
// (hash, hash32). Legacy headerless files store the node's community in
// payload and use the entry rank as node id; v2 files store the node id.
struct NodeHashEntry {
    std::uint64_t hash;
    std::uint32_t hash32;
    std::uint32_t payload;
};

// How index_gfa chose the node id order recorded in a v2 .ndx. Ids are always
// grouped by community; this is the key used inside each community.
enum class NodeOrderKey : std::uint32_t {
    integer_name = 1,      // every segment name is an unsigned integer
    prefixed_integer = 2,  // every name is one shared non-digit prefix + digits
    first_appearance = 3,  // internal id: first appearance in L lines, then S lines
};

// Final node id assignment for v2 indexes. Node ids are dense, grouped by
// community, and ordered by a locality key inside each community so nodes that
// are adjacent on paths get numerically close ids.
struct NodeIdAssignment {
    std::vector<std::uint32_t> node_ids;          // internal id -> node id
    std::vector<std::uint32_t> community_starts;  // community c owns [starts[c], starts[c+1])
    NodeOrderKey key{NodeOrderKey::first_appearance};
};

// FNV-1a hash used for stable node-id hashing.
std::uint64_t fnv1a_hash64(std::string_view s);
std::uint32_t fnv1a_hash32(std::string_view s);

// Sort internal ids by (community, locality key, internal id). Segment names
// are only sort keys here; node identity stays the name hashed into .ndx.
// With use_name_key false, or when some name does not parse, the key is the
// internal id (first appearance in L lines, then S lines).
NodeIdAssignment assign_node_ids(const std::unordered_map<std::string, unsigned int>& node_to_id,
                                 const std::vector<std::uint32_t>& id_to_comm,
                                 std::uint32_t community_count,
                                 bool use_name_key = true);

// Write a legacy headerless .ndx where node id == sorted entry rank.
void write_node_hash_index(const std::unordered_map<std::string, unsigned int>& node_to_id,
                           const std::vector<std::uint32_t>& id_to_comm,
                           const std::string& out_path);

// Write a v2 .ndx using the supplied node id assignment.
void write_node_hash_index(const std::unordered_map<std::string, unsigned int>& node_to_id,
                           const NodeIdAssignment& assignment,
                           const std::string& out_path);

// Streaming on-disk lookup for node->community via binary search.
class NodeHashIndex {
public:
    explicit NodeHashIndex(const std::string& path);
    ~NodeHashIndex();

    NodeHashIndex(const NodeHashIndex&) = delete;
    NodeHashIndex& operator=(const NodeHashIndex&) = delete;

    [[nodiscard]] std::uint64_t size() const;
    bool lookup(std::string_view node_id, std::uint32_t& out_com) const;
    // Resolve a segment name to its node id, the integer node space shared by
    // .pdx, .lnx and .cdx. Legacy files use the sorted entry rank as id.
    bool lookup_rank(std::string_view node_id, std::uint32_t& out_rank) const;
    [[nodiscard]] std::uint32_t community_id_by_rank(std::uint32_t rank) const;

    // Fingerprint of the name -> node id mapping. Zero for legacy files, whose
    // ids depend only on the node set. Path indexes record the value they were
    // built against so a .pdx is never paired with a differently numbered .ndx.
    [[nodiscard]] std::uint64_t node_id_space() const { return node_id_space_; }
    [[nodiscard]] bool is_legacy() const { return community_starts_ == nullptr; }

private:
    bool find_entry(std::string_view node_id, std::size_t& out_index) const;

    // The node hash index is mmap-backed and only supported on Unix-like
    // systems. gfaidx does not target Windows builds.
    int fd_ = -1;
    const unsigned char* mapping_ = nullptr;
    const NodeHashEntry* data_ = nullptr;
    std::size_t file_size_ = 0;
    std::size_t n_entries_ = 0;
    const std::uint32_t* community_starts_ = nullptr;
    std::size_t community_count_ = 0;
    std::uint64_t node_id_space_ = 0;
};

// 32-bit tag of a node id space for sidecars that only have a spare uint32
// header field (.cdx). Zero stays zero (legacy ids); any other space maps to
// a non-zero tag.
inline std::uint32_t node_id_space_tag(std::uint64_t node_id_space) {
    if (node_id_space == 0) return 0;
    const auto tag = static_cast<std::uint32_t>(node_id_space ^ (node_id_space >> 32));
    return tag == 0 ? 1u : tag;
}

// Throw unless a path index built against node_id_space can be used with this
// .ndx (same node count and the same id numbering).
void require_matching_node_ids(const NodeHashIndex& node_index,
                               std::uint64_t node_count,
                               std::uint64_t node_id_space,
                               const char* other_index);

// Same check for a sidecar that stores node_id_space_tag() instead.
void require_matching_node_id_tag(const NodeHashIndex& node_index,
                                  std::uint64_t node_count,
                                  std::uint32_t node_id_space_tag,
                                  const char* other_index);

}  // namespace gfaidx::indexer

#endif  // GFAIDX_NODE_HASH_INDEX_H

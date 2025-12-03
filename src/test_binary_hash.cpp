#include <iostream>
#include <vector>
#include <string>
#include <string_view>
#include <fstream>
#include <algorithm>
#include <stdexcept>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

//---------------------------------------------
// 64-bit FNV-1a hash
//---------------------------------------------
uint64_t hash_id(std::string_view s) {
    const uint64_t FNV_OFFSET = 1469598103934665603ULL;
    const uint64_t FNV_PRIME  = 1099511628211ULL;
    uint64_t h = FNV_OFFSET;
    for (unsigned char c : s) {
        h ^= c;
        h *= FNV_PRIME;
    }
    return h;
}

//---------------------------------------------
// A single index entry (16 bytes)
//---------------------------------------------
struct IndexEntry {
    uint64_t hash;          // 8 bytes
    uint32_t community_id;  // 4 bytes
    uint32_t padding;       // 4 bytes (explicit padding for alignment)
}; // sizeof(IndexEntry) == 16

//---------------------------------------------
// Build the index file on disk
//---------------------------------------------
void build_index(const std::vector<std::pair<std::string, uint32_t>>& nodes,
                 const std::string& out_path)
{
    std::vector<IndexEntry> entries;
    entries.reserve(nodes.size());

    for (const auto& p : nodes) {
        IndexEntry e;
        e.hash         = hash_id(p.first);
        e.community_id = p.second;
        e.padding      = 0;
        entries.push_back(e);
    }

    // Sort by hash for binary search
    std::sort(entries.begin(), entries.end(),
              [](const IndexEntry& a, const IndexEntry& b) {
                  return a.hash < b.hash;
              });

    // Write the binary file
    std::ofstream out(out_path, std::ios::binary);
    if (!out) throw std::runtime_error("Failed to open output file");

    out.write(reinterpret_cast<const char*>(entries.data()),
              entries.size() * sizeof(IndexEntry));

    out.close();
}

//---------------------------------------------
// Memory-mapped index reader
//---------------------------------------------
class OnDiskIndex {
public:
    explicit OnDiskIndex(const std::string& path) {
        fd_ = ::open(path.c_str(), O_RDONLY);
        if (fd_ == -1)
            throw std::runtime_error("Failed to open index file");

        struct stat st{};
        if (fstat(fd_, &st) == -1)
            throw std::runtime_error("Failed to stat index file");

        file_size_ = st.st_size;
        if (file_size_ % sizeof(IndexEntry) != 0)
            throw std::runtime_error("Index file size is invalid");

        n_entries_ = file_size_ / sizeof(IndexEntry);

        data_ = static_cast<const IndexEntry*>(
            mmap(nullptr, file_size_, PROT_READ, MAP_SHARED, fd_, 0)
        );
        if (data_ == MAP_FAILED)
            throw std::runtime_error("mmap failed");
    }

    ~OnDiskIndex() {
        if (data_) munmap(const_cast<IndexEntry*>(data_), file_size_);
        if (fd_ != -1) close(fd_);
    }

    // Lookup the community ID for a node name
    bool lookup(std::string_view name, uint32_t& out_com) const {
        uint64_t h = hash_id(name);

        size_t lo = 0, hi = n_entries_;
        while (lo < hi) {
            size_t mid = lo + (hi - lo) / 2;
            uint64_t mh = data_[mid].hash;

            if (mh < h) lo = mid + 1;
            else if (mh > h) hi = mid;
            else {
                out_com = data_[mid].community_id;
                return true;    // found exact hash match
            }
        }
        return false; // not found
    }

private:
    int fd_ = -1;
    const IndexEntry* data_ = nullptr;
    size_t file_size_ = 0;
    size_t n_entries_ = 0;
};

//---------------------------------------------
// Main demonstration
//---------------------------------------------
int main() {
    const std::string index_file = "node_index.bin";

    //--------------------------------------------------
    // 1. Build a test index file
    //--------------------------------------------------
    std::vector<std::pair<std::string, uint32_t>> test_nodes = {
        {"nodeA", 1},
        {"nodeB", 1},
        {"nodeC", 2},
        {"nodeXYZ", 5},
        {"scaffold12", 3}
    };

    build_index(test_nodes, index_file);
    std::cout << "Index built: " << index_file << "\n";

    //--------------------------------------------------
    // 2. Load the index from disk (memory-mapped)
    //--------------------------------------------------
    OnDiskIndex idx(index_file);

    //--------------------------------------------------
    // 3. Test lookups
    //--------------------------------------------------
    const std::vector<std::string> queries = {
        "nodeA",
        "nodeXYZ",
        "scaffold12",
        "not_in_index"
    };

    for (const auto& q : queries) {
        uint32_t com;
        bool ok = idx.lookup(q, com);
        if (ok)
            std::cout << q << " → community " << com << "\n";
        else
            std::cout << q << " → not found\n";
    }

    return 0;
}

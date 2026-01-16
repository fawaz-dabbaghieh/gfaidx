#include <iostream>
#include <vector>
#include <string>
#ifdef __linux__
#include <cstdint>
#include<cstring>
#include <sys/wait.h>
#endif
#include <string_view>
#include <fstream>
#include <algorithm>
#include <stdexcept>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>


// 64-bit FNV-1a hash
uint64_t fnv1a_hash(std::string_view s) {
    constexpr uint64_t FNV_OFFSET = 1469598103934665603ULL;
    const uint64_t FNV_PRIME  = 1099511628211ULL;
    uint64_t h = FNV_OFFSET;
    for (unsigned char c : s) {
        h ^= c;
        h *= FNV_PRIME;
    }
    return h;
}

// 32-bit FNV-1a hash
// uint32_t hash_id_32(std::string_view s) {
//     constexpr uint32_t FNV_OFFSET = 2166136261U;
//     const uint32_t FNV_PRIME  = 16777619U;
//     uint32_t h = FNV_OFFSET;
//     for (unsigned char c : s) {
//         h ^= c;
//         h *= FNV_PRIME;
//     }
//     return h;
// }

// the hash entries, size of 16 bytes, probably can be made smaller
// at the moment there are 4 bits padding which are wasted, I can use them for another smaller hash to avoid collisions
// i.e., do another has that is 32 bits, and first look at the 64 then the 32 bit hash, even for a couple hundred million
// strings, it'll be almost impossible to get a hash collision.
struct IndexEntry {
    uint64_t hash;          // 8 bytes
    uint32_t community_id;  // 4 bytes
};


// building index of strings
void build_index(const std::vector<std::pair<std::string, uint32_t>>& nodes,
                 const std::string& out_path)
{
    std::vector<IndexEntry> entries;
    entries.reserve(nodes.size());

    // later this should read from some stream of node ids and their communities instead of reading from some pairs
    // stored on RAM
    for (const auto& p : nodes) {
        IndexEntry e{};
        e.hash = fnv1a_hash(p.first);
        e.community_id = p.second;
        entries.push_back(e);
    }

    // Sorting hash
    // if I add a second 32 bits hash, then I need to change this comparison here in the sort
    std::sort(entries.begin(), entries.end(),
              [](const IndexEntry& a, const IndexEntry& b) {
                  return a.hash < b.hash;
              });

    // writing binary
    std::ofstream out(out_path, std::ios::binary);
    if (!out) {
        throw std::runtime_error("Failed to open output file");
    }

    out.write(reinterpret_cast<const char*>(entries.data()),entries.size() * sizeof(IndexEntry));
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

    // destructor
    ~OnDiskIndex() {
        if (data_) munmap(const_cast<IndexEntry*>(data_), file_size_);
        if (fd_ != -1) close(fd_);
    }

    // Binary search lookup
    bool lookup(std::string_view name, uint32_t& out_com) const {
        uint64_t h = fnv1a_hash(name);

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

class OnDiskIndexStreaming {
public:
    explicit OnDiskIndexStreaming(const std::string& path) {
        file_.open(path, std::ios::binary);
        if (!file_)
            throw std::runtime_error("Failed to open index file");

        // I can probably replace this by adding the size
        // of the index as the first uint_32 or 64 value
        // so I just need to read the first 8 or 4 bytes
        // and the binary search starts from the entry after.

        // Get file size
        file_.seekg(0, std::ios::end);
        file_size_ = file_.tellg();
        file_.seekg(0, std::ios::beg);

        if (file_size_ % index_entry_size_ != 0)
            throw std::runtime_error("Index file size is invalid");

        n_entries_ = file_size_ / index_entry_size_;
    }

    // Lookup the community ID for a node name
    bool lookup(std::string_view node_id, uint32_t& out_com) const {
        // binary search implementation for the on-disk
        const uint64_t query_hash = fnv1a_hash(node_id);

        size_t low_val = 0, high_val = n_entries_;
        while (low_val < high_val) {
            const size_t mid_val = low_val + (high_val - low_val) / 2;

            // Seek to the entry and read it
            file_.seekg(mid_val * index_entry_size_, std::ios::beg);
            IndexEntry entry{};
            file_.read(reinterpret_cast<char*>(&entry), index_entry_size_);

            if (!file_)
                throw std::runtime_error("Failed to read from index file");

            uint64_t mid_val_hash = entry.hash;

            if (mid_val_hash < query_hash) low_val = mid_val + 1;
            else if (mid_val_hash > query_hash) high_val = mid_val;
            else {
                out_com = entry.community_id;
                return true;
            }
        }
        return false;
    }

private:
    mutable std::ifstream file_;
    size_t index_entry_size_ = sizeof(IndexEntry);
    size_t file_size_ = 0;
    size_t n_entries_ = 0;
};


int main(int argc, char** argv) {
    if (argc < 5) { std::cerr << "usage: " << argv[0] << " nodes_communites file then queries file then the index 0 for building the index and 1 for querying\n"; return 1; }


    if (strcmp(argv[4], "0") == 0) {
        const std::string index_file = argv[3];

        // some sample data to build an index for
        std::ifstream in_nodes_file(argv[1]);
        std::vector<std::pair<std::string, uint32_t>> nodes;
        std::string node_id;
        int community_id;
        while (in_nodes_file >> node_id >> community_id) {
            nodes.emplace_back(node_id, community_id);
        }
        build_index(nodes, index_file);
        std::cout << "Index built: " << index_file << "\n";
    }


    if (strcmp(argv[4], "1") == 0) {
        std::string node_id;
        std::ifstream in_queries(argv[2]);
        std::vector<std::string> queries;
        while (in_queries >> node_id) {
            queries.push_back(node_id);
        }

        {
            OnDiskIndex disk_hash(argv[3]);

            for (const auto& q : queries) {
                uint32_t com;
                bool ok = disk_hash.lookup(q, com);
                if (ok)
                    std::cout << q << " → community " << com << "\n";
                else
                    std::cout << q << " → not found\n";
            }
        }

        // {
        //     OnDiskIndexStreaming disk_hash(argv[3]);
        //
        //     for (const auto& q : queries) {
        //         uint32_t com;
        //         bool ok = disk_hash.lookup(q, com);
        //         if (ok)
        //             std::cout << q << " → community " << com << "\n";
        //         else
        //             std::cout << q << " → not found\n";
        //     }
        // }
    }

    return 0;
}

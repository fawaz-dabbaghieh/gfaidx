#ifndef GFAIDX_TEXT_HANDLE_CACHE_H
#define GFAIDX_TEXT_HANDLE_CACHE_H

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <list>
#include <string_view>
#include <unordered_map>
#include <vector>

// LRU cache for raw text temp files to limit open file descriptors.
class TextHandleCache {
public:
    TextHandleCache(std::vector<std::filesystem::path> paths, std::size_t max_open);

    void write_line(std::uint32_t cid, std::string_view line_no_nl);
    void close_all();

private:
    struct OpenRec {
        std::ofstream file;
        std::list<std::uint32_t>::iterator it;
    };

    std::ofstream& get_handle_(std::uint32_t cid);

    std::vector<std::filesystem::path> paths_;
    std::size_t max_open_;
    std::unordered_map<std::uint32_t, OpenRec> open_;
    std::list<std::uint32_t> lru_;
};

#endif //GFAIDX_TEXT_HANDLE_CACHE_H

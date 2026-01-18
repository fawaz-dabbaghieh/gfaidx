#include "chunk/text_handle_cache.h"

#include <stdexcept>

TextHandleCache::TextHandleCache(std::vector<std::filesystem::path> paths, std::size_t max_open)
    : paths_(std::move(paths)), max_open_(max_open) {}

void TextHandleCache::write_line(std::uint32_t cid, std::string_view line_no_nl) {
    std::ofstream& f = get_handle_(cid);
    f.write(line_no_nl.data(), static_cast<std::streamsize>(line_no_nl.size()));
    f.put('\n');
    if (!f) throw std::runtime_error("write failed for " + paths_.at(cid).string());
}

void TextHandleCache::close_all() {
    for (auto& kv : open_) {
        kv.second.file.close();
    }
    open_.clear();
    lru_.clear();
}

std::ofstream& TextHandleCache::get_handle_(std::uint32_t cid) {
    auto it = open_.find(cid);
    if (it != open_.end()) {
        lru_.erase(it->second.it);
        lru_.push_front(cid);
        it->second.it = lru_.begin();
        return it->second.file;
    }

    // Evict if needed
    if (open_.size() >= max_open_) {
        std::uint32_t evict = lru_.back();
        lru_.pop_back();
        auto eit = open_.find(evict);
        if (eit != open_.end()) {
            eit->second.file.close();
            open_.erase(eit);
        }
    }

    const std::filesystem::path& p = paths_.at(cid);

    std::filesystem::create_directories(p.parent_path());

    // Append mode; create if missing.
    std::ofstream f(p, std::ios::binary | std::ios::out | std::ios::app);
    if (!f) throw std::runtime_error("Failed to open temp text file: " + p.string());

    lru_.push_front(cid);
    OpenRec rec{std::move(f), lru_.begin()};
    auto [ins_it, ok] = open_.emplace(cid, std::move(rec));
    return ins_it->second.file;
}

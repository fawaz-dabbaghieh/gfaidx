#ifndef GFAIDX_MEMORY_H
#define GFAIDX_MEMORY_H

#include <cstdint>
#include <string>
#include <unordered_map>

std::uint64_t get_current_rss_bytes();
std::string format_bytes(std::uint64_t bytes);
void log_memory(const std::string& label);
void log_map_stats(const std::string& label,
                   std::size_t size,
                   std::size_t buckets,
                   float load_factor);
void log_map_stats(const std::string& label,
                   const std::unordered_map<std::string, unsigned int>& map);

#endif // GFAIDX_MEMORY_H

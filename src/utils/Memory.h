#ifndef GFAIDX_MEMORY_H
#define GFAIDX_MEMORY_H

#include <cstdint>
#include <string>

std::uint64_t get_current_rss_bytes();
std::string format_bytes(std::uint64_t bytes);
void log_memory(const std::string& label);
void log_map_stats(const std::string& label,
                   std::size_t size,
                   std::size_t buckets,
                   float load_factor);

#endif // GFAIDX_MEMORY_H

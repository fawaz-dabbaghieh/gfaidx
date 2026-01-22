#include "utils/Memory.h"

#include <iostream>
#include <sstream>

#include "utils/Timer.h"

#if defined(__linux__)
#include <fstream>
#include <unistd.h>
#elif defined(__APPLE__)
#include <mach/mach.h>
#endif

std::uint64_t get_current_rss_bytes() {
#if defined(__linux__)
    std::ifstream statm("/proc/self/statm");
    if (!statm) return 0;
    std::uint64_t size_pages = 0;
    std::uint64_t resident_pages = 0;
    statm >> size_pages >> resident_pages;
    if (!statm) return 0;
    long page_size = sysconf(_SC_PAGESIZE);
    if (page_size <= 0) return 0;
    return resident_pages * static_cast<std::uint64_t>(page_size);
#elif defined(__APPLE__)
    mach_task_basic_info info{};
    mach_msg_type_number_t count = MACH_TASK_BASIC_INFO_COUNT;
    if (task_info(mach_task_self(), MACH_TASK_BASIC_INFO,
                  reinterpret_cast<task_info_t>(&info), &count) != KERN_SUCCESS) {
        return 0;
    }
    return info.resident_size;
#else
    return 0;
#endif
}

std::string format_bytes(const std::uint64_t bytes) {
    const char* units[] = {"B", "KB", "MB", "GB", "TB"};
    auto value = static_cast<double>(bytes);
    std::size_t unit = 0;
    while (value >= 1024.0 && unit + 1 < sizeof(units) / sizeof(units[0])) {
        value /= 1024.0;
        unit++;
    }
    std::ostringstream out;
    out.setf(std::ios::fixed);
    out.precision(2);
    out << value << " " << units[unit];
    return out.str();
}

void log_memory(const std::string& label) {
    const std::uint64_t rss = get_current_rss_bytes();
    if (rss == 0) {
        std::cout << get_time() << ": " << label << " (RSS unavailable)" << std::endl;
        return;
    }
    std::cout << get_time() << ": " << label << " (RSS " << format_bytes(rss) << ")" << std::endl;
}

void log_map_stats(const std::string& label,
                   std::size_t size,
                   std::size_t buckets,
                   float load_factor) {

    std::cout << get_time() << ": " << label
              << " size=" << size
              << " buckets=" << buckets
              << " load_factor=" << load_factor
              << std::endl;
}

void log_map_stats(const std::string& label,
                   const std::unordered_map<std::string, unsigned int>& map) {
    std::uint64_t key_capacity_bytes = 0;
    for (const auto& p : map) {
        key_capacity_bytes += p.first.capacity();
    }
    const std::uint64_t bucket_bytes = map.bucket_count() * sizeof(void*);
    const std::uint64_t value_bytes = map.size() * sizeof(unsigned int);
    const std::uint64_t approx_total = key_capacity_bytes + bucket_bytes + value_bytes;

    std::cout << get_time() << ": " << label
              << " size=" << map.size()
              << " buckets=" << map.bucket_count()
              << " load_factor=" << map.load_factor()
              << " approx_key_bytes=" << format_bytes(key_capacity_bytes)
              << " approx_bucket_bytes=" << format_bytes(bucket_bytes)
              << " approx_total=" << format_bytes(approx_total)
              << std::endl;
}

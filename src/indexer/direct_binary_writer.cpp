#include "indexer/direct_binary_writer.h"

#include <cerrno>
#include <cstring>
#include <fstream>
#include <stdexcept>
#include <vector>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

static void throw_io_error(const std::string& message) {
    throw std::runtime_error(message + ": " + std::strerror(errno));
}

void write_binary_graph_from_edgelist(const std::string& edge_list_path,
                                      const std::string& out_binary_path,
                                      std::uint32_t num_nodes) {

    // maybe I should change the ifstream to my reader at some point, but for now it's ok
    std::ifstream in(edge_list_path);
    if (!in) {
        throw std::runtime_error("Failed to open edge list: " + edge_list_path);
    }

    std::vector<std::uint64_t> degrees(num_nodes, 0);

    std::uint32_t src = 0;
    std::uint32_t dst = 0;
    while (in >> src >> dst) {
        if (src >= num_nodes || dst >= num_nodes) {
            throw std::runtime_error("Edge list node id out of range");
        }
        degrees[src] += 1;
        if (src != dst) {
            degrees[dst] += 1;
        }
    }

    std::uint64_t total_links = 0;
    for (std::uint32_t i = 0; i < num_nodes; ++i) {
        total_links += degrees[i];
        degrees[i] = total_links; // cumulative degree
    }

    const std::uint64_t header_bytes = sizeof(std::uint32_t) + sizeof(std::uint64_t) * num_nodes;
    const std::uint64_t links_bytes = sizeof(std::uint32_t) * total_links;
    const std::uint64_t total_bytes = header_bytes + links_bytes;

    int fd = ::open(out_binary_path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (fd == -1) {
        throw_io_error("Failed to open output file");
    }

    if (ftruncate(fd, static_cast<off_t>(total_bytes)) != 0) {
        ::close(fd);
        throw_io_error("Failed to resize output file");
    }

    void* mapped = mmap(nullptr, static_cast<size_t>(total_bytes), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (mapped == MAP_FAILED) {
        ::close(fd);
        throw_io_error("mmap failed");
    }

    unsigned char* base = static_cast<unsigned char*>(mapped);
    std::memcpy(base, &num_nodes, sizeof(std::uint32_t));
    std::memcpy(base + sizeof(std::uint32_t), degrees.data(), sizeof(std::uint64_t) * num_nodes);

    auto* links = reinterpret_cast<std::uint32_t*>(base + header_bytes);

    std::vector<std::uint64_t> cursor(num_nodes, 0);
    std::uint64_t prev = 0;
    for (std::uint32_t i = 0; i < num_nodes; ++i) {
        cursor[i] = prev;
        prev = degrees[i];
    }

    in.clear();
    in.seekg(0, std::ios::beg);
    while (in >> src >> dst) {
        links[cursor[src]++] = dst;
        if (src != dst) {
            links[cursor[dst]++] = src;
        }
    }

    if (msync(mapped, static_cast<size_t>(total_bytes), MS_SYNC) != 0) {
        munmap(mapped, static_cast<size_t>(total_bytes));
        ::close(fd);
        throw_io_error("msync failed");
    }

    munmap(mapped, static_cast<size_t>(total_bytes));
    ::close(fd);
}

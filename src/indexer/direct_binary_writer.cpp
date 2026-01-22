// Small example to understand this part and how the binary file is written. (from my paper example)
// 0 1
// 0 2
// 1 2
// 3 4
//
// countring degrees
// Node 0: connected to 1,2 → degree = 2
// Node 1: connected to 0,2 → degree = 2
// Node 2: connected to 0,1 → degree = 2
// Node 3: connected to 4 → degree = 1
// Node 4: connected to 3 → degree = 1
//
// degrees (non‑cumulative) = [2, 2, 2, 1, 1]
// degrees (cumulative)     = [2, 4, 6, 7, 8]
//
// the cumulative tells us where each adjacency list ends in the list of adjacencies basically
// i.e., first node has (2-0 = 2 edges), second node has (4-2=2) edges ... final node has (8-7=1) edges.
//
// later for the adjacency list (the cursor part)
//
// cursor[0] = 0
// cursor[1] = degrees[0] = 2
// cursor[2] = degrees[1] = 4
// cursor[3] = degrees[2] = 6
// cursor[4] = degrees[3] = 7
//
// cursor = [0, 2, 4, 6, 7]  pointers basically to the neighbors in links
//
// On the second stream through the edge list, we are filling the links vector
// we increment cursor here for that value after getting the value to point to writing the next edge
// Edge 0 1
// links[cursor[0]++] = 1   // links[0] = 1, cursor[0] increment to 1
// links[cursor[1]++] = 0   // links[2] = 0, cursor[1] increment to 3
//
// Edge 0 2
// links[cursor[0]++] = 2   // links[1] = 2, cursor[0] increment to 2
// links[cursor[2]++] = 0   // links[4] = 0, cursor[2] increment to 5
//
// Edge 1 2
// links[cursor[1]++] = 2   // links[3] = 2, cursor[1] increment to 4
// links[cursor[2]++] = 1   // links[5] = 1, cursor[2] increment to 6
//
// Edge 3 4
// links[cursor[3]++] = 4   // links[6] = 4, cursor[3] increment to 7
// links[cursor[4]++] = 3   // links[7] = 3, cursor[4] increment to 8
//
// Final links array (indices 0..7):
//
// [1, 2, 0, 2, 0, 1, 4, 3]

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
        // don't count self edges twice
        degrees[src] += 1;
        if (src != dst) {
            degrees[dst] += 1;
        }
    }

    // the binary louvain graph expects the cumulative degree
    std::uint64_t total_links = 0;
    for (std::uint32_t i = 0; i < num_nodes; ++i) {
        total_links += degrees[i];
        degrees[i] = total_links; // cumulative degree
    }

    // the binary file has 4bytes of n_nodes, then degrees, so we set up how much to write
    const std::uint64_t header_bytes = sizeof(std::uint32_t) + sizeof(std::uint64_t) * num_nodes;
    const std::uint64_t links_bytes = sizeof(std::uint32_t) * total_links;
    const std::uint64_t total_bytes = header_bytes + links_bytes;

    // writing with mmap so we don't hold everything in RAM to write links directly to disk
    int fd = ::open(out_binary_path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (fd == -1) {
        throw_io_error("Failed to open output file");
    }

    if (ftruncate(fd, static_cast<off_t>(total_bytes)) != 0) {
        ::close(fd);
        throw_io_error("Failed to resize output file");
    }

    // create memory mapping, nullptr so the OS chooses the pointer, length of how much we'll be writing
    // read and write flags, MAP_SHARED for changes to be written back to the file
    // and at 0 offset
    void* mapped = mmap(nullptr, static_cast<size_t>(total_bytes), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (mapped == MAP_FAILED) {
        ::close(fd);
        throw_io_error("mmap failed");
    }

    // casting to a byte-pointer for byte arithmetic
    auto* base = static_cast<unsigned char*>(mapped);
    // write the 4 bytes number of nodes
    std::memcpy(base, &num_nodes, sizeof(std::uint32_t));
    // write the the cumulative degree array
    std::memcpy(base + sizeof(std::uint32_t), degrees.data(), sizeof(std::uint64_t) * num_nodes);

    // links points now to after what we have written, i.e., to the start of the adjacency array that we'll be writing
    auto* links = reinterpret_cast<std::uint32_t*>(base + header_bytes);

    // cursor[i] is initialized to the start offset of node i’s adjacency range.
    // computed by keeping prev = degrees[i-1].
    std::vector<std::uint64_t> cursor(num_nodes, 0);
    std::uint64_t prev = 0;
    for (std::uint32_t i = 0; i < num_nodes; ++i) {
        cursor[i] = prev;
        prev = degrees[i];
    }


    in.clear();
    in.seekg(0, std::ios::beg);
    // passing through the edge list again
    // each time we see an edge (src, dst):
    //   1 - write dst into node src’s list at cursor[src], then increment.
    //   2 - If it’s not a self‑loop, also write src into node dst’s list.
    while (in >> src >> dst) {
        links[cursor[src]++] = dst;
        if (src != dst) {
            links[cursor[dst]++] = src;
        }
    }

    // Explicitly flush dirty pages to disk. but not strictly required
    if (msync(mapped, static_cast<size_t>(total_bytes), MS_SYNC) != 0) {
        munmap(mapped, static_cast<size_t>(total_bytes)); // release mapping
        ::close(fd);
        throw_io_error("msync failed");
    }

    munmap(mapped, static_cast<size_t>(total_bytes));  // release mapping
    ::close(fd);
}

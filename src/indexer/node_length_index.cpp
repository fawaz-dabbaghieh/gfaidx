#include "indexer/node_length_index.h"

#include <cstring>
#include <fstream>
#include <limits>
#include <stdexcept>
#include <string_view>
#include <vector>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "fs/fs_helpers.h"
#include "indexer/node_hash_index.h"

namespace gfaidx::indexer {
namespace {

constexpr char kNodeLengthIndexMagic[8] = {'G', 'F', 'A', 'L', 'N', 'X', '0', '1'};
constexpr std::uint32_t kNodeLengthIndexVersion = 1;
constexpr std::uint32_t kNodeLengthValueWidth = 4;

struct NodeLengthIndexHeaderDisk {
    char magic[8]{};
    std::uint32_t version{};
    std::uint32_t value_width{};
    std::uint64_t node_count{};
};

static_assert(sizeof(NodeLengthIndexHeaderDisk) == 24,
              "Unexpected node-length-index header size");

bool test_seen_bit(const std::vector<std::uint64_t>& bits, std::uint32_t value) {
    const std::size_t word = value / 64;
    const unsigned bit = value % 64;
    return (bits[word] & (1ULL << bit)) != 0;
}

void set_seen_bit(std::vector<std::uint64_t>& bits, std::uint32_t value) {
    const std::size_t word = value / 64;
    const unsigned bit = value % 64;
    bits[word] |= (1ULL << bit);
}

bool parse_s_line_name_and_length(std::string_view line,
                                  std::string& out_name,
                                  std::uint32_t& out_length) {
    const auto t1 = line.find('\t');
    if (t1 == std::string_view::npos) return false;
    const auto t2 = line.find('\t', t1 + 1);
    if (t2 == std::string_view::npos) return false;
    const auto t3 = line.find('\t', t2 + 1);

    out_name.assign(line.substr(t1 + 1, t2 - (t1 + 1)));
    const auto seq = (t3 == std::string_view::npos)
        ? line.substr(t2 + 1)
        : line.substr(t2 + 1, t3 - (t2 + 1));

    std::uint64_t parsed_length = 0;
    if (seq != "*") {
        parsed_length = static_cast<std::uint64_t>(seq.size());
    } else {
        if (t3 == std::string_view::npos) return false;
        bool found_length = false;
        std::size_t pos = t3 + 1;
        while (pos < line.size()) {
            const auto next_tab = line.find('\t', pos);
            const auto end = (next_tab == std::string_view::npos) ? line.size() : next_tab;
            const auto field = line.substr(pos, end - pos);
            if (field.substr(0, 5) == "LN:i:" && field.size() > 5) {
                try {
                    parsed_length = static_cast<std::uint64_t>(
                        std::stoull(std::string(field.substr(5))));
                } catch (const std::exception&) {
                    return false;
                }
                found_length = true;
                break;
            }
            if (next_tab == std::string_view::npos) break;
            pos = next_tab + 1;
        }
        if (!found_length) return false;
    }

    if (parsed_length > std::numeric_limits<std::uint32_t>::max()) {
        throw std::runtime_error("Node length exceeds uint32_t range for node '" +
                                 out_name + "'");
    }
    out_length = static_cast<std::uint32_t>(parsed_length);
    return true;
}

}  // namespace

void NodeLengthIndexReader::close_mapping() {
    if (mapping_) {
        munmap(mapping_, file_size_);
        mapping_ = nullptr;
    }
    if (fd_ != -1) {
        ::close(fd_);
        fd_ = -1;
    }
    lengths_ = nullptr;
    file_size_ = 0;
    node_count_ = 0;
}

void build_node_length_index(const std::string& input_gfa,
                             const std::string& node_index_path,
                             const std::string& output_path,
                             const Reader::Options& reader_options) {
    if (file_exists(output_path.c_str())) {
        throw std::runtime_error("Node length index already exists: " + output_path);
    }

    NodeHashIndex node_index(node_index_path);
    if (node_index.size() > std::numeric_limits<std::uint32_t>::max()) {
        throw std::runtime_error("Node count exceeds uint32_t rank range for .lnx");
    }

    std::vector<std::uint32_t> lengths(static_cast<std::size_t>(node_index.size()), 0);
    std::vector<std::uint64_t> seen((static_cast<std::size_t>(node_index.size()) + 63) / 64, 0);
    std::uint64_t seen_count = 0;

    Reader reader(reader_options);
    if (!reader.open(input_gfa)) {
        throw std::runtime_error("Failed to open GFA for node length indexing: " + input_gfa);
    }

    std::string_view line;
    std::string node_name;
    std::uint32_t node_length = 0;
    while (reader.read_line(line)) {
        if (line.empty() || line[0] != 'S') continue;

        if (!parse_s_line_name_and_length(line, node_name, node_length)) {
            throw std::runtime_error("Could not derive segment length while building .lnx");
        }

        std::uint32_t rank = 0;
        if (!node_index.lookup_rank(node_name, rank)) {
            throw std::runtime_error("Node from GFA was not found in .ndx while building .lnx: " +
                                     node_name);
        }
        if (test_seen_bit(seen, rank)) {
            throw std::runtime_error("Duplicate node rank while building .lnx: " + node_name);
        }
        set_seen_bit(seen, rank);
        ++seen_count;
        lengths[rank] = node_length;
    }

    if (seen_count != node_index.size()) {
        throw std::runtime_error("The GFA node set does not match the .ndx while building .lnx");
    }

    NodeLengthIndexHeaderDisk header{};
    std::memcpy(header.magic, kNodeLengthIndexMagic, sizeof(header.magic));
    header.version = kNodeLengthIndexVersion;
    header.value_width = kNodeLengthValueWidth;
    header.node_count = node_index.size();

    const auto staged_output = make_temp_output_path(output_path);
    try {
        std::ofstream out(staged_output, std::ios::binary | std::ios::trunc);
        if (!out) {
            throw std::runtime_error("Failed to open node length index output: " + staged_output);
        }
        out.write(reinterpret_cast<const char*>(&header), sizeof(header));
        if (!lengths.empty()) {
            out.write(reinterpret_cast<const char*>(lengths.data()),
                      static_cast<std::streamsize>(lengths.size() * sizeof(std::uint32_t)));
        }
        out.close();
        if (!out) {
            throw std::runtime_error("Failed while writing node length index: " + output_path);
        }
        rename_path_or_throw(staged_output, output_path);
    } catch (...) {
        remove_path_if_exists(staged_output);
        throw;
    }
}

NodeLengthIndexReader::NodeLengthIndexReader(const std::string& path) {
    fd_ = ::open(path.c_str(), O_RDONLY);
    if (fd_ == -1) {
        throw std::runtime_error("Failed to open node length index: " + path);
    }

    struct stat st{};
    if (fstat(fd_, &st) == -1) {
        ::close(fd_);
        fd_ = -1;
        throw std::runtime_error("Failed to stat node length index: " + path);
    }
    file_size_ = static_cast<std::size_t>(st.st_size);
    if (file_size_ < sizeof(NodeLengthIndexHeaderDisk)) {
        ::close(fd_);
        fd_ = -1;
        throw std::runtime_error("Node length index is too small: " + path);
    }

    mapping_ = mmap(nullptr, file_size_, PROT_READ, MAP_SHARED, fd_, 0);
    if (mapping_ == MAP_FAILED) {
        mapping_ = nullptr;
        ::close(fd_);
        fd_ = -1;
        throw std::runtime_error("mmap failed for node length index: " + path);
    }

    const auto* header = static_cast<const NodeLengthIndexHeaderDisk*>(mapping_);
    if (std::memcmp(header->magic, kNodeLengthIndexMagic, sizeof(header->magic)) != 0) {
        close_mapping();
        throw std::runtime_error("Invalid node length index magic: " + path);
    }
    if (header->version != kNodeLengthIndexVersion) {
        close_mapping();
        throw std::runtime_error("Unsupported node length index version: " +
                                 std::to_string(header->version));
    }
    if (header->value_width != kNodeLengthValueWidth) {
        close_mapping();
        throw std::runtime_error("Unsupported node length index value width: " +
                                 std::to_string(header->value_width));
    }
    const std::uint64_t expected_size = sizeof(NodeLengthIndexHeaderDisk) +
        header->node_count * sizeof(std::uint32_t);
    if (expected_size != file_size_) {
        close_mapping();
        throw std::runtime_error("Node length index file size is invalid: " + path);
    }

    node_count_ = header->node_count;
    lengths_ = reinterpret_cast<const std::uint32_t*>(
        static_cast<const char*>(mapping_) + sizeof(NodeLengthIndexHeaderDisk));
}

NodeLengthIndexReader::~NodeLengthIndexReader() {
    close_mapping();
}

std::uint32_t NodeLengthIndexReader::length(std::uint32_t rank) const {
    if (rank >= node_count_) {
        throw std::runtime_error("Node length rank out of range");
    }
    return lengths_[rank];
}

}  // namespace gfaidx::indexer

//
// Created by Fawaz Dabbaghie on 09/12/2025.
//

#include <iostream>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <stdexcept>
#include <string_view>
#include <vector>
#include <utility>

class GFAReader {
public:
    explicit GFAReader(const std::string& path) {
        fd_ = ::open(path.c_str(), O_RDONLY);
        if (fd_ == -1)
            throw std::runtime_error("Failed to open GFA file");

        struct stat st{};
        if (fstat(fd_, &st) == -1)
            throw std::runtime_error("Failed to stat GFA file");

        file_size_ = st.st_size;
        if (file_size_ == 0)
            throw std::runtime_error("GFA file is empty");

        data_ = static_cast<const char*>(
            mmap(nullptr, file_size_, PROT_READ, MAP_SHARED, fd_, 0)
        );
        if (data_ == MAP_FAILED)
            throw std::runtime_error("mmap failed");
    }

    ~GFAReader() {
        if (data_) munmap(const_cast<char*>(data_), file_size_);
        if (fd_ != -1) close(fd_);
    }

    // Process all L lines and call the callback for each one
    void process_L_lines(std::function<void(std::string_view, std::string_view)> callback) const {
        size_t pos = 0;
        while (pos < file_size_) {
            // Find the start of the next line
            size_t line_start = pos;
            size_t line_end = find_line_end(pos);

            // Extract the line as a string_view (no copying)
            std::string_view line(data_ + line_start, line_end - line_start);

            // Check if line starts with 'L'
            if (!line.empty() && line[0] == 'L' && (line.size() < 2 || line[1] == '\t')) {
                // Parse the L line
                auto [node1, node2] = parse_L_line(line);
                if (!node1.empty() && !node2.empty()) {
                    callback(node1, node2);
                }
            }
            pos = line_end + 1; // Move past the newline
        }
    }

private:
    int fd_ = -1;
    const char* data_ = nullptr;
    size_t file_size_ = 0;

    // Find the end of the current line (position of \n or end of file)
    size_t find_line_end(size_t start) const {
        size_t pos = start;
        while (pos < file_size_ && data_[pos] != '\n') {
            pos++;
        }
        return pos;
    }

    // Parse an L line and extract the two node identifiers
    // L line format: L\t<node1>\t<node2>\t...
    std::pair<std::string_view, std::string_view> parse_L_line(std::string_view line) const {
        // Skip the 'L' and the first tab
        size_t pos = 0;
        if (pos < line.size() && line[pos] == 'L') pos++;
        if (pos < line.size() && line[pos] == '\t') pos++;

        // Extract first node (until the next tab)
        size_t node1_start = pos;
        while (pos < line.size() && line[pos] != '\t') {
            pos++;
        }
        std::string_view node1 = line.substr(node1_start, pos - node1_start);

        // Skip the tab
        if (pos < line.size() && line[pos] == '\t') pos++;

        // Extract second node (until the next tab or end of line)
        size_t node2_start = pos;
        while (pos < line.size() && line[pos] != '\t') {
            pos++;
        }
        std::string_view node2 = line.substr(node2_start, pos - node2_start);

        return {node1, node2};
    }
};

// Example usage
int main(int argc, char** argv) {
    if (argc < 3) {
        std::cerr << "usage: " << argv[0] << " <input_gfa> <output_edge_list>\n"; return 1;
    }
    try {
        std::string input_gfa = argv[1];
        GFAReader reader(input_gfa);

        // Process L lines
        // I need to integrate this later in the code
        // and probably start adding to it an S line parser and a P line parser
        // so one class that parses all of GFAs lines
        // I can use this to generate the edge lists that louvain method consumes
        // but for now, I'll keep this stored and work with normal fstream and read line
        std::vector<std::pair<std::string, std::string>> edges;
        reader.process_L_lines([&edges](std::string_view node1, std::string_view node2) {
            // Store edges (convert to string if needed for storage)
            edges.emplace_back(std::string(node1), std::string(node2));
            std::cout << node1 << " " << node2 << std::endl;
            // Or just print them
            // std::cout << "Edge: " << node1 << " -> " << node2 << "\n";
        });

        // std::cout << "Total edges: " << edges.size() << "\n";

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }

    return 0;
}
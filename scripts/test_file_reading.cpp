#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <iostream>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <stdexcept>


// Count lines using mmap
// much much faster than the other implementations
void count_lines_mmap(const std::string& path) {
    int f_descriptor = ::open(path.c_str(), O_RDONLY);
    if (f_descriptor == -1)
        throw std::runtime_error("Failed to open file");

    struct stat st{};
    if (fstat(f_descriptor, &st) == -1)
        throw std::runtime_error("Failed to stat file");

    size_t file_size = st.st_size;
    const char* data = static_cast<const char*>(
        mmap(nullptr, file_size, PROT_READ, MAP_SHARED, f_descriptor, 0)
    );
    if (data == MAP_FAILED)
        throw std::runtime_error("mmap failed");

    size_t line_count = 0;
    for (size_t i = 0; i < file_size; i++) {
        if (data[i] == '\n') {
            line_count++;
        }
    }

    munmap(const_cast<char*>(data), file_size);
    close(f_descriptor);

    std::cout << "Number of lines: " << line_count << std::endl;
}


// this one was from somewhere on google, it runs in the same speed as the other one
// but this also could by because the lines in the text file are short, but parsing very long line
// can be a problem
void parseFileEfficiently(const std::string& filename, size_t bufferSize = 65536) {
    std::ifstream inputFile(filename);
    if (!inputFile.is_open()) {
        std::cerr << "Error opening file: " << filename << std::endl;
        return;
    }

    std::vector<char> buffer(bufferSize);
    std::string line;
    std::string remainingData; // To handle partial lines at chunk boundaries
    long int n_lines = 0;
    while (inputFile.read(buffer.data(), bufferSize)) {
        std::string chunk(buffer.data(), bufferSize);
        std::istringstream iss(remainingData + chunk); // Prepend remaining data
        remainingData.clear();

        while (std::getline(iss, line)) {
            if (iss.eof()) { // Check if the last read was a partial line
                remainingData = line;
            } else {
                n_lines++;
            }
        }
    }

    // Process any final remaining data
    if (!remainingData.empty()) {
        // std::cout << "Parsed line: " << remainingData << std::endl;
        n_lines++;
    }
    std::cout << "Number of lines: " << n_lines << std::endl;
    inputFile.close();
}


void normal_reading(const char *filename) {
    std::ifstream in(filename);
    std::string line;
    long int n_lines = 0;
    while (std::getline(in, line)) {
        n_lines++;
    }
    std::cout << "Number of lines: " << n_lines << std::endl;
}


int main(int argc, char** argv) {
    // Create a dummy file for testing
    // std::ofstream(argv[1]) << "Line 1\nLine 2\nLine 3\nLine 4";
    if (argc < 2) { std::cerr << "usage: " << argv[0] << " <text_file> <method> (0 for normal 1 for chunking and 2 for mmap)\n"; return 1; }
    const std::string type = argv[2];
    if (type == "0") normal_reading(argv[1]);
    if (type == "1") parseFileEfficiently(argv[1]);
    if (type == "2") count_lines_mmap(argv[1]);

    return 0;
}
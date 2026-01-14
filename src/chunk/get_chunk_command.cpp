#include "chunk/get_chunk_command.h"

#include <cstdint>
#include <iostream>
#include <string>

#include "fs/fs_helpers.h"
#include "chunk/split_gfa_to_comms.h"

namespace gfaidx::chunk {

void configure_get_chunk_parser(argparse::ArgumentParser& parser) {
    parser.add_argument("in_gz")
      .help("input indexed GFA gzip file");

    parser.add_argument("community_id")
      .help("community id to stream");

    parser.add_argument("--index")
      .default_value(std::string(""))
      .help("path to .idx file (defaults to <input>.idx)");
}

int run_get_chunk(const argparse::ArgumentParser& program) {
    const auto input_gz = program.get<std::string>("in_gz");
    if (!file_exists(input_gz.c_str())) {
        std::cerr << "Input file does not exist: " << input_gz << std::endl;
        return 1;
    }

    std::string index_path = program.get<std::string>("index");
    if (index_path.empty()) {
        index_path = input_gz + ".idx";
    }

    if (!file_exists(index_path.c_str())) {
        std::cerr << "Index file does not exist: " << index_path << std::endl;
        return 1;
    }

    std::uint32_t community_id = 0;
    try {
        community_id = static_cast<std::uint32_t>(
            std::stoul(program.get<std::string>("community_id"))
        );
    } catch (const std::exception& err) {
        std::cerr << "Invalid community id: " << err.what() << std::endl;
        return 1;
    }

    try {
        stream_community_lines(index_path, input_gz, community_id,
        [](const std::string& line) -> bool {
            std::cout << line << "\n";
            return true;
        });
    } catch (const std::exception& err) {
        std::cerr << err.what() << std::endl;
        return 1;
    }

    return 0;
}

}  // namespace gfaidx::chunk

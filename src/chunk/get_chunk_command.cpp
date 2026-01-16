#include "chunk/get_chunk_command.h"

#include <cstdint>
#include <iostream>
#include <string>

#include "chunk/split_gfa_to_comms.h"
#include "fs/fs_helpers.h"
#include "indexer/node_hash_index.h"

namespace gfaidx::chunk {

void configure_get_chunk_parser(argparse::ArgumentParser& parser) {
    parser.add_argument("in_gz")
      .help("input indexed GFA gzip file");

    // .idx with gzip member offsets/sizes (defaults to <input>.idx).
    parser.add_argument("--index")
      .default_value(std::string(""))
      .nargs(1)
      .help("path to .idx file (defaults to <input>.idx)");

    // .ndx with node-hash->community mapping (defaults to <input>.ndx).
    parser.add_argument("--node_index")
      .default_value(std::string(""))
      .nargs(1)
      .help("path to .ndx file (defaults to <input>.ndx)");

    // Optional community id for direct lookup.
    parser.add_argument("--community_id")
      .default_value(std::string(""))
      .nargs(1)
      .help("community id to stream");

    // Optional node id; overrides community_id when provided.
    parser.add_argument("--node_id")
      .default_value(std::string(""))
      .nargs(1)
      .help("node id to resolve into a community id");
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

    const std::string node_id = program.get<std::string>("node_id");
    const std::string community_id_str = program.get<std::string>("community_id");

    std::uint32_t community_id = 0;
    if (!node_id.empty()) {
        // Resolve node id to community id using the .ndx file.
        std::string node_index_path = program.get<std::string>("node_index");
        if (node_index_path.empty()) {
            node_index_path = input_gz + ".ndx";
        }
        if (!file_exists(node_index_path.c_str())) {
            std::cerr << "Node index file does not exist: " << node_index_path << std::endl;
            return 1;
        }
        try {
            indexer::NodeHashIndex node_index(node_index_path);
            if (!node_index.lookup(node_id, community_id)) {
                std::cerr << "Node ID " << node_id << " does not exist in index " << node_index_path << std::endl;
                // std::cerr << "Node id not found in index: " << node_id << std::endl;
                return 1;
            }
        } catch (const std::exception& err) {
            std::cerr << err.what() << std::endl;
            return 1;
        }
    } else if (!community_id_str.empty()) {
        // Fall back to a direct community id when provided.
        try {
            community_id = static_cast<std::uint32_t>(std::stoul(community_id_str));
        } catch (const std::exception& err) {
            std::cerr << "Invalid community id: " << err.what() << std::endl;
            return 1;
        }
    } else {
        std::cerr << "Either --community_id or --node_id must be provided" << std::endl;
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

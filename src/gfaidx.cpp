#include <iostream>

#include <argparse/argparse.hpp>

#include "chunk/get_chunk_command.h"
#include "indexer/index_gfa_helpers.h"
#include "indexer/index_gfa_main.h"

// todo: 1- need to make sure that if I run index_gfa with same input, it checks that these file exist already and stops
//       2- parallelize the compressing and maybe the splitting
//       3- ask codex to generate more test for both indexing and retrieval to make sure everything is working
//       4- compress the community index or make into a binary file, which then can give us constant access as community
//          IDs are ordered
//       5- build the node_id->community_id binary index and add that to get_chunk
int main(int argc, char** argv) {
    argparse::ArgumentParser program("gfaidx", "0.2.0");

    argparse::ArgumentParser index_gfa("index_gfa");
    index_gfa.add_description("Index and split a GFA file into communities");
    gfaidx::indexer::configure_index_gfa_parser(index_gfa);
    program.add_subparser(index_gfa);

    argparse::ArgumentParser get_chunk("get_chunk");
    get_chunk.add_description("Stream a community chunk from an indexed GFA");
    gfaidx::chunk::configure_get_chunk_parser(get_chunk);
    program.add_subparser(get_chunk);

    if (argc == 2 && std::string(argv[1]) == "index_gfa") {
        std::cerr << index_gfa;
        return 1;
    }

    if (argc == 2 && std::string(argv[1]) == "get_chunk") {
        std::cerr << get_chunk;
        return 1;
    }

    try {
        program.parse_args(argc, argv);
    } catch (const std::exception& err) {
        std::cerr << err.what() << std::endl;
        std::cerr << program;
        return 1;
    }

    if (program.is_subcommand_used("index_gfa")) {
        return gfaidx::indexer::run_index_gfa(index_gfa);
    }

    if (program.is_subcommand_used("get_chunk")) {
        return gfaidx::chunk::run_get_chunk(get_chunk);
    }

    std::cerr << program;
    return 1;
}

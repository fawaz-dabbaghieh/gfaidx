#include <iostream>

#include <argparse/argparse.hpp>

#include "chunk/get_chunk_command.h"
#include "indexer/index_gfa_helpers.h"
#include "indexer/index_gfa_main.h"
#include "paths/get_path_command.h"
#include "paths/index_paths_command.h"


int main(int argc, char** argv) {

    constexpr const char* version = "0.5.0";
    std::cerr << "gfaidx version " << version << std::endl;

    argparse::ArgumentParser program("gfaidx", version);
    
    argparse::ArgumentParser index_gfa("index_gfa", version);
    index_gfa.add_description("Index and split a GFA file into communities");
    gfaidx::indexer::configure_index_gfa_parser(index_gfa);
    program.add_subparser(index_gfa);

    argparse::ArgumentParser get_chunk("get_chunk", version);
    get_chunk.add_description("Stream a community chunk from an indexed GFA");
    gfaidx::chunk::configure_get_chunk_parser(get_chunk);
    program.add_subparser(get_chunk);

    argparse::ArgumentParser index_paths("index_paths", version);
    index_paths.add_description("Index the P and W lines of a GFA file into a binary path index");
    gfaidx::paths::configure_index_paths_parser(index_paths);
    program.add_subparser(index_paths);

    argparse::ArgumentParser get_path("get_path", version);
    get_path.add_description("Print a full P/W record or node-restricted subpaths from a path index");
    gfaidx::paths::configure_get_path_parser(get_path);
    program.add_subparser(get_path);

    if (argc == 2 && std::string(argv[1]) == "index_gfa") {
        std::cerr << index_gfa;
        return 1;
    }

    if (argc == 2 && std::string(argv[1]) == "get_chunk") {
        std::cerr << get_chunk;
        return 1;
    }

    if (argc == 2 && std::string(argv[1]) == "index_paths") {
        std::cerr << index_paths;
        return 1;
    }

    if (argc == 2 && std::string(argv[1]) == "get_path") {
        std::cerr << get_path;
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

    if (program.is_subcommand_used("index_paths")) {
        return gfaidx::paths::run_index_paths(index_paths);
    }

    if (program.is_subcommand_used("get_path")) {
        return gfaidx::paths::run_get_path(get_path);
    }

    std::cerr << program;
    return 1;
}

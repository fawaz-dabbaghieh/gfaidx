#include <iostream>

#include <argparse/argparse.hpp>

#include "chunk/get_chunk_command.h"
#include "chunk/get_subgraph_command.h"
#include "coordinates/coordinate_commands.h"
#include "indexer/index_gfa_helpers.h"
#include "indexer/index_gfa_main.h"
#include "paths/get_path_command.h"
#include "paths/index_path_checkpoints_command.h"
#include "paths/index_paths_command.h"


int main(int argc, char** argv) {

    constexpr const char* version = "1.7.0";
    std::cerr << "gfaidx version " << version << std::endl;

    argparse::ArgumentParser program("gfaidx", version);
    
    argparse::ArgumentParser index_gfa("index_gfa", version);
    index_gfa.add_description("Index and split a GFA file into communities and, by default, build a path index");
    gfaidx::indexer::configure_index_gfa_parser(index_gfa);
    program.add_subparser(index_gfa);

    argparse::ArgumentParser get_chunk("get_chunk", version);
    get_chunk.add_description("Stream a community chunk from an indexed GFA");
    gfaidx::chunk::configure_get_chunk_parser(get_chunk);
    program.add_subparser(get_chunk);

    argparse::ArgumentParser get_subgraph("get_subgraph", version);
    get_subgraph.add_description("Extract a BFS neighborhood subgraph from an indexed GFA");
    gfaidx::chunk::configure_get_subgraph_parser(get_subgraph);
    program.add_subparser(get_subgraph);

    argparse::ArgumentParser index_paths("index_paths", version);
    index_paths.add_description("Index the P and W lines of a GFA file into a binary path index");
    gfaidx::paths::configure_index_paths_parser(index_paths);
    program.add_subparser(index_paths);

    argparse::ArgumentParser index_path_checkpoints(
        "index_path_checkpoints",
        version);
    index_path_checkpoints.add_description(
        "Build path-coordinate checkpoints from existing .pdx and .lnx indexes");
    gfaidx::paths::configure_index_path_checkpoints_parser(
        index_path_checkpoints);
    program.add_subparser(index_path_checkpoints);

    argparse::ArgumentParser get_path("get_path", version);
    get_path.add_description("Print a full P/W record or node-restricted subpaths from an indexed graph's path index");
    gfaidx::paths::configure_get_path_parser(get_path);
    program.add_subparser(get_path);

    argparse::ArgumentParser index_coordinates("index_coordinates", version);
    index_coordinates.add_description("Build a coordinate sidecar from selected P/W records, RS W records, or SR:i:0 segments");
    gfaidx::coordinates::configure_index_coordinates_parser(index_coordinates);
    program.add_subparser(index_coordinates);

    argparse::ArgumentParser get_region("get_region", version);
    get_region.add_description("Extract a graph neighborhood from a reference-coordinate interval");
    gfaidx::coordinates::configure_get_region_parser(get_region);
    program.add_subparser(get_region);

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

    if (argc == 2 &&
        std::string(argv[1]) == "index_path_checkpoints") {
        std::cerr << index_path_checkpoints;
        return 1;
    }

    if (argc == 2 && std::string(argv[1]) == "get_subgraph") {
        std::cerr << get_subgraph;
        return 1;
    }

    if (argc == 2 && std::string(argv[1]) == "get_path") {
        std::cerr << get_path;
        return 1;
    }

    if (argc == 2 && std::string(argv[1]) == "index_coordinates") {
        std::cerr << index_coordinates;
        return 1;
    }

    if (argc == 2 && std::string(argv[1]) == "get_region") {
        std::cerr << get_region;
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

    if (program.is_subcommand_used("index_path_checkpoints")) {
        return gfaidx::paths::run_index_path_checkpoints(
            index_path_checkpoints);
    }

    if (program.is_subcommand_used("get_subgraph")) {
        return gfaidx::chunk::run_get_subgraph(get_subgraph);
    }

    if (program.is_subcommand_used("get_path")) {
        return gfaidx::paths::run_get_path(get_path);
    }

    if (program.is_subcommand_used("index_coordinates")) {
        return gfaidx::coordinates::run_index_coordinates(index_coordinates);
    }

    if (program.is_subcommand_used("get_region")) {
        return gfaidx::coordinates::run_get_region(get_region);
    }

    std::cerr << program;
    return 1;
}

#include "paths/index_path_checkpoints_command.h"

#include <cstdint>
#include <iostream>
#include <stdexcept>
#include <string>

#include "fs/fs_helpers.h"
#include "paths/path_coordinate_checkpoints.h"
#include "utils/Timer.h"
#include "utils/cli_helpers.h"

namespace gfaidx::paths {

void configure_index_path_checkpoints_parser(
    argparse::ArgumentParser& parser) {
    parser.add_argument("in_gfa")
      .help("indexed GFA graph used to infer companion .pdx and .lnx files");

    parser.add_argument("out_index")
      .default_value(std::string(""))
      .nargs(argparse::nargs_pattern::optional)
      .help("output checkpoint index; defaults to <in_gfa>.pcx");

    parser.add_argument("--pdx")
      .default_value(std::string(""))
      .nargs(1)
      .help("path index override; defaults to <in_gfa>.pdx");

    parser.add_argument("--lnx")
      .default_value(std::string(""))
      .nargs(1)
      .help("node length index override; defaults to <in_gfa>.lnx");

    parser.add_argument("--checkpoint_steps")
      .default_value(
          std::to_string(kDefaultPathCheckpointStride))
      .nargs(1)
      .help("store one cumulative coordinate every N path steps; defaults to 4096");

    parser.add_argument("--progress_every_paths")
      .default_value(
          std::to_string(kDefaultPathCheckpointProgressEvery))
      .nargs(1)
      .help("report progress every N completed paths; 0 disables periodic progress");
}

int run_index_path_checkpoints(
    const argparse::ArgumentParser& program) {
    try {
        const auto input_gfa = program.get<std::string>("in_gfa");
        auto output_index = program.get<std::string>("out_index");
        auto path_index = program.get<std::string>("pdx");
        auto length_index = program.get<std::string>("lnx");

        // Reuse the common sidecar naming convention so renamed indexes need
        // overrides only when they no longer sit beside the indexed graph.
        if (output_index.empty()) {
            output_index = utils::companion_path(input_gfa, ".pcx");
        }
        if (path_index.empty()) {
            path_index = utils::companion_path(input_gfa, ".pdx");
        }
        if (length_index.empty()) {
            length_index = utils::companion_path(input_gfa, ".lnx");
        }

        if (!file_exists(path_index.c_str())) {
            throw std::runtime_error(
                "Path index does not exist: " + path_index);
        }
        if (!file_exists(length_index.c_str())) {
            throw std::runtime_error(
                "Node length index does not exist: " + length_index);
        }

        const auto stride = utils::parse_u64_strict(
            program.get<std::string>("checkpoint_steps"),
            "--checkpoint_steps");
        if (stride == 0) {
            throw std::runtime_error(
                "--checkpoint_steps must be greater than zero");
        }
        const auto progress_every_paths = utils::parse_u64_strict(
            program.get<std::string>("progress_every_paths"),
            "--progress_every_paths");

        Timer timer;
        std::cout << "Building path coordinate checkpoints "
                  << output_index << " from " << path_index
                  << " and " << length_index << std::endl;
        build_path_coordinate_checkpoint_index(path_index,
                                               length_index,
                                               output_index,
                                               stride,
                                               progress_every_paths);

        // Reopen the completed sidecar to validate its header and report the
        // number of checkpoint values actually published.
        PathCoordinateCheckpointIndexReader checkpoint_index(
            output_index);
        std::cout << "Indexed " << checkpoint_index.path_count()
                  << " paths with "
                  << checkpoint_index.checkpoint_count()
                  << " coordinate checkpoints at a "
                  << checkpoint_index.checkpoint_stride()
                  << "-step stride in " << timer.elapsed()
                  << " seconds" << std::endl;
        return 0;
    } catch (const std::exception& err) {
        std::cerr << err.what() << std::endl;
        return 1;
    }
}

}  // namespace gfaidx::paths

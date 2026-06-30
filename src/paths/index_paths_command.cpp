#include "paths/index_paths_command.h"

#include <iostream>
#include <stdexcept>
#include <string>

#include "fs/fs_helpers.h"
#include "paths/path_index.h"

namespace gfaidx::paths {

void configure_index_paths_parser(argparse::ArgumentParser& parser) {
    parser.add_argument("in_gfa")
      .help("input GFA graph");

    parser.add_argument("out_index")
      .help("output path index (.pdx)");

    parser.add_argument("--ndx")
      .default_value(std::string(""))
      .nargs(1)
      .help("path to the node hash index (.ndx) built by index_gfa; defaults to <in_gfa>.ndx when present");

    parser.add_argument("--tmp_dir").default_value(std::string(""))
      .nargs(1)
      .help("temporary directory base for external posting sort files (default: output directory)");

    parser.add_argument("--progress_every").default_value(std::string("1000000"))
      .nargs(1)
      .help("print progress every N lines while reading (default: 1000000), give 0 to disable");
}

int run_index_paths(const argparse::ArgumentParser& program) {
    const auto input_gfa = program.get<std::string>("in_gfa");
    const auto out_index = program.get<std::string>("out_index");
    auto node_index = program.get<std::string>("ndx");
    const auto tmp_dir = program.get<std::string>("tmp_dir");

    if (!file_exists(input_gfa.c_str())) {
        std::cerr << "Input file does not exist: " << input_gfa << std::endl;
        return 1;
    }
    if (node_index.empty()) {
        // Keep index_paths consistent with query commands: the normal companion
        // path for an indexed graph is simply the graph path plus ".ndx".
        const auto inferred = input_gfa + ".ndx";
        if (file_exists(inferred.c_str())) node_index = inferred;
    }
    if (node_index.empty()) {
        std::cerr << "Could not find companion node index <in_gfa>.ndx; provide --ndx to build a path index aligned to the graph's node hash index" << std::endl;
        return 1;
    }
    if (!file_exists(node_index.c_str())) {
        std::cerr << "Node index file does not exist: " << node_index << std::endl;
        return 1;
    }

    if (file_exists(out_index.c_str())) {
        std::cerr << "Output path index already exists: " << out_index << std::endl;
        return 1;
    }

    Reader::Options reader_options;
    const auto progress_str = program.get<std::string>("progress_every");
    try {
        long long parsed = std::stoll(progress_str);
        if (parsed < 0) {
            throw std::invalid_argument("progress must be >= 0");
        }
        reader_options.progress_every = static_cast<std::uint64_t>(parsed);
    } catch (const std::exception& err) {
        std::cerr << "Warning: invalid --progress_every value '" << progress_str
                  << "', using default 1000000 (" << err.what() << ")" << std::endl;
        reader_options.progress_every = 1000000;
    }

    try {
        build_path_index(input_gfa, out_index, node_index, reader_options, tmp_dir, false);
    } catch (const std::exception& err) {
        std::cerr << err.what() << std::endl;
        return 1;
    }

    return 0;
}

}  // namespace gfaidx::paths

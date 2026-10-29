#include "paths/index_paths_command.h"

#include <iostream>
#include <stdexcept>
#include <string>

#include "fs/fs_helpers.h"
#include "paths/path_index.h"
#include "utils/cli_helpers.h"

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

    parser.add_argument("--step_encoding").default_value(std::string("delta"))
      .nargs(1)
      .choices("delta", "packed32")
      .help("step table: delta (zigzag varint blocks) or packed32 (4 bytes per step); packed32 with a legacy .ndx writes a v4 .pdx");

    parser.add_argument("--posting_encoding").default_value(std::string("slots"))
      .nargs(1)
      .choices("slots", "groups")
      .help("postings: slots (predicted from neighbouring node ids) or groups (v4 per-path groups)");

    parser.add_argument("--posting_block_nodes")
      .default_value(std::to_string(kDefaultPostingBlockNodes))
      .nargs(1)
      .help("node ids per independently decodable posting block for --posting_encoding slots");
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
        const auto inferred = utils::companion_path(input_gfa, ".ndx");
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
        reader_options.progress_every = utils::parse_u64_strict(progress_str, "--progress_every");
    } catch (const std::exception& err) {
        std::cerr << "Warning: invalid --progress_every value '" << progress_str
                  << "', using default 1000000 (" << err.what() << ")" << std::endl;
        reader_options.progress_every = 1000000;
    }

    try {
        PathIndexEncoding encoding;
        if (program.get<std::string>("step_encoding") == "packed32") {
            encoding.steps = StepEncoding::packed32;
        }
        if (program.get<std::string>("posting_encoding") == "groups") {
            encoding.postings = PostingEncoding::path_groups;
        }
        encoding.posting_block_nodes = utils::parse_u32_strict(
            program.get<std::string>("posting_block_nodes"), "--posting_block_nodes", 1, 1u << 16);
        build_path_index(input_gfa, out_index, node_index, reader_options, tmp_dir, false, encoding);
    } catch (const std::exception& err) {
        std::cerr << err.what() << std::endl;
        return 1;
    }

    return 0;
}

}  // namespace gfaidx::paths

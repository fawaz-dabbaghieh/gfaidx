#ifndef GFAIDX_INDEX_PATH_CHECKPOINTS_COMMAND_H
#define GFAIDX_INDEX_PATH_CHECKPOINTS_COMMAND_H

#include <argparse/argparse.hpp>

namespace gfaidx::paths {

// Configure and run standalone .pcx construction for existing .pdx/.lnx files.
void configure_index_path_checkpoints_parser(
    argparse::ArgumentParser& parser);
int run_index_path_checkpoints(
    const argparse::ArgumentParser& program);

}  // namespace gfaidx::paths

#endif  // GFAIDX_INDEX_PATH_CHECKPOINTS_COMMAND_H

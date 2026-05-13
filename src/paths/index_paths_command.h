#ifndef GFAIDX_INDEX_PATHS_COMMAND_H
#define GFAIDX_INDEX_PATHS_COMMAND_H

#include <argparse/argparse.hpp>

namespace gfaidx::paths {

void configure_index_paths_parser(argparse::ArgumentParser& parser);
int run_index_paths(const argparse::ArgumentParser& program);

}  // namespace gfaidx::paths

#endif  // GFAIDX_INDEX_PATHS_COMMAND_H

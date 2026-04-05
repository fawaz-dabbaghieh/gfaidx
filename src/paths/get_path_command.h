#ifndef GFAIDX_GET_PATH_COMMAND_H
#define GFAIDX_GET_PATH_COMMAND_H

#include <argparse/argparse.hpp>

namespace gfaidx::paths {

void configure_get_path_parser(argparse::ArgumentParser& parser);
int run_get_path(const argparse::ArgumentParser& program);

}  // namespace gfaidx::paths

#endif  // GFAIDX_GET_PATH_COMMAND_H

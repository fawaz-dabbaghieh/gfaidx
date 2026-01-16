#ifndef GFAIDX_GET_CHUNK_COMMAND_H
#define GFAIDX_GET_CHUNK_COMMAND_H

#include <argparse/argparse.hpp>

namespace gfaidx::chunk {

void configure_get_chunk_parser(argparse::ArgumentParser& parser);
int run_get_chunk(const argparse::ArgumentParser& program);

}  // namespace gfaidx::chunk

#endif  // GFAIDX_GET_CHUNK_COMMAND_H

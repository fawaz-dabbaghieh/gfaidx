#ifndef GFAIDX_COORDINATE_COMMANDS_H
#define GFAIDX_COORDINATE_COMMANDS_H

#include <argparse/argparse.hpp>

namespace gfaidx::coordinates {

// Configure and execute standalone coordinate-index construction.
void configure_index_coordinates_parser(argparse::ArgumentParser& parser);
int run_index_coordinates(const argparse::ArgumentParser& program);

// Configure and execute coordinate-seeded graph extraction.
void configure_get_region_parser(argparse::ArgumentParser& parser);
int run_get_region(const argparse::ArgumentParser& program);

}  // namespace gfaidx::coordinates

#endif  // GFAIDX_COORDINATE_COMMANDS_H

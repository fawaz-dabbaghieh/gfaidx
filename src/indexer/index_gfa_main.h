#ifndef GFAIDX_INDEX_GFA_MAIN_H
#define GFAIDX_INDEX_GFA_MAIN_H

#include <argparse/argparse.hpp>

namespace gfaidx::indexer {

int run_index_gfa(const argparse::ArgumentParser& program);

}  // namespace gfaidx::indexer

#endif  // GFAIDX_INDEX_GFA_MAIN_H

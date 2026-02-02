#ifndef GFAIDX_INDEX_GFA_HELPERS_H
#define GFAIDX_INDEX_GFA_HELPERS_H

#include <string>
#include <unordered_map>

#include <argparse/argparse.hpp>
#include <graph_binary.h>

#include "fs/Reader.h"

namespace gfaidx::indexer {

extern int nb_pass;
extern double precision;
extern int display_level;
extern int k1;
extern unsigned int N_NODES;
extern unsigned int N_EDGES;

void configure_index_gfa_parser(argparse::ArgumentParser& parser);

bool run_sort(const std::string& input_edges,
              const std::string& output_edges,
              const std::string& tmpdir = ".",
              const std::string& mem = "50%",
              bool unique = true,
              int threads = 1);

void generate_edgelist(const std::string& input_gfa,
                       const std::string& tmp_edgelist,
                       std::unordered_map<std::string, unsigned int>& node_id_map,
                       const Reader::Options& reader_options);

void generate_communities(const std::string &binary_graph,
                          BGraph &g);

void add_singleton_community(const std::string& input_gfa,
                             std::unordered_map<std::string, unsigned int>& node_id_map,
                             BGraph& g,
                             const Reader::Options& reader_options);

void output_communities(const BGraph& g,
                        const std::string& out_file,
                        const std::unordered_map<std::string, unsigned int>& node_id_map);

}  // namespace gfaidx::indexer

#endif  // GFAIDX_INDEX_GFA_HELPERS_H

#include "indexer/index_gfa_main.h"

#include <cstdint>
#include <fstream>
#include <iostream>

#include <argparse/argparse.hpp>
#include <graph.h>

#include "fs/fs_helpers.h"
#include "indexer/index_gfa_helpers.h"
#include "chunk/split_gfa_to_comms.h"
#include "utils/Timer.h"

#define WEIGHTED     0
#define UNWEIGHTED   1

namespace gfaidx::indexer {

int run_index_gfa(const argparse::ArgumentParser& program) {
    /*
     * parse arguments and check inputs/outputs
     */
    Timer total_time;

    auto input_gfa = program.get<std::string>("in_gfa");
    if (!file_exists(input_gfa.c_str())) {
        std::cerr << "Input file does not exist: " << input_gfa << std::endl;
        return 1;
    }

    auto out_gzip = program.get<std::string>("out_gz");
    if (file_exists(out_gzip.c_str())) {
        std::cerr << "Output file already exists: " << out_gzip << std::endl;
        return 1;
    }
    if (!file_writable(out_gzip.c_str())) {
        std::cerr << "Output file is not writable: " << out_gzip << std::endl;
        return 1;
    }

    bool keep_tmp = program.get<bool>("keep_tmp");

    std::uint64_t progress_every;
    const auto progress_str = program.get<std::string>("progress_every");
    try {
        long long parsed = std::stoll(progress_str);
        if (parsed <= 0) {
            throw std::invalid_argument("progress must be positive");
        }

        progress_every = static_cast<std::uint64_t>(parsed);
    } catch (const std::exception& err) {
        std::cerr << "Warning: invalid --progress_every value '" << progress_str
                  << "', using default 1000000 (" << err.what() << ")" << std::endl;
        progress_every = 1000000;
    }

    int gzip_level = 6;
    const auto gzip_level_str = program.get<std::string>("gzip_level");
    try {
        long long parsed = std::stoll(gzip_level_str);
        if (parsed < 1 || parsed > 9) {
            throw std::invalid_argument("gzip level must be 1-9");
        }
        gzip_level = static_cast<int>(parsed);
    } catch (const std::exception& err) {
        std::cerr << "Warning: invalid --gzip_level value '" << gzip_level_str
                  << "', using default 6 (" << err.what() << ")" << std::endl;
        gzip_level = 6;
    }

    int gzip_mem_level = 8;
    const auto gzip_mem_level_str = program.get<std::string>("gzip_mem_level");
    try {
        long long parsed = std::stoll(gzip_mem_level_str);
        if (parsed < 1 || parsed > 9) {
            throw std::invalid_argument("gzip mem level must be 1-9");
        }
        gzip_mem_level = static_cast<int>(parsed);
    } catch (const std::exception& err) {
        std::cerr << "Warning: invalid --gzip_mem_level value '" << gzip_mem_level_str
                  << "', using default 8 (" << err.what() << ")" << std::endl;
        gzip_mem_level = 8;
    }

    Reader::Options reader_options;
    reader_options.progress_every = progress_every;

    std::ifstream in(input_gfa);
    if (!in.good()) {
        std::cerr << "Could not open input file: " << input_gfa << std::endl;
        return 1;
    }

    Timer timer;
    /*
     * creating the edge list from the GFA file
     */
    // todo: I probably want to change this
    //     also change the hard-coded tmp locations to be in the specified tmp file
    std::string tmp_base = program.get<std::string>("tmp_dir");
    if (tmp_base.empty()) {
        std::filesystem::path input_path(input_gfa);
        auto parent = input_path.parent_path();
        tmp_base = parent.empty() ? std::string("") : parent.string();
    }

    auto tmp_dir = create_temp_dir(tmp_base,
                                   "gfaidx_tmp_",
                                   "latest",
                                   keep_tmp);
    std::cout << get_time() << ": Using temp directory " << tmp_dir << std::endl;

    std::string sep = "/";
    std::unordered_map<std::string, unsigned int> node_id_map;
    std::string tmp_edgelist = tmp_dir + sep + "tmp_edgelist.txt";
    // generates the edge list from the GFA with integer node IDs
    std::cout << get_time() << ": Generating the edges list" << std::endl;
    timer.reset();
    generate_edgelist(input_gfa, tmp_edgelist, node_id_map, reader_options);
    std::cout << get_time() << ": Finished generating the edges list in " << timer.elapsed() << " seconds" << std::endl;
    std::cout << get_time() << ": The GFA has " << N_NODES << " S lines, and " << N_EDGES << " L lines" << std::endl;

    /*
     * sorting the edge list with linux sort
     */
    std::string sorted_tmp_edgelist = tmp_dir + sep + "tmp_edgelist_sorted.txt";
    timer.reset();
    std::cout << get_time() << ": Sorting the edges" << std::endl;
    run_sort(tmp_edgelist, sorted_tmp_edgelist, tmp_dir);
    std::cout << get_time() << ": Finished sorting the edges in " << timer.elapsed() << " seconds" << std::endl;

    /*
     * loading the graph from the edge list with the louvain graph class
     */
    std::cout << get_time() << ": Loading the edge list from disk" << std::endl;
    timer.reset();
    Graph graph(sorted_tmp_edgelist.c_str(), UNWEIGHTED);
    std::cout << get_time() << ": Finished loading the edge list from disk in " << timer.elapsed() << " seconds" << std::endl;

    /*
     * converting the graph to binary format for faster processing
     */
    std::string tmp_binary = tmp_dir + sep + "tmp_binary.bin";
    std::cout <<  get_time() << ": Saving the graph as a compressed binary to disk to: " << tmp_binary << std::endl;
    timer.reset();
    graph.display_binary(tmp_binary.c_str(), nullptr, UNWEIGHTED);
    std::cout <<  get_time() << ": Finished saving the binary graph to disk in " << timer.elapsed() << " seconds" << std::endl;

    /*
     * performing community detection on the binary graph
     */
    timer.reset();
    std::cout << get_time() << ": Starting community detection" << std::endl;
    BGraph final_graph;
    generate_communities(tmp_binary, final_graph, display_level);
    std::cout << get_time() << ": Finished community detection in " << timer.elapsed() << " seconds" << std::endl;

    timer.reset();
    std::cout << get_time() << ": Scanning for singleton nodes" << std::endl;
    add_singleton_community(input_gfa, node_id_map, final_graph, reader_options);
    std::cout << get_time() << ": Finished scanning for singleton nodes in " << timer.elapsed() << " seconds" << std::endl;


    timer.reset();
    std::cout << get_time() << ": Starting splitting and gzipping" << std::endl;
    split_gzip_gfa(input_gfa,
                   out_gzip,
                   tmp_dir,
                   final_graph,
                   150,
                   node_id_map,
                   reader_options,
                   gzip_level,
                   gzip_mem_level);

    std::cout << get_time() << ": Finished splitting and gzipping" << std::endl;

    if (!keep_tmp) {
        std::cout << get_time() << ": Removing the temporary files" << std::endl;
        remove_file(tmp_edgelist.c_str());
        remove_file(sorted_tmp_edgelist.c_str());
        remove_file(tmp_binary.c_str());
        std::filesystem::remove_all(tmp_dir);
    }

    std::cout << get_time() << ": Finished in total time of " << total_time.elapsed() << " seconds" << std::endl;

    return 0;
}

}  // namespace gfaidx::indexer

#include "indexer/index_gfa_main.h"

#include <cstdint>
#include <fstream>
#include <iostream>

#include <argparse/argparse.hpp>

#include "fs/fs_helpers.h"
#include "indexer/direct_binary_writer.h"
#include "indexer/index_gfa_helpers.h"
#include "indexer/node_hash_index.h"
#include "chunk/split_gfa_to_comms.h"
#include "paths/path_index.h"
#include "utils/Memory.h"
#include "utils/Timer.h"

#define WEIGHTED     0
#define UNWEIGHTED   1

namespace gfaidx::indexer {
int run_index_gfa(const argparse::ArgumentParser& program) {
    Timer total_time;

    // validating inputs
    auto input_gfa = program.get<std::string>(in_gfa);
    if (!file_exists(input_gfa.c_str())) {
        std::cerr << "Input file does not exist: " << input_gfa << std::endl;
        return 1;
    }

    auto out_gzip = program.get<std::string>(out_graph);
    if (file_exists(out_gzip.c_str())) {
        std::cerr << "Output file already exists: " << out_gzip << std::endl;
        return 1;
    }

    // The gzip companion index must not already exist before we start work.
    const std::string chunk_index_path = out_gzip + ".idx";
    if (file_exists(chunk_index_path.c_str())) {
        std::cerr << "Chunk index file already exists: " << chunk_index_path << std::endl;
        return 1;
    }

    // Write node hash index alongside the gzip output.
    std::string node_index_path = out_gzip + ".ndx";
    if (file_exists(node_index_path.c_str())) {
        std::cerr << "Node index file already exists: " << node_index_path << std::endl;
        return 1;
    }

    // Path indexing depends on .ndx rank order, so index_gfa can now produce
    // a matching .pdx as part of the default indexing workflow.
    const bool no_paths = program.get<bool>("no_paths");
    const std::string path_index_path = out_gzip + ".pdx";
    if (!no_paths && file_exists(path_index_path.c_str())) {
        std::cerr << "Path index file already exists: " << path_index_path << std::endl;
        return 1;
    }


    bool keep_tmp = program.get<bool>("keep_tmp");

    // check progress_every user input
    std::uint64_t progress_every;
    const auto progress_str = program.get<std::string>("progress_every");
    try {
        long long parsed = std::stoll(progress_str);
        if (parsed <= 0) {
            throw std::invalid_argument("progress must be a positive integer");
        }
        progress_every = static_cast<std::uint64_t>(parsed);
    } catch (const std::exception& err) {
        std::cerr << "Warning: invalid --progress_every value '" << progress_str
                  << "', using default 1000000 (" << err.what() << ")" << std::endl;
        progress_every = 1000000;
    }

    Reader::Options reader_options;
    reader_options.progress_every = progress_every;

    // check gzip_level user input
    int gzip_level;
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

    // check gzip_mem_level user input
    int gzip_mem_level;
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

    std::ifstream in(input_gfa);
    if (!in.good()) {
        std::cerr << "Could not open input file: " << input_gfa << std::endl;
        return 1;
    }

    // Stage every final artifact on hidden sibling paths so validation and failures stay side-effect free.
    const std::string staged_out_gzip = make_temp_output_path(out_gzip);
    const std::string staged_chunk_index_path = staged_out_gzip + ".idx";
    const std::string staged_node_index_path = staged_out_gzip + ".ndx";
    const std::string staged_path_index_path = staged_out_gzip + ".pdx";
    auto cleanup_staged_outputs = [&]() {
        // Remove any staged final artifacts that were not successfully published.
        remove_path_if_exists(staged_out_gzip);
        remove_path_if_exists(staged_chunk_index_path);
        remove_path_if_exists(staged_node_index_path);
        remove_path_if_exists(staged_path_index_path);
    };

    Timer timer;

    /*
     * creating the edge list from the GFA file
     */
    auto tmp_base = program.get<std::string>("tmp_dir");
    if (tmp_base.empty()) {
        std::filesystem::path input_path(input_gfa);
        auto parent = input_path.parent_path();
        tmp_base = parent.empty() ? std::string("") : parent.string();
    }

    // symlink (latest) to the latest tmp, in case there is more than one, we know which one is the latest
    auto tmp_dir = create_temp_dir(tmp_base,
                                   "gfaidx_tmp_",
                                   "latest",
                                   true);
    std::cout << get_time() << ": Using temp directory " << tmp_dir << std::endl;
    log_memory("After temp directory setup");

    std::string sep = "/";
    // Keep the normal intermediate work products inside the run-specific temp directory.
    std::string tmp_edgelist = tmp_dir + sep + "tmp_edgelist.txt";
    std::string sorted_tmp_edgelist = tmp_dir + sep + "tmp_edgelist_sorted.txt";
    std::string tmp_binary = tmp_dir + sep + "tmp_binary.bin";
    auto cleanup_work_dir = [&]() {
        if (!keep_tmp) {
            // Remove the major intermediate files first so a partially removed temp dir is still small.
            remove_path_if_exists(tmp_edgelist);
            remove_path_if_exists(sorted_tmp_edgelist);
            remove_path_if_exists(tmp_binary);
            std::error_code ec;
            // Then remove the full working directory tree for this run.
            std::filesystem::remove_all(tmp_dir, ec);
            std::filesystem::path latest_path = std::filesystem::path(tmp_base.empty()
                ? std::filesystem::current_path()
                : std::filesystem::path(tmp_base)) / "latest";
            // Finally drop the convenience symlink if it still points at this run directory.
            if (std::filesystem::exists(latest_path) || std::filesystem::is_symlink(latest_path)) {
                std::filesystem::remove(latest_path, ec);
            }
        }
    };

    try {
        std::unordered_map<std::string, unsigned int> node_id_map;

        // Generate the edge list from the GFA using compact integer node ids.
        std::cout << get_time() << ": Generating the edges list" << std::endl;
        timer.reset();
        generate_edgelist(input_gfa, tmp_edgelist, node_id_map, reader_options);
        std::cout << get_time() << ": Finished generating the edges list in " << timer.elapsed() << " seconds" << std::endl;
        std::cout << get_time() << ": The GFA has " << N_NODES << " S lines, and " << N_EDGES << " L lines" << std::endl;
        log_map_stats("Node id map stats", node_id_map);
        log_memory("After edge list generation");

        /*
         * sorting the edge list with linux sort
         */
        timer.reset();
        std::cout << get_time() << ": Sorting the edges" << std::endl;
        if (!run_sort(tmp_edgelist, sorted_tmp_edgelist, tmp_dir)) {
            throw std::runtime_error("Failed to sort the edge list");
        }
        std::cout << get_time() << ": Finished sorting the edges in " << timer.elapsed() << " seconds" << std::endl;
        log_memory("After edge list sort");

        /*
         * converting the edge list to binary format for faster processing
         */
        std::cout <<  get_time() << ": Saving the graph as a compressed binary to disk to: " << tmp_binary << std::endl;
        timer.reset();
        // I made this functions instead of using the Louvain graph object from the earlier versions of gfaidx
        // I think that object was too big, and one can build the binary version from the edge list directly
        // but having to spend a bit more time due to passing through the list twice
        write_binary_graph_from_edgelist(sorted_tmp_edgelist, tmp_binary, N_NODES);
        std::cout <<  get_time() << ": Finished saving the binary graph to disk in " << timer.elapsed() << " seconds" << std::endl;
        log_memory("After binary graph write");

        /*
         * performing community detection on the binary graph
         */
        std::vector<std::uint32_t> id_to_comm;
        std::uint32_t ncom = 0;
        // scoping so objects that are not needed anymore get destructed, mainly final_graph
        {
            timer.reset();
            std::cout << get_time() << ": Starting community detection" << std::endl;

            BGraph final_graph; // binary graph
            generate_communities(tmp_binary, final_graph, display_level);
            std::cout << get_time() << ": Finished community detection in " << timer.elapsed() << " seconds" << std::endl;
            log_memory("After community detection");

            timer.reset();
            std::cout << get_time() << ": Scanning for singleton nodes" << std::endl;
            add_singleton_community(input_gfa, node_id_map, final_graph, reader_options);
            std::cout << get_time() << ": Finished scanning for singleton nodes in " << timer.elapsed() << " seconds" << std::endl;
            log_memory("After singleton scan");

            // Build node-id -> community-id mapping, then let the full graph die here.
            id_to_comm.resize(node_id_map.size());
            for (std::uint32_t c = 0; c < final_graph.nodes.size(); ++c) {
                for (const auto n : final_graph.nodes[c]) {
                    id_to_comm[n] = c;
                }
            }
            ncom = static_cast<std::uint32_t>(final_graph.nodes.size());
        }

        timer.reset();
        log_memory("After releasing final graph");
        std::cout << get_time() << ": Starting splitting and gzipping" << std::endl;
        // Write the chunked graph and its .idx into staged sibling paths rather than the final names.
        split_gzip_gfa(input_gfa, staged_out_gzip, tmp_dir, ncom, 150, node_id_map,
                       id_to_comm, reader_options, gzip_level, gzip_mem_level);

        std::cout << get_time() << ": Finished splitting and gzipping" << std::endl;
        log_memory("After split and gzip");

        timer.reset();
        std::cout << get_time() << ": Writing node hash index to " << node_index_path << std::endl;
        // Stage the node hash index too so a later failure cannot leave a partial .ndx behind.
        write_node_hash_index(node_id_map, id_to_comm, staged_node_index_path);
        std::cout << get_time() << ": Finished node hash index in " << timer.elapsed() << " seconds" << std::endl;
        log_memory("After node hash index");

        if (no_paths) {
            std::cout << get_time() << ": Skipping path indexing because --no_paths was provided" << std::endl;
        } else {
            timer.reset();
            // Build the path index last so it can reuse the finished .ndx. That
            // keeps .pdx node ids aligned to .ndx entry ranks and means a single
            // index_gfa run now produces the full graph + path query stack.
            std::cout << get_time() << ": Building path index " << path_index_path << std::endl;
            // Point the path builder at the staged .ndx so every staged artifact stays internally consistent.
            gfaidx::paths::build_path_index(input_gfa,
                                            staged_path_index_path,
                                            staged_node_index_path,
                                            reader_options,
                                            tmp_dir,
                                            keep_tmp);
            std::cout << get_time() << ": Finished path index in " << timer.elapsed() << " seconds" << std::endl;
            log_memory("After path index");
        }

        std::cout << get_time() << ": Publishing final output files" << std::endl;
        // Publish the companion indexes first so the final .gz only appears once its sidecars are ready too.
        rename_path_or_throw(staged_chunk_index_path, chunk_index_path);
        rename_path_or_throw(staged_node_index_path, node_index_path);
        if (!no_paths) {
            rename_path_or_throw(staged_path_index_path, path_index_path);
        }
        // Publish the graph itself last as the visible signal that the full output set is ready.
        rename_path_or_throw(staged_out_gzip, out_gzip);

        if (!keep_tmp) {
            std::cout << get_time() << ": Removing the temporary files" << std::endl;
        }
        cleanup_work_dir();

        std::cout << get_time() << ": Finished in total time of " << total_time.elapsed() << " seconds" << std::endl;

        return 0;
    } catch (const std::exception& err) {
        // On any failure, remove staged finals first, then clear the per-run work directory.
        cleanup_staged_outputs();
        cleanup_work_dir();
        std::cerr << err.what() << std::endl;
        return 1;
    }
}

}  // namespace gfaidx::indexer

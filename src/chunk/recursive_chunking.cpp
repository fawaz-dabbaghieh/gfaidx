#include "chunk/recursive_chunking.h"

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>

#include "fs/gfa_line_parsers.h"
#include "indexer/direct_binary_writer.h"
#include "indexer/index_gfa_helpers.h"
#include "utils/Timer.h"


namespace gfaidx::chunk {

std::uint32_t compute_ncom(const std::vector<std::uint32_t>& id_to_comm) {
    std::uint32_t max_id = 0;
    for (auto c : id_to_comm) {
        if (c > max_id) max_id = c;
    }
    return id_to_comm.empty() ? 0 : (max_id + 1);
}

bool should_recurse(const CommunityStats& stats, const RecursiveChunkingConfig& config) {
    if (stats.node_count > config.hard_max_nodes) return true;
    if (stats.seq_bp_total > config.hard_max_seq_bp) return true;

    int above = 0;
    if (stats.node_count > config.max_nodes) above++;
    if (stats.seq_bp_total > config.max_seq_bp) above++;
    if (stats.edge_count > config.max_edges) above++;
    return above >= 2;
}

std::vector<CommunityStats> compute_community_stats(
    const std::string& input_gfa,
    const std::unordered_map<std::string, unsigned int>& node_id_map,
    const std::vector<std::uint32_t>& id_to_comm,
    const Reader::Options& reader_options,
    std::uint32_t ncom) {

    std::vector<CommunityStats> stats(ncom);

    // Count nodes per community directly from the id_to_comm map.
    for (std::uint32_t node_id = 0; node_id < id_to_comm.size(); ++node_id) {
        const auto comm = id_to_comm[node_id];
        if (comm < stats.size()) {
            stats[comm].node_count++;
        }
    }

    // Scan the GFA for sequence lengths and intra-community edges.
    Reader reader(reader_options);
    if (!reader.open(input_gfa)) {
        throw std::runtime_error("Could not open input GFA: " + input_gfa);
    }

    std::string node_id;
    std::string node_seq;
    std::string_view line;

    while (reader.read_line(line)) {
        if (line.empty()) continue;

        if (line[0] == 'S') {
            extract_S_node(line, node_id, node_seq);
            auto it = node_id_map.find(node_id);
            if (it == node_id_map.end()) {
                continue;
            }
            auto comm = id_to_comm[it->second];
            if (comm < stats.size()) {
                stats[comm].seq_bp_total += node_seq.size();
            }
        } else if (line[0] == 'L') {
            auto [src, dst] = extract_L_nodes(line);
            auto it_src = node_id_map.find(src);
            auto it_dst = node_id_map.find(dst);
            if (it_src == node_id_map.end() || it_dst == node_id_map.end()) {
                continue;
            }
            auto src_comm = id_to_comm[it_src->second];
            auto dst_comm = id_to_comm[it_dst->second];
            if (src_comm == dst_comm && src_comm < stats.size()) {
                stats[src_comm].edge_count++;
            }
        }
    }
    return stats;
}

void write_community_stats_tsv(const std::vector<CommunityStats>& stats,
                               const std::string& out_path) {
    std::ofstream out(out_path);
    if (!out) {
        throw std::runtime_error("Failed to write community stats TSV: " + out_path);
    }

    out << "community_id\tnode_count\tseq_bp_total\tedge_count\n";
    for (std::size_t cid = 0; cid < stats.size(); ++cid) {
        const auto& s = stats[cid];
        out << cid << '\t' << s.node_count << '\t' << s.seq_bp_total
            << '\t' << s.edge_count << '\n';
    }
}

std::uint64_t write_local_edgelist(
    const std::string& sorted_edgelist,
    const std::unordered_map<std::uint32_t, std::uint32_t>& global_to_local,
    const std::string& out_path) {

    std::ifstream in(sorted_edgelist);
    if (!in) {
        throw std::runtime_error("Failed to open sorted edge list: " + sorted_edgelist);
    }

    std::ofstream out(out_path);
    if (!out) {
        throw std::runtime_error("Failed to write local edge list: " + out_path);
    }

    std::uint32_t src = 0;
    std::uint32_t dst = 0;
    std::uint64_t edges = 0;
    bool first = true;
    while (in >> src >> dst) {
        auto it_src = global_to_local.find(src);
        if (it_src == global_to_local.end()) continue;
        auto it_dst = global_to_local.find(dst);
        if (it_dst == global_to_local.end()) continue;

        if (first) {
            out << it_src->second << " " << it_dst->second;
            first = false;
        } else {
            out << "\n" << it_src->second << " " << it_dst->second;
        }
        edges++;
    }

    return edges;
}


bool refine_id_to_comm_recursive(const std::string& input_gfa,
                                 const std::string& sorted_edgelist,
                                 const std::string& tmp_dir,
                                 const std::unordered_map<std::string, unsigned int>& node_id_map,
                                 const Reader::Options& reader_options,
                                 const RecursiveChunkingConfig& config,
                                 std::vector<std::uint32_t>& id_to_comm,
                                 std::uint32_t& ncom) {
    if (!config.enabled) {
        return false;
    }

    if (id_to_comm.empty()) {
        return false;
    }

    const std::uint32_t base_ncom = compute_ncom(id_to_comm);
    if (base_ncom == 0) {
        return false;
    }

    std::cout << get_time() << ": Computing per-community stats for recursive chunking" << std::endl;
    auto stats = compute_community_stats(input_gfa, node_id_map, id_to_comm, reader_options, base_ncom);

    // check which communities we want to do further chunking on
    std::vector<std::uint32_t> recursed_ids;
    recursed_ids.reserve(base_ncom);
    std::vector<bool> is_recursed(base_ncom, false);
    for (std::uint32_t cid = 0; cid < base_ncom; ++cid) {
        if (should_recurse(stats[cid], config)) {
            is_recursed[cid] = true;
            recursed_ids.push_back(cid);
        }
    }

    if (recursed_ids.empty()) {
        std::cout << get_time() << ": No communities exceed recursive thresholds" << std::endl;
        return false;
    }

    std::sort(recursed_ids.begin(), recursed_ids.end());

    // Build node lists for just the communities we are going to split.
    // todo this can get really big, maybe not the best way to do this
    std::unordered_map<std::uint32_t, std::vector<std::uint32_t>> comm_nodes;
    comm_nodes.reserve(recursed_ids.size());
    {
        std::unordered_map<std::uint32_t, std::size_t> recursed_lookup;
        recursed_lookup.reserve(recursed_ids.size());
        for (std::uint32_t cid : recursed_ids) {
            recursed_lookup.emplace(cid, recursed_lookup.size());
            comm_nodes.emplace(cid, std::vector<std::uint32_t>{});
        }

        for (std::uint32_t node_id = 0; node_id < id_to_comm.size(); ++node_id) {
            auto comm = id_to_comm[node_id];
            if (!is_recursed[comm]) continue;
            comm_nodes[comm].push_back(node_id);
        }
    }

    std::filesystem::path recursive_dir = std::filesystem::path(tmp_dir) / "recursive";
    std::error_code ec;
    std::filesystem::create_directories(recursive_dir, ec);
    // todo this assigning too big of a value
    std::vector new_id_to_comm(id_to_comm.size(), std::numeric_limits<std::uint32_t>::max());

    std::uint32_t next_comm_id = 0;
    std::unordered_map<std::uint32_t, std::uint32_t> non_recursed_remap;
    non_recursed_remap.reserve(base_ncom);

    // Assign new ids for recursed communities in order of their original id.
    for (std::uint32_t cid = 0; cid < base_ncom; ++cid) {
        if (is_recursed[cid]) {
            auto node_it = comm_nodes.find(cid);
            if (node_it == comm_nodes.end() || node_it->second.empty()) {
                continue;
            }

            const auto& nodes = node_it->second;
            std::unordered_map<std::uint32_t, std::uint32_t> global_to_local;
            global_to_local.reserve(nodes.size());
            std::vector<std::uint32_t> local_to_global(nodes.size());

            for (std::size_t i = 0; i < nodes.size(); ++i) {
                global_to_local.emplace(nodes[i], static_cast<std::uint32_t>(i));
                local_to_global[i] = nodes[i];
            }

            const std::string comm_prefix = "comm_" + std::to_string(cid);
            const std::string local_edgelist = (recursive_dir / (comm_prefix + "_edgelist.txt")).string();
            const std::string local_binary = (recursive_dir / (comm_prefix + "_binary.bin")).string();

            std::cout << get_time() << ": Building local edge list for community " << cid << std::endl;
            Timer timer;
            std::uint64_t local_edges = write_local_edgelist(sorted_edgelist, global_to_local, local_edgelist);
            std::cout << get_time() << ": Local edge list has " << local_edges
                      << " edges (" << timer.elapsed() << " seconds)" << std::endl;

            if (local_edges == 0) {
                // Keep this community intact when there are no local edges to split.
                std::uint32_t only_comm = next_comm_id++;
                for (const auto global_id : nodes) {
                    new_id_to_comm[global_id] = only_comm;
                }
                continue;
            }

            try {
                write_binary_graph_from_edgelist(local_edgelist,
                                                 local_binary,
                                                 static_cast<std::uint32_t>(nodes.size()));
            } catch (const std::exception& err) {
                std::cerr << "Failed to write local binary for community " << cid
                          << ": " << err.what() << std::endl;
                return false;
            }

            std::cout << get_time() << ": Running community detection for community " << cid << std::endl;
            BGraph local_graph;
            // todo: I need to stop communities run in the small local_binary graph
            //       once the number of new chunks is less than the cutoff already
            //       I can make use of the iteration variable in the while loop
            //       make this variable passable to the function and set it to 1 here
            indexer::generate_communities(local_binary, local_graph);

            // Map local community ids to new global ids deterministically.
            const auto local_ncom = static_cast<std::uint32_t>(local_graph.nodes.size());
            std::vector<std::uint32_t> local_comm_to_global(local_ncom, 0);
            for (std::uint32_t lc = 0; lc < local_ncom; ++lc) {
                local_comm_to_global[lc] = next_comm_id++;
            }

            for (std::uint32_t lc = 0; lc < local_ncom; ++lc) {
                for (const auto local_node : local_graph.nodes[lc]) {
                    const auto global_id = local_to_global[local_node];
                    new_id_to_comm[global_id] = local_comm_to_global[lc];
                }
            }
        } else {
            non_recursed_remap.emplace(cid, next_comm_id++);
        }
    }

    // Fill in any nodes that belong to untouched communities.
    for (std::uint32_t node_id = 0; node_id < id_to_comm.size(); ++node_id) {
        if (new_id_to_comm[node_id] != std::numeric_limits<std::uint32_t>::max()) {
            continue;
        }
        const auto old_comm = id_to_comm[node_id];
        auto it = non_recursed_remap.find(old_comm);
        if (it == non_recursed_remap.end()) {
            // Should not happen, but keep the mapping safe.
            non_recursed_remap.emplace(old_comm, next_comm_id++);
            it = non_recursed_remap.find(old_comm);
        }
        new_id_to_comm[node_id] = it->second;
    }

    id_to_comm.swap(new_id_to_comm);
    ncom = next_comm_id;

    std::cout << get_time() << ": Recursive chunking produced " << ncom
              << " communities (from " << base_ncom << ")" << std::endl;
    return true;
}

}  // namespace gfaidx::chunk

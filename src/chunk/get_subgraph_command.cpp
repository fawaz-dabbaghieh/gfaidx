#include "chunk/get_subgraph_command.h"

#include <algorithm>
#include <cstdlib>
#include <cstdint>
#include <deque>
#include <fstream>
#include <iostream>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "chunk/chunk_reader.h"
#include "fs/fs_helpers.h"
#include "fs/gfa_line_parsers.h"
#include "indexer/node_hash_index.h"
#include "paths/path_index.h"
#include "paths/walk_coords.h"
#include "utils/debug_trace.h"
#include "utils/Timer.h"
#include "utils/cli_helpers.h"

namespace gfaidx::chunk {
namespace {

struct ResolvedIndexPaths {
    std::string idx_path;
    std::string ndx_path;
    std::string pdx_path;
    bool has_pdx{false};
    bool pdx_explicit{false};
};

// Store one parsed shared edge and refer to it from both endpoint posting lists.
// This matches the Python shared-edge cache without duplicating the edge object.
struct SharedEdge {
    std::string left;
    std::string right;
};

// BFS uses original node-name strings so reading L records never requires a
// rank lookup across the mmap-backed .ndx. The node index is consulted only to
// locate communities that have not already been loaded.
struct NeighborhoodState {
    // Initialize per-community load flags with the .idx span count while keeping
    // the graph and node indexes as non-owning references for the query lifetime.
    NeighborhoodState(const std::string& graph_path,
                      const std::vector<CommunitySpan>& community_spans,
                      const indexer::NodeHashIndex& index,
                      std::uint32_t shared_id)
        : gz_path(graph_path),
          spans(community_spans),
          node_index(index),
          shared_chunk_id(shared_id),
          loaded_communities(community_spans.size(), 0) {}

    const std::string& gz_path;
    const std::vector<CommunitySpan>& spans;
    const indexer::NodeHashIndex& node_index;
    std::uint32_t shared_chunk_id;
    std::unordered_map<std::string, std::uint32_t> node_community_cache;
    std::unordered_map<std::string, std::vector<std::string>> adjacency;
    std::vector<std::uint8_t> loaded_communities;
    std::vector<std::uint32_t> touched_communities;
    std::vector<SharedEdge> shared_edges;
    std::unordered_map<std::string, std::vector<std::size_t>> shared_edges_by_node;
    std::vector<std::uint8_t> applied_shared_edges;
    bool shared_edge_cache_loaded{false};
};

struct EmissionStats {
    std::uint64_t s_lines{0};
    std::uint64_t l_lines{0};
};

void warn_get_subgraph(std::string_view message) {
    std::cerr << get_time() << ": Warning: " << message << std::endl;
}

void info_get_subgraph(std::string_view message) {
    std::cerr << get_time() << ": " << message << std::endl;
}

// Build contextual error text around the exact get_subgraph step that was
// running so a malformed member still carries its community/line context.
std::runtime_error annotate_get_subgraph_error(std::string_view context,
                                               const std::exception& err) {
    return std::runtime_error(std::string(context) + ": " + err.what());
}

std::string extract_s_node_id_only(std::string_view line) {
    const auto t1 = line.find('\t');
    if (t1 == std::string_view::npos) offending_line(line);
    const auto t2 = line.find('\t', t1 + 1);
    if (t2 == std::string_view::npos) offending_line(line);
    return std::string(line.substr(t1 + 1, t2 - (t1 + 1)));
}

// Remember membership learned from a community member and reject an index/GFA
// disagreement instead of silently assigning one string to two communities.
void remember_node_community(NeighborhoodState& state,
                             const std::string& node_name,
                             std::uint32_t community_id) {
    const auto [it, inserted] = state.node_community_cache.emplace(node_name, community_id);
    if (!inserted && it->second != community_id) {
        throw std::runtime_error("Node '" + node_name +
                                 "' appeared in multiple community members");
    }
}

// Resolve a node's community only when BFS reaches a node outside the chunks
// already loaded. Unlike the old rank cache, this never receives every endpoint
// from the global shared-edge member.
std::uint32_t resolve_node_community(NeighborhoodState& state,
                                     std::string_view node_name,
                                     std::string_view context) {
    std::string key(node_name);
    const auto it = state.node_community_cache.find(key);
    if (it != state.node_community_cache.end()) {
        if (gfaidx::debug::subgraph_trace_enabled()) {
            std::ostringstream oss;
            oss << context << " resolved cached node '" << key
                << "' to community " << it->second;
            gfaidx::debug::log_subgraph_trace(oss.str());
        }
        return it->second;
    }

    std::uint32_t community_id = 0;
    if (!state.node_index.lookup(key, community_id)) {
        std::ostringstream oss;
        oss << context << " could not find node '" << key << "' in .ndx";
        throw std::runtime_error(oss.str());
    }
    if (gfaidx::debug::subgraph_trace_enabled()) {
        std::ostringstream oss;
        oss << context << " resolved node '" << key
            << "' to community " << community_id;
        gfaidx::debug::log_subgraph_trace(oss.str());
    }
    state.node_community_cache.emplace(std::move(key), community_id);
    return community_id;
}

// Keep one string adjacency entry in each direction. Community loading happens
// before BFS iterates a node, so these vectors are not mutated mid-iteration.
void add_undirected_edge(NeighborhoodState& state,
                         const std::string& left,
                         const std::string& right) {
    state.adjacency[left].push_back(right);
    if (left != right) {
        state.adjacency[right].push_back(left);
    }
}

ResolvedIndexPaths resolve_index_paths(const std::string& input_gz,
                                       const std::string& idx_override,
                                       const std::string& ndx_override,
                                       const std::string& pdx_override,
                                       bool include_paths) {
    ResolvedIndexPaths paths;
    paths.idx_path = idx_override;
    paths.ndx_path = ndx_override;
    paths.pdx_path = pdx_override;
    paths.pdx_explicit = !paths.pdx_path.empty();

    // inferred because it's usually just attached to the end of the input graph file
    if (paths.idx_path.empty()) {
        paths.idx_path = utils::companion_path(input_gz, ".idx");
    }
    if (paths.ndx_path.empty()) {
        paths.ndx_path = utils::companion_path(input_gz, ".ndx");
    }
    if (paths.pdx_path.empty()) {
        paths.pdx_path = utils::companion_path(input_gz, ".pdx");
    }

    if (!file_exists(paths.idx_path.c_str())) {
        throw std::runtime_error("Index file does not exist: " + paths.idx_path);
    }
    if (!file_exists(paths.ndx_path.c_str())) {
        throw std::runtime_error("Node index file does not exist: " + paths.ndx_path);
    }

    // Skip all .pdx discovery and warnings when callers explicitly requested a
    // graph-only subgraph extraction without any P/W subpath output.
    if (!include_paths) {
        paths.has_pdx = false;
        return paths;
    }

    if (paths.pdx_explicit) {
        if (!file_exists(paths.pdx_path.c_str())) {
            throw std::runtime_error("Path index file does not exist: " + paths.pdx_path);
        }
        paths.has_pdx = true;
    } else if (file_exists(paths.pdx_path.c_str())) {
        paths.has_pdx = true;
    } else {
        warn_get_subgraph("No companion .pdx found at " + paths.pdx_path +
                          "; continuing without P/W subpaths");
        paths.has_pdx = false;
    }

    return paths;
}

// Build the global shared-edge cache once using only endpoint strings. This is
// the current-format compatibility backend; future boundary members can replace
// it without changing the string-based BFS.
void load_shared_edge_cache(NeighborhoodState& state) {
    if (state.shared_edge_cache_loaded) {
        return;
    }
    state.shared_edge_cache_loaded = true;

    if (state.shared_chunk_id >= state.spans.size()) {
        return;
    }
    const auto shared_span = state.spans[state.shared_chunk_id];
    if (shared_span.gz_size == 0) {
        return;
    }

    info_get_subgraph("Loading shared-edge cache from member " +
                      std::to_string(state.shared_chunk_id));
    std::uint64_t shared_edge_lines = 0;
    stream_community_lines_from_gz_range(
        state.gz_path,
        shared_span.gz_offset,
        shared_span.gz_size,
        [&](const std::string& line) -> bool {
            if (line.empty() || line[0] != 'L') return true;
            ++shared_edge_lines;
            try {
                auto [left_name, right_name] = extract_L_nodes(line);
                const std::size_t edge_id = state.shared_edges.size();
                state.shared_edges.push_back(SharedEdge{left_name, right_name});
                state.shared_edges_by_node[left_name].push_back(edge_id);
                if (left_name != right_name) {
                    state.shared_edges_by_node[right_name].push_back(edge_id);
                }
            } catch (const std::exception& err) {
                std::ostringstream oss;
                oss << "While caching shared L line " << shared_edge_lines
                    << " from span offset=" << shared_span.gz_offset
                    << " size=" << shared_span.gz_size
                    << " line='" << line << "'";
                throw annotate_get_subgraph_error(oss.str(), err);
            }
            return true;
        });
    state.applied_shared_edges.assign(state.shared_edges.size(), 0);
    info_get_subgraph("Finished shared-edge cache with " +
                      std::to_string(state.shared_edges.size()) + " edges across " +
                      std::to_string(state.shared_edges_by_node.size()) + " endpoint nodes");
}

// Apply every cached shared edge incident to this community's S records. Edge
// ids are sorted to preserve the original shared-member order, and each edge is
// inserted once even after the opposite community is loaded later.
void apply_shared_edges_for_nodes(NeighborhoodState& state,
                                  const std::vector<std::string>& community_nodes) {
    std::vector<std::size_t> edge_ids;
    for (const auto& node_name : community_nodes) {
        const auto posting_it = state.shared_edges_by_node.find(node_name);
        if (posting_it == state.shared_edges_by_node.end()) {
            continue;
        }
        for (const std::size_t edge_id : posting_it->second) {
            if (state.applied_shared_edges[edge_id] == 0) {
                edge_ids.push_back(edge_id);
            }
        }
    }

    std::sort(edge_ids.begin(), edge_ids.end());
    edge_ids.erase(std::unique(edge_ids.begin(), edge_ids.end()), edge_ids.end());
    for (const std::size_t edge_id : edge_ids) {
        if (state.applied_shared_edges[edge_id] != 0) {
            continue;
        }
        state.applied_shared_edges[edge_id] = 1;
        const auto& edge = state.shared_edges[edge_id];
        add_undirected_edge(state, edge.left, edge.right);
    }
}

// Load one complete local community using node-name strings and then attach its
// incident shared edges from the one-pass cache without consulting .ndx.
void load_community_adjacency(NeighborhoodState& state, std::uint32_t community_id) {
    if (community_id >= state.spans.size() || community_id == state.shared_chunk_id) {
        throw std::runtime_error("Community id out of range in .idx: " +
                                 std::to_string(community_id));
    }
    if (state.loaded_communities[community_id] != 0) {
        return;
    }

    state.loaded_communities[community_id] = 1;
    state.touched_communities.push_back(community_id);
    info_get_subgraph("Loading adjacency for community " + std::to_string(community_id) +
                      " (" + std::to_string(state.touched_communities.size()) +
                      " communities touched so far)");

    const auto span = state.spans[community_id];
    std::vector<std::string> community_nodes;
    std::uint64_t local_edge_lines = 0;
    if (span.gz_size > 0) {
        stream_community_lines_from_gz_range(
            state.gz_path,
            span.gz_offset,
            span.gz_size,
            [&](const std::string& line) -> bool {
                if (line.empty()) return true;
                try {
                    if (line[0] == 'S') {
                        std::string node_name = extract_s_node_id_only(line);
                        remember_node_community(state, node_name, community_id);
                        community_nodes.push_back(std::move(node_name));
                    } else if (line[0] == 'L') {
                        ++local_edge_lines;
                        auto [left_name, right_name] = extract_L_nodes(line);
                        remember_node_community(state, left_name, community_id);
                        remember_node_community(state, right_name, community_id);
                        add_undirected_edge(state, left_name, right_name);
                    }
                } catch (const std::exception& err) {
                    std::ostringstream oss;
                    oss << "While processing community " << community_id
                        << " local edge line " << local_edge_lines
                        << " from span offset=" << span.gz_offset
                        << " size=" << span.gz_size
                        << " line='" << line << "'";
                    throw annotate_get_subgraph_error(oss.str(), err);
                }
                return true;
            });
    }

    load_shared_edge_cache(state);
    apply_shared_edges_for_nodes(state, community_nodes);
    if (gfaidx::debug::subgraph_trace_enabled()) {
        std::ostringstream oss;
        oss << "Finished string adjacency load for community " << community_id
            << " with " << community_nodes.size() << " S lines and "
            << local_edge_lines << " local L lines";
        gfaidx::debug::log_subgraph_trace(oss.str());
    }
}

// Collect a strict max_nodes BFS neighborhood using original GFA node names.
// Community loading occurs only when a queued node is actually expanded, so the
// final node admitted at the cap cannot trigger an unnecessary chunk load.
std::vector<std::string> bfs_collect_node_names(NeighborhoodState& state,
                                                const std::vector<std::string>& start_nodes,
                                                std::uint32_t max_nodes) {
    std::deque<std::string> queue;
    std::unordered_set<std::string> discovered;
    discovered.reserve(static_cast<std::size_t>(max_nodes) * 2);
    // Preserve the caller's seed order while suppressing repeated path nodes.
    for (const auto& start_node : start_nodes) {
        if (discovered.insert(start_node).second) queue.push_back(start_node);
    }
    if (discovered.size() > max_nodes) {
        throw std::runtime_error("The coordinate interval contains more seed nodes than --max_nodes");
    }

    while (!queue.empty() && discovered.size() < max_nodes) {
        std::string current = std::move(queue.front());
        queue.pop_front();

        std::ostringstream context;
        context << "BFS node '" << current << "'";
        const std::uint32_t community_id =
            resolve_node_community(state, current, context.str());
        load_community_adjacency(state, community_id);

        const auto adjacency_it = state.adjacency.find(current);
        if (adjacency_it == state.adjacency.end()) {
            continue;
        }

        for (const auto& neighbor : adjacency_it->second) {
            if (discovered.size() >= max_nodes) break;
            if (discovered.insert(neighbor).second) {
                queue.push_back(neighbor);
                if (discovered.size() % 500 == 0) {
                    std::cerr << get_time() << ": BFS neighborhood currently has "
                              << discovered.size() << " nodes across "
                              << state.touched_communities.size() << " loaded communities"
                              << std::endl;
                }
            }
        }
    }

    std::vector<std::string> node_names(discovered.begin(), discovered.end());
    std::sort(node_names.begin(), node_names.end());
    return node_names;
}

void emit_header_if_present(std::ostream& out,
                            const std::string& gz_path,
                            const std::vector<CommunitySpan>& spans) {
    if (spans.empty() || spans[0].gz_size == 0) return;

    bool emitted = false;
    stream_community_lines_from_gz_range(
        gz_path,
        spans[0].gz_offset,
        spans[0].gz_size,
        [&](const std::string& line) -> bool {
            if (!line.empty() && line[0] == 'H') {
                out << line << '\n';
                emitted = true;
                return false;
            }
            return true;
        });
    (void)emitted;
}

// Re-stream the original chunk lines and emit only the records whose endpoint
// names are part of the final extracted node set. String membership avoids
// touching .ndx pages again during materialization.
EmissionStats emit_filtered_member(std::ostream& out,
                                   const std::string& gz_path,
                                   const CommunitySpan& span,
                                   const std::unordered_set<std::string>& node_set) {
    EmissionStats stats{};
    if (span.gz_size == 0) return stats;

    stream_community_lines_from_gz_range(
        gz_path,
        span.gz_offset,
        span.gz_size,
        [&](const std::string& line) -> bool {
            if (line.empty() || line[0] == 'H') return true;

            if (line[0] == 'S') {
                const std::string node_name = extract_s_node_id_only(line);
                if (node_set.find(node_name) != node_set.end()) {
                    out << line << '\n';
                    ++stats.s_lines;
                }
                return true;
            }

            if (line[0] == 'L') {
                auto [left_name, right_name] = extract_L_nodes(line);
                if (node_set.find(left_name) != node_set.end() &&
                    node_set.find(right_name) != node_set.end()) {
                    out << line << '\n';
                    ++stats.l_lines;
                }
            }
            return true;
        });
    return stats;
}

// Convert only the final selected node names to .ndx ranks when path extraction
// needs the rank-aligned .pdx. Graph-only queries never call this helper.
std::vector<std::uint32_t> resolve_selected_node_ranks(
    const indexer::NodeHashIndex& node_index,
    const std::vector<std::string>& node_names) {
    std::vector<std::uint32_t> node_ids;
    node_ids.reserve(node_names.size());
    for (const auto& node_name : node_names) {
        std::uint32_t rank = 0;
        if (!node_index.lookup_rank(node_name, rank)) {
            throw std::runtime_error("Selected node was not found in .ndx: " + node_name);
        }
        node_ids.push_back(rank);
    }
    std::sort(node_ids.begin(), node_ids.end());
    return node_ids;
}

// Append indexed P/W subpaths for the extracted node set when a companion
// .pdx exists. This keeps graph extraction and path extraction in one command.
std::uint64_t emit_subpaths_if_available(std::ostream& out,
                                         const std::string& pdx_path,
                                         const std::vector<std::uint32_t>& node_ids,
                                         const indexer::NodeHashIndex& node_index,
                                         const std::string& source_gfa,
                                         const std::string& length_index_path,
                                         bool with_walk_coordinates) {
    paths::PathIndexReader index(pdx_path);
    const auto runs = paths::find_subpaths_for_node_ids(index, node_ids);
    if (runs.empty()) {
        return 0;
    }

    // W records use SeqStart/SeqEnd fields, and coordinate-indexed P records
    // use path-local offsets in their output name. Both need node lengths.
    bool has_coordinate_run = false;
    if (with_walk_coordinates) {
        for (const auto& run : runs) {
            const auto record_type = index.get_path_info(run.path_id).record_type;
            if (record_type == 'W' || record_type == 'P') {
                has_coordinate_run = true;
                break;
            }
        }
    }

    paths::WalkCoordState walk_coord_state;
    if (with_walk_coordinates && has_coordinate_run) {
        // Prefer .lnx for node lengths. If it is absent, the indexed graph
        // remains a valid fallback because all S records stay in the gzip.
        walk_coord_state = paths::load_node_lengths_by_index(
            index,
            node_index,
            source_gfa,
            length_index_path,
            [](const std::string& message) {
                warn_get_subgraph(message);
            });
    }

    std::uint64_t emitted = 0;
    for (const auto& run : runs) {
        const auto info = index.get_path_info(run.path_id);
        const auto base_name = (info.record_type == 'W') ? info.seq_id : info.name;
        const std::string subpath_name = std::string(base_name) + "#subpath_" +
            std::to_string(run.start_step) + "_" +
            std::to_string(run.start_step + run.step_count - 1);

        if (with_walk_coordinates && walk_coord_state.usable &&
            (info.record_type == 'W' || info.record_type == 'P')) {
            bool wrote_coordinates = false;
            if (info.record_type == 'W') {
                wrote_coordinates = paths::write_w_subpath_with_coords_bounded(
                    out,
                    index,
                    run.path_id,
                    walk_coord_state,
                    run.start_step,
                    run.step_count,
                    subpath_name,
                    [](const std::string& message) {
                        warn_get_subgraph(message);
                    });
            } else {
                wrote_coordinates = paths::write_p_subpath_with_coords_bounded(
                    out,
                    index,
                    run.path_id,
                    walk_coord_state,
                    run.start_step,
                    run.step_count,
                    [](const std::string& message) {
                        warn_get_subgraph(message);
                    });
            }
            if (wrote_coordinates) {
                ++emitted;
                continue;
            }
        }

        paths::write_subpath_as_gfa_line(out,
                                         index,
                                         run.path_id,
                                         run.start_step,
                                         run.step_count,
                                         subpath_name);
        ++emitted;
    }
    return emitted;
}

// Write an already selected node set by replaying only its communities and the
// shared-edge member. BFS and posting-based coordinate selection both finish
// here, keeping graph/path output behavior identical between selection modes.
int materialize_selected_subgraph(
    const SubgraphExtractionOptions& options,
    const std::vector<CommunitySpan>& spans,
    const ResolvedIndexPaths& index_paths,
    const indexer::NodeHashIndex& node_index,
    std::vector<std::string> node_names,
    std::vector<std::uint32_t> materialization_communities,
    const std::vector<std::uint32_t>* selected_node_ranks) {

    if (node_names.empty()) {
        warn_get_subgraph("No nodes were selected for the requested subgraph");
        return 0;
    }

    // Community order is made deterministic before gzip members are replayed.
    std::sort(materialization_communities.begin(),
              materialization_communities.end());
    materialization_communities.erase(
        std::unique(materialization_communities.begin(),
                    materialization_communities.end()),
        materialization_communities.end());

    // Resolve BFS names before moving them into the membership table. Exact
    // posting selection already supplies the rank vector and skips this pass.
    std::vector<std::uint32_t> resolved_node_ranks;
    const std::vector<std::uint32_t>* path_node_ranks =
        selected_node_ranks;
    if (options.include_paths && index_paths.has_pdx &&
        path_node_ranks == nullptr) {
        resolved_node_ranks =
            resolve_selected_node_ranks(node_index, node_names);
        path_node_ranks = &resolved_node_ranks;
    }

    // Move names into the lookup table instead of keeping a second full string
    // copy during chunk replay. Releasing the moved-from vector also drops its
    // per-string object array before the potentially large shared-edge scan.
    std::unordered_set<std::string> node_set;
    node_set.reserve(node_names.size());
    for (auto& node_name : node_names) {
        node_set.emplace(std::move(node_name));
    }
    std::vector<std::string>().swap(node_names);

    std::ofstream out(options.output_gfa);
    if (!out) {
        throw std::runtime_error("Failed to open output GFA file for writing: " +
                                 options.output_gfa);
    }

    info_get_subgraph("Starting subgraph materialization into " +
                      options.output_gfa);
    emit_header_if_present(out, options.input_gz, spans);
    EmissionStats total_stats{};
    for (const auto community_id : materialization_communities) {
        info_get_subgraph("Materializing community " +
                          std::to_string(community_id));
        const auto stats = emit_filtered_member(out,
                                                options.input_gz,
                                                spans[community_id],
                                                node_set);
        total_stats.s_lines += stats.s_lines;
        total_stats.l_lines += stats.l_lines;
    }
    if (spans.size() >= 2) {
        const auto shared_chunk_id =
            static_cast<std::uint32_t>(spans.size() - 1);
        info_get_subgraph("Materializing shared-edge member " +
                          std::to_string(shared_chunk_id));
        const auto stats = emit_filtered_member(out,
                                                options.input_gz,
                                                spans[shared_chunk_id],
                                                node_set);
        total_stats.s_lines += stats.s_lines;
        total_stats.l_lines += stats.l_lines;
    }
    info_get_subgraph("Finished subgraph materialization with " +
                      std::to_string(total_stats.s_lines) + " S lines and " +
                      std::to_string(total_stats.l_lines) + " L lines");

    if (options.include_paths && index_paths.has_pdx) {
        info_get_subgraph("Starting indexed subpath extraction from " +
                          index_paths.pdx_path);
        const auto subpath_count = emit_subpaths_if_available(
            out,
            index_paths.pdx_path,
            *path_node_ranks,
            node_index,
            options.input_gz,
            options.lnx_path,
            options.with_walk_coordinates);
        info_get_subgraph("Finished indexed subpath extraction with " +
                          std::to_string(subpath_count) + " P/W records");
    }

    info_get_subgraph("Finished writing extracted subgraph to " +
                      options.output_gfa);
    return 0;
}

}  // namespace

void configure_get_subgraph_parser(argparse::ArgumentParser& parser) {
    parser.add_argument("in_gz")
      .help("input indexed GFA gzip file");

    parser.add_argument("start_node")
      .help("node id to start the BFS neighborhood from");

    parser.add_argument("out_gfa")
      .help("output GFA file for the extracted subgraph");

    parser.add_argument("--idx")
      .default_value(std::string(""))
      .nargs(1)
      .help("path to .idx file (defaults to <in_gz>.idx)");

    parser.add_argument("--ndx")
      .default_value(std::string(""))
      .nargs(1)
      .help("path to .ndx file (defaults to <in_gz>.ndx)");

    parser.add_argument("--pdx")
      .default_value(std::string(""))
      .nargs(1)
      .help("path to .pdx file (defaults to <in_gz>.pdx); used to emit P/W subpaths when available");

    parser.add_argument("--max_nodes")
      .default_value(std::string("100"))
      .nargs(1)
      .help("maximum number of nodes to include in the BFS neighborhood (default: 100)");

    parser.add_argument("--no_paths").default_value(false)
      .implicit_value(true)
      .help("skip indexed P/W subpath extraction and emit only the graph records");

    parser.add_argument("--debug_trace").default_value(false)
      .implicit_value(true)
      .help("enable temporary cross-file tracing for get_subgraph debugging");
}

int extract_subgraph_from_seeds(const SubgraphExtractionOptions& options,
                                const std::vector<std::string>& seed_nodes) {
    if (!file_exists(options.input_gz.c_str())) {
        throw std::runtime_error("Input file does not exist: " + options.input_gz);
    }
    if (seed_nodes.empty()) {
        throw std::runtime_error("At least one seed node is required for subgraph extraction");
    }
    if (options.max_nodes == 0) {
        throw std::runtime_error("--max_nodes must be greater than zero");
    }

    // Export the trace flag before opening indexes so helper code in other
    // translation units can participate in the same debug run.
    if (options.debug_trace) {
        ::setenv("GFAIDX_DEBUG_SUBGRAPH", "1", 1);
        debug::log_subgraph_trace("Enabled temporary get_subgraph trace logging");
    }

    const auto index_paths = resolve_index_paths(options.input_gz,
                                                 options.idx_path,
                                                 options.ndx_path,
                                                 options.pdx_path,
                                                 options.include_paths);
    const auto spans = load_all_community_spans_tsv(index_paths.idx_path);
    if (spans.empty()) {
        throw std::runtime_error("The .idx file does not contain any community spans");
    }

    // Record the resolved companion files once so the external repro log
    // confirms which index set was paired with the input gzip.
    if (debug::subgraph_trace_enabled()) {
        std::ostringstream oss;
        oss << "Resolved index paths idx=" << index_paths.idx_path
            << " ndx=" << index_paths.ndx_path
            << " pdx=" << index_paths.pdx_path
            << " has_pdx=" << index_paths.has_pdx
            << " spans=" << spans.size();
        gfaidx::debug::log_subgraph_trace(oss.str());
    }

    indexer::NodeHashIndex node_index(index_paths.ndx_path);
    std::vector<std::string> unique_seeds = seed_nodes;
    std::sort(unique_seeds.begin(), unique_seeds.end());
    unique_seeds.erase(std::unique(unique_seeds.begin(), unique_seeds.end()), unique_seeds.end());
    if (unique_seeds.size() > options.max_nodes) {
        throw std::runtime_error("The query contains " + std::to_string(unique_seeds.size()) +
                                 " seed nodes, exceeding --max_nodes=" +
                                 std::to_string(options.max_nodes));
    }

    std::vector<std::uint32_t> seed_communities;
    seed_communities.reserve(unique_seeds.size());
    for (const auto& seed : unique_seeds) {
        std::uint32_t community_id = 0;
        if (!node_index.lookup(seed, community_id)) {
            throw std::runtime_error("Seed node was not found in .ndx: " + seed);
        }
        seed_communities.push_back(community_id);
    }

    std::vector<std::string> node_names;
    std::vector<std::uint32_t> materialization_communities;
    std::size_t loaded_community_count = 0;
    {
        // Scope the potentially large string adjacency and shared-edge cache
        // to BFS so they are released before materialization and path work.
        const std::uint32_t shared_chunk_id =
            spans.size() >= 2 ? static_cast<std::uint32_t>(spans.size() - 1)
                              : std::numeric_limits<std::uint32_t>::max();
        NeighborhoodState state(options.input_gz, spans, node_index, shared_chunk_id);
        for (std::size_t i = 0; i < unique_seeds.size(); ++i) {
            state.node_community_cache.emplace(unique_seeds[i], seed_communities[i]);
        }

        info_get_subgraph("Starting multi-source BFS from " +
                          std::to_string(unique_seeds.size()) +
                          " seed nodes with max_nodes=" +
                          std::to_string(options.max_nodes));
        node_names = bfs_collect_node_names(state, unique_seeds, options.max_nodes);
        loaded_community_count = state.touched_communities.size();

        // The last admitted node may never be expanded, so independently
        // collect every selected node's community for final S/L replay.
        std::unordered_set<std::uint32_t> community_set;
        community_set.reserve(node_names.size());
        for (const auto& node_name : node_names) {
            const std::uint32_t community_id =
                resolve_node_community(state, node_name, "selected node materialization");
            if (community_id >= spans.size() || community_id == shared_chunk_id) {
                throw std::runtime_error("Selected node resolved to an invalid community: " +
                                         node_name);
            }
            community_set.insert(community_id);
        }
        materialization_communities.assign(community_set.begin(), community_set.end());
    }

    info_get_subgraph("BFS finished with " + std::to_string(node_names.size()) +
                      " nodes across " + std::to_string(loaded_community_count) +
                      " loaded communities");
    return materialize_selected_subgraph(options,
                                         spans,
                                         index_paths,
                                         node_index,
                                         std::move(node_names),
                                         std::move(materialization_communities),
                                         nullptr);
}

int extract_subgraph_from_node_ranks(
    const SubgraphExtractionOptions& options,
    const std::vector<std::uint32_t>& node_ranks) {

    if (!file_exists(options.input_gz.c_str())) {
        throw std::runtime_error("Input file does not exist: " +
                                 options.input_gz);
    }
    if (node_ranks.empty()) {
        throw std::runtime_error(
            "At least one node rank is required for exact subgraph extraction");
    }

    // Export the same trace flag used by BFS so cross-index diagnostics remain
    // available for exact path-supported materialization.
    if (options.debug_trace) {
        ::setenv("GFAIDX_DEBUG_SUBGRAPH", "1", 1);
        debug::log_subgraph_trace(
            "Enabled temporary exact-subgraph trace logging");
    }

    // Exact rank materialization always needs .pdx for rank-to-name conversion,
    // even when --no_paths suppresses P/W records in the output.
    const auto index_paths = resolve_index_paths(options.input_gz,
                                                 options.idx_path,
                                                 options.ndx_path,
                                                 options.pdx_path,
                                                 true);
    if (!index_paths.has_pdx) {
        throw std::runtime_error(
            "Exact rank-based subgraph extraction requires a companion .pdx");
    }

    const auto spans = load_all_community_spans_tsv(index_paths.idx_path);
    if (spans.empty()) {
        throw std::runtime_error(
            "The .idx file does not contain any community spans");
    }

    indexer::NodeHashIndex node_index(index_paths.ndx_path);
    std::vector<std::uint32_t> unique_node_ranks = node_ranks;
    std::sort(unique_node_ranks.begin(), unique_node_ranks.end());
    unique_node_ranks.erase(
        std::unique(unique_node_ranks.begin(), unique_node_ranks.end()),
        unique_node_ranks.end());

    std::vector<std::string> node_names;
    node_names.reserve(unique_node_ranks.size());
    std::vector<std::uint32_t> materialization_communities;
    std::vector<std::uint8_t> seen_communities(spans.size(), 0);
    const std::uint32_t shared_chunk_id =
        spans.size() >= 2 ? static_cast<std::uint32_t>(spans.size() - 1)
                          : std::numeric_limits<std::uint32_t>::max();

    {
        // Scope the reader so its path metadata and file handle are released
        // before selected names move into the graph materialization hash set.
        paths::PathIndexReader path_index(index_paths.pdx_path);
        if (path_index.node_count() != node_index.size()) {
            throw std::runtime_error(
                ".pdx and .ndx node counts differ; rebuild aligned indexes");
        }

        for (const auto node_rank : unique_node_ranks) {
            if (node_rank >= path_index.node_count()) {
                throw std::runtime_error(
                    "Selected node rank is outside the .pdx node table");
            }

            const auto community_id =
                node_index.community_id_by_rank(node_rank);
            if (community_id >= spans.size() ||
                community_id == shared_chunk_id) {
                throw std::runtime_error(
                    "Selected node rank resolved to an invalid community");
            }
            if (seen_communities[community_id] == 0) {
                seen_communities[community_id] = 1;
                materialization_communities.push_back(community_id);
            }
            node_names.emplace_back(path_index.copy_node_name(node_rank));
        }
    }

    info_get_subgraph("Exact path-supported selection contains " +
                      std::to_string(unique_node_ranks.size()) +
                      " nodes across " +
                      std::to_string(materialization_communities.size()) +
                      " communities; BFS was not run");
    return materialize_selected_subgraph(options,
                                         spans,
                                         index_paths,
                                         node_index,
                                         std::move(node_names),
                                         std::move(materialization_communities),
                                         &unique_node_ranks);
}

int run_get_subgraph(const argparse::ArgumentParser& program) {
    try {
        SubgraphExtractionOptions options;
        options.input_gz = program.get<std::string>("in_gz");
        options.output_gfa = program.get<std::string>("out_gfa");
        options.idx_path = program.get<std::string>("idx");
        options.ndx_path = program.get<std::string>("ndx");
        options.pdx_path = program.get<std::string>("pdx");
        options.max_nodes = utils::parse_u32_strict(
            program.get<std::string>("max_nodes"),
            "--max_nodes",
            1,
            std::numeric_limits<std::uint32_t>::max());
        options.include_paths = !program.get<bool>("no_paths");
        options.debug_trace = program.get<bool>("debug_trace");
        return extract_subgraph_from_seeds(
            options,
            std::vector<std::string>{program.get<std::string>("start_node")});
    } catch (const std::exception& err) {
        std::cerr << err.what() << std::endl;
        return 1;
    }
}

}  // namespace gfaidx::chunk

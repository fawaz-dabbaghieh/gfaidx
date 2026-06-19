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
#include "utils/debug_trace.h"
#include "utils/Timer.h"

namespace gfaidx::chunk {
namespace {

struct ResolvedIndexPaths {
    std::string idx_path;
    std::string ndx_path;
    std::string pdx_path;
    bool has_pdx{false};
    bool pdx_explicit{false};
};

// BFS mode keeps only a simple node-level neighborhood state in memory.
// Final GFA emission replays original chunk lines so edge orientation and
// record text match the indexed graph instead of being reconstructed.
struct NeighborhoodState {
    const std::string& gz_path;
    const std::vector<CommunitySpan>& spans;
    const indexer::NodeHashIndex& node_index;
    std::uint32_t shared_chunk_id;
    std::unordered_map<std::string, std::uint32_t> node_id_cache;
    std::unordered_map<std::uint32_t, std::vector<std::uint32_t>> adjacency;
    std::vector<std::uint8_t> loaded_communities;
    std::vector<std::uint32_t> touched_communities;
    // Track the adjacency vector currently being iterated by BFS so temporary
    // debug traces can detect whether a community load mutates that same vector.
    std::uint32_t active_bfs_node{std::numeric_limits<std::uint32_t>::max()};
    std::size_t active_bfs_vector_size{0};
    std::size_t active_bfs_vector_capacity{0};
    const void* active_bfs_vector_data{nullptr};
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

// Format vector storage details so the trace can show whether a push_back on
// the active BFS node changed size, capacity, or the underlying data pointer.
std::string format_vector_storage(const std::vector<std::uint32_t>& values) {
    std::ostringstream oss;
    oss << "size=" << values.size()
        << " capacity=" << values.capacity()
        << " data=" << static_cast<const void*>(values.data());
    return oss.str();
}

// Emit a one-line summary of the active BFS adjacency vector before or after a
// community load so iterator invalidation can be correlated with the crash.
void log_active_bfs_vector_state(const NeighborhoodState& state, std::string_view context) {
    if (!gfaidx::debug::subgraph_trace_enabled()) {
        return;
    }
    if (state.active_bfs_node == std::numeric_limits<std::uint32_t>::max()) {
        return;
    }
    const auto it = state.adjacency.find(state.active_bfs_node);
    if (it == state.adjacency.end()) {
        std::ostringstream oss;
        oss << context << " active_bfs_node=" << state.active_bfs_node
            << " has no adjacency entry";
        gfaidx::debug::log_subgraph_trace(oss.str());
        return;
    }
    std::ostringstream oss;
    oss << context << " active_bfs_node=" << state.active_bfs_node
        << " " << format_vector_storage(it->second);
    gfaidx::debug::log_subgraph_trace(oss.str());
}

// Build contextual error text around the exact get_subgraph step that was
// running so the large-graph repro can report where a rank or parse failure
// first surfaced.
std::runtime_error annotate_get_subgraph_error(std::string_view context,
                                               const std::exception& err) {
    return std::runtime_error(std::string(context) + ": " + err.what());
}

// Include the current .ndx size in rank checks so out-of-range values can be
// diagnosed before they are used to address the mmap-backed node index.
void log_rank_if_suspicious(const indexer::NodeHashIndex& node_index,
                            std::uint32_t rank,
                            std::string_view context) {
    if (!gfaidx::debug::subgraph_trace_enabled()) {
        return;
    }
    if (static_cast<std::uint64_t>(rank) < node_index.size()) {
        return;
    }
    std::ostringstream oss;
    oss << context << " produced suspicious rank " << rank
        << " while .ndx contains " << node_index.size() << " entries";
    gfaidx::debug::log_subgraph_trace(oss.str());
}

// Wrap rank->community resolution so failures from NodeHashIndex carry the
// queue/chunk context from get_subgraph instead of only the low-level message.
std::uint32_t community_id_by_rank_checked(const indexer::NodeHashIndex& node_index,
                                           std::uint32_t rank,
                                           std::string_view context) {
    log_rank_if_suspicious(node_index, rank, context);
    try {
        return node_index.community_id_by_rank(rank);
    } catch (const std::exception& err) {
        throw annotate_get_subgraph_error(context, err);
    }
}

std::string infer_companion_path(const std::string& input_gz, std::string_view suffix) {
    return input_gz + std::string(suffix);
}

std::string extract_s_node_id_only(std::string_view line) {
    const auto t1 = line.find('\t');
    if (t1 == std::string_view::npos) offending_line(line);
    const auto t2 = line.find('\t', t1 + 1);
    if (t2 == std::string_view::npos) offending_line(line);
    return std::string(line.substr(t1 + 1, t2 - (t1 + 1)));
}

std::uint32_t resolve_node_rank(const indexer::NodeHashIndex& node_index,
                                std::unordered_map<std::string, std::uint32_t>& cache,
                                std::string_view node_name,
                                std::string_view context) {
    std::string key(node_name);
    const auto it = cache.find(key);
    if (it != cache.end()) {
        // Cache hits are common, so only emit them when the temporary trace is
        // explicitly enabled for a large-graph debugging run.
        if (gfaidx::debug::subgraph_trace_enabled()) {
            std::ostringstream oss;
            oss << context << " resolved cached node '" << key
                << "' to rank " << it->second;
            gfaidx::debug::log_subgraph_trace(oss.str());
        }
        return it->second;
    }

    std::uint32_t rank = 0;
    if (!node_index.lookup_rank(key, rank)) {
        std::ostringstream oss;
        oss << context << " could not find node '" << key << "' in .ndx";
        throw std::runtime_error(oss.str());
    }
    // Log cache misses because they show exactly which line first introduced a
    // new node id into the BFS/shared-edge working set.
    if (gfaidx::debug::subgraph_trace_enabled()) {
        std::ostringstream oss;
        oss << context << " resolved node '" << key << "' to rank " << rank;
        gfaidx::debug::log_subgraph_trace(oss.str());
    }
    log_rank_if_suspicious(node_index, rank, context);
    cache.emplace(std::move(key), rank);
    return rank;
}

void add_undirected_edge(NeighborhoodState& state,
                         std::uint32_t left,
                         std::uint32_t right,
                         std::string_view context) {
    auto append_one = [&](std::uint32_t owner, std::uint32_t neighbor) {

        auto& vec = state.adjacency[owner];
        const auto before_size = vec.size();
        const auto before_capacity = vec.capacity();
        const void* before_data = static_cast<const void*>(vec.data());
        const bool touches_active_bfs_node = owner == state.active_bfs_node;
        // Log mutations to the exact vector BFS is currently iterating because
        // even a push_back without reallocation can invalidate the stored end().
        if (gfaidx::debug::subgraph_trace_enabled() && touches_active_bfs_node) {
            std::ostringstream oss;
            oss << context << " mutating active BFS vector for node " << owner
                << " by appending neighbor " << neighbor
                << " before " << format_vector_storage(vec);
            gfaidx::debug::log_subgraph_trace(oss.str());
        }
        vec.push_back(neighbor);
        if (gfaidx::debug::subgraph_trace_enabled() && touches_active_bfs_node) {
            const void* after_data = static_cast<const void*>(vec.data());
            std::ostringstream oss;
            oss << context << " mutated active BFS vector for node " << owner
                << " after " << format_vector_storage(vec)
                << " size_changed=" << (vec.size() != before_size)
                << " capacity_changed=" << (vec.capacity() != before_capacity)
                << " data_changed=" << (after_data != before_data);
            gfaidx::debug::log_subgraph_trace(oss.str());
        }
    };

    append_one(left, right);
    if (left != right) {
        append_one(right, left);
    }
}

std::uint32_t parse_required_u32(const std::string& value, const char* flag_name) {
    try {
        const auto parsed = std::stoull(value);
        if (parsed > std::numeric_limits<std::uint32_t>::max()) {
            throw std::out_of_range("value does not fit in uint32_t");
        }
        return static_cast<std::uint32_t>(parsed);
        if (parsed == 0) {
            throw std::runtime_error("--max_nodes must be greater than zero");
        }
    } catch (const std::exception& err) {
        throw std::runtime_error(std::string("Invalid value for ") + flag_name + ": " + err.what());
    }
}

ResolvedIndexPaths resolve_index_paths(const argparse::ArgumentParser& program,
                                       const std::string& input_gz) {
    ResolvedIndexPaths paths;
    paths.idx_path = program.get<std::string>("idx");
    paths.ndx_path = program.get<std::string>("ndx");
    paths.pdx_path = program.get<std::string>("pdx");
    paths.pdx_explicit = !paths.pdx_path.empty();

    // inferred because it's usually just attached to the end of the input graph file
    if (paths.idx_path.empty()) {
        paths.idx_path = infer_companion_path(input_gz, ".idx");
    }
    if (paths.ndx_path.empty()) {
        paths.ndx_path = infer_companion_path(input_gz, ".ndx");
    }
    if (paths.pdx_path.empty()) {
        paths.pdx_path = infer_companion_path(input_gz, ".pdx");
    }

    if (!file_exists(paths.idx_path.c_str())) {
        throw std::runtime_error("Index file does not exist: " + paths.idx_path);
    }
    if (!file_exists(paths.ndx_path.c_str())) {
        throw std::runtime_error("Node index file does not exist: " + paths.ndx_path);
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

// Load the L-line adjacency for one community on demand. Shared-edge lines are
// rescanned lazily when needed so BFS can cross community boundaries. NEEDS CHANGING
void load_community_adjacency(NeighborhoodState& state, std::uint32_t community_id) {
    if (community_id >= state.spans.size()) {
        throw std::runtime_error("Community id out of range in .idx: " + std::to_string(community_id));
    }
    if (state.loaded_communities[community_id] != 0) {
        return;
    }

    state.loaded_communities[community_id] = 1;
    state.touched_communities.push_back(community_id);
    info_get_subgraph("Loading adjacency for community " + std::to_string(community_id) +
                      " (" + std::to_string(state.touched_communities.size()) +
                      " communities touched so far)");
    // Record the span metadata up front so a later failure can be tied to the
    // exact gzip member being scanned on the large external graph.
    if (gfaidx::debug::subgraph_trace_enabled()) {
        std::ostringstream oss;
        oss << "Preparing adjacency load for community " << community_id
            << " using chunk span offset=" << state.spans[community_id].gz_offset
            << " size=" << state.spans[community_id].gz_size
            << " shared_chunk_id=" << state.shared_chunk_id;
        gfaidx::debug::log_subgraph_trace(oss.str());
    }

    const auto span = state.spans[community_id];
    if (span.gz_size > 0) {
        std::uint64_t local_edge_lines = 0;
        stream_community_lines_from_gz_range(
            state.gz_path,
            span.gz_offset,
            span.gz_size,
            [&](const std::string& line) -> bool {
                if (line.empty() || line[0] != 'L') return true;
                ++local_edge_lines;
                // Emit coarse progress through the member so the last completed
                // line count is visible if the run aborts mid-community.
                if (gfaidx::debug::subgraph_trace_enabled() && local_edge_lines % 100000 == 0) {
                    std::ostringstream oss;
                    oss << "Scanned " << local_edge_lines
                        << " local L lines for community " << community_id;
                    gfaidx::debug::log_subgraph_trace(oss.str());
                }
                try {
                    auto [left_name, right_name] = extract_L_nodes(line);
                    std::ostringstream left_context;
                    left_context << "community " << community_id
                                 << " local edge line " << local_edge_lines
                                 << " left endpoint";
                    const auto left_id = resolve_node_rank(state.node_index,
                                                           state.node_id_cache,
                                                           left_name,
                                                           left_context.str());
                    std::ostringstream right_context;
                    right_context << "community " << community_id
                                  << " local edge line " << local_edge_lines
                                  << " right endpoint";
                    const auto right_id = resolve_node_rank(state.node_index,
                                                            state.node_id_cache,
                                                            right_name,
                                                            right_context.str());
                    add_undirected_edge(state,
                                        left_id,
                                        right_id,
                                        "local edge insertion");
                } catch (const std::exception& err) {
                    std::ostringstream oss;
                    oss << "While processing local L line " << local_edge_lines
                        << " for community " << community_id
                        << " from span offset=" << span.gz_offset
                        << " size=" << span.gz_size
                        << " line='" << line << "'";
                    throw annotate_get_subgraph_error(oss.str(), err);
                }
                return true;
            });
        // Summarize the member scan once it completes so the large-graph log
        // shows whether the failure happened before or after the local edges.
        if (gfaidx::debug::subgraph_trace_enabled()) {
            std::ostringstream oss;
            oss << "Finished local adjacency scan for community " << community_id
                << " after " << local_edge_lines << " L lines";
            gfaidx::debug::log_subgraph_trace(oss.str());
        }
    }

    if (state.shared_chunk_id < state.spans.size() && state.shared_chunk_id != community_id) {
        const auto shared_span = state.spans[state.shared_chunk_id];
        if (shared_span.gz_size > 0) {
            std::uint64_t shared_edge_lines = 0;
            std::uint64_t shared_edge_matches = 0;
            // TODO: rescanning the shared-edge member for every newly loaded
            // community is simple and acceptable for small neighborhood queries,
            // but can be replaced later with a shared-edge cache if needed.
            stream_community_lines_from_gz_range(
                state.gz_path,
                shared_span.gz_offset,
                shared_span.gz_size,
                [&](const std::string& line) -> bool {
                    if (line.empty() || line[0] != 'L') return true;
                    ++shared_edge_lines;
                    // Shared-edge scans are the suspected hot path, so keep a
                    // periodic heartbeat with the current line count.
                    if (gfaidx::debug::subgraph_trace_enabled() && shared_edge_lines % 100000 == 0) {
                        std::ostringstream oss;
                        oss << "Scanned " << shared_edge_lines
                            << " shared L lines while loading community "
                            << community_id;
                        gfaidx::debug::log_subgraph_trace(oss.str());
                    }
                    try {
                        auto [left_name, right_name] = extract_L_nodes(line);
                        std::ostringstream left_context;
                        left_context << "community " << community_id
                                     << " shared edge line " << shared_edge_lines
                                     << " left endpoint";
                        const auto left_id = resolve_node_rank(state.node_index,
                                                               state.node_id_cache,
                                                               left_name,
                                                               left_context.str());
                        std::ostringstream right_context;
                        right_context << "community " << community_id
                                      << " shared edge line " << shared_edge_lines
                                      << " right endpoint";
                        const auto right_id = resolve_node_rank(state.node_index,
                                                                state.node_id_cache,
                                                                right_name,
                                                                right_context.str());
                        std::ostringstream left_rank_context;
                        left_rank_context << "community " << community_id
                                          << " shared edge line " << shared_edge_lines
                                          << " left rank=" << left_id;
                        const auto left_comm = community_id_by_rank_checked(state.node_index,
                                                                            left_id,
                                                                            left_rank_context.str());
                        std::ostringstream right_rank_context;
                        right_rank_context << "community " << community_id
                                           << " shared edge line " << shared_edge_lines
                                           << " right rank=" << right_id;
                        const auto right_comm = community_id_by_rank_checked(state.node_index,
                                                                             right_id,
                                                                             right_rank_context.str());
                        if (left_comm == community_id || right_comm == community_id) {
                            ++shared_edge_matches;
                            add_undirected_edge(state,
                                                left_id,
                                                right_id,
                                                "shared edge insertion");
                        }
                    } catch (const std::exception& err) {
                        std::ostringstream oss;
                        oss << "While processing shared L line " << shared_edge_lines
                            << " for community " << community_id
                            << " from shared span offset=" << shared_span.gz_offset
                            << " size=" << shared_span.gz_size
                            << " line='" << line << "'";
                        throw annotate_get_subgraph_error(oss.str(), err);
                    }
                    return true;
                });
            // Log both the scan size and the number of matching cross-community
            // edges so the shared-member pass can be compared between runs.
            if (gfaidx::debug::subgraph_trace_enabled()) {
                std::ostringstream oss;
                oss << "Finished shared adjacency scan for community " << community_id
                    << " after " << shared_edge_lines << " L lines with "
                    << shared_edge_matches << " matches";
                gfaidx::debug::log_subgraph_trace(oss.str());
            }
        }
    }
}

std::vector<std::uint32_t> bfs_collect_node_ids(NeighborhoodState& state,
                                                std::uint32_t start_node_id,
                                                std::uint32_t max_nodes) {
    std::deque<std::uint32_t> queue;
    std::unordered_set<std::uint32_t> discovered;
    discovered.reserve(max_nodes * 2);

    // Resolve the seed node community through the same checked helper used for
    // BFS expansion so any out-of-range rank carries the start-node context.
    {
        std::ostringstream oss;
        oss << "start node rank=" << start_node_id;
        std::uint32_t comm_id = community_id_by_rank_checked(state.node_index, start_node_id, oss.str());
        load_community_adjacency(state, comm_id);
    }
    queue.push_back(start_node_id);
    discovered.insert(start_node_id);

    while (!queue.empty() && discovered.size() < max_nodes) {
        const auto current = queue.front();
        queue.pop_front();

        const auto it = state.adjacency.find(current);
        if (it == state.adjacency.end()) {
            continue;
        }

        // Snapshot the vector BFS is about to iterate so later logs can prove
        // whether a community load mutated this same vector mid-iteration.
        state.active_bfs_node = current;
        state.active_bfs_vector_size = it->second.size();
        state.active_bfs_vector_capacity = it->second.capacity();
        state.active_bfs_vector_data = static_cast<const void*>(it->second.data());
        log_active_bfs_vector_state(state, "Starting BFS adjacency iteration");

        for (const auto neighbor : it->second) {
            if (discovered.size() >= max_nodes) break;

            if (discovered.insert(neighbor).second) {
                queue.push_back(neighbor);
                try {
                    std::ostringstream oss;
                    oss << "neighbor rank=" << neighbor
                        << " discovered_from=" << current
                        << " discovered_size=" << discovered.size()
                        << " queue_size=" << queue.size();
                    std::uint32_t comm_id =  community_id_by_rank_checked(state.node_index, neighbor, oss.str());
                    load_community_adjacency(state, comm_id);
                    // Compare the current adjacency vector after loading a new
                    // community so the trace can show if the active loop range changed.
                    if (gfaidx::debug::subgraph_trace_enabled()) {
                        const auto current_it = state.adjacency.find(current);
                        if (current_it != state.adjacency.end()) {
                            const bool size_changed =
                                current_it->second.size() != state.active_bfs_vector_size;
                            const bool capacity_changed =
                                current_it->second.capacity() != state.active_bfs_vector_capacity;
                            const bool data_changed =
                                static_cast<const void*>(current_it->second.data()) !=
                                state.active_bfs_vector_data;
                            if (size_changed || capacity_changed || data_changed) {
                                std::ostringstream mutation_oss;
                                mutation_oss << "Active BFS vector changed while iterating node "
                                             << current
                                             << " after loading neighbor " << neighbor
                                             << " before size=" << state.active_bfs_vector_size
                                             << " capacity=" << state.active_bfs_vector_capacity
                                             << " data=" << state.active_bfs_vector_data
                                             << " after " << format_vector_storage(current_it->second)
                                             << " size_changed=" << size_changed
                                             << " capacity_changed=" << capacity_changed
                                             << " data_changed=" << data_changed;
                                gfaidx::debug::log_subgraph_trace(mutation_oss.str());
                            }
                        }
                    }
                } catch (const std::exception& err) {
                    std::ostringstream oss;
                    oss << "While expanding BFS from rank " << current
                        << " to neighbor rank " << neighbor;
                    throw annotate_get_subgraph_error(oss.str(), err);
                }
                if (discovered.size() % 500 == 0) {
                    std::cerr << get_time() << ": BFS neighborhood currently has "
                              << discovered.size() << " nodes across "
                              << state.touched_communities.size() << " loaded communities"
                              << std::endl;
                }
            }
        }

        // Clear the active-vector snapshot once BFS moves on to the next node
        // so later adjacency insertions are not misattributed to this iteration.
        state.active_bfs_node = std::numeric_limits<std::uint32_t>::max();
        state.active_bfs_vector_size = 0;
        state.active_bfs_vector_capacity = 0;
        state.active_bfs_vector_data = nullptr;
    }

    std::vector<std::uint32_t> node_ids(discovered.begin(), discovered.end());
    std::sort(node_ids.begin(), node_ids.end());
    return node_ids;
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
// nodes are part of the final extracted node set.
EmissionStats emit_filtered_member(std::ostream& out,
                          const std::string& gz_path,
                          const CommunitySpan& span,
                          const std::unordered_set<std::uint32_t>& node_set,
                          indexer::NodeHashIndex& node_index,
                          std::unordered_map<std::string, std::uint32_t>& cache) {
    EmissionStats stats{};
    if (span.gz_size == 0) return stats;

    stream_community_lines_from_gz_range(
        gz_path,
        span.gz_offset,
        span.gz_size,
        [&](const std::string& line) -> bool {
            if (line.empty() || line[0] == 'H') return true;

            if (line[0] == 'S') {
                // Carry materialization context into node-rank resolution so a
                // late failure can still identify the member being emitted.
                const auto node_id = resolve_node_rank(node_index,
                                                       cache,
                                                       extract_s_node_id_only(line),
                                                       "materializing S line");
                if (node_set.find(node_id) != node_set.end()) {
                    out << line << '\n';
                    ++stats.s_lines;
                }
                return true;
            }

            if (line[0] == 'L') {
                auto [left_name, right_name] = extract_L_nodes(line);
                // Reuse the same contextual resolver during final replay so the
                // debug run can distinguish BFS failures from emit-time ones.
                const auto left_id = resolve_node_rank(node_index,
                                                       cache,
                                                       left_name,
                                                       "materializing L line left endpoint");
                const auto right_id = resolve_node_rank(node_index,
                                                        cache,
                                                        right_name,
                                                        "materializing L line right endpoint");
                if (node_set.find(left_id) != node_set.end() &&
                    node_set.find(right_id) != node_set.end()) {
                    out << line << '\n';
                    ++stats.l_lines;
                }
            }
            return true;
        });
    return stats;
}

// Append indexed P/W subpaths for the extracted node set when a companion
// .pdx exists. This keeps graph extraction and path extraction in one command.
std::uint64_t emit_subpaths_if_available(std::ostream& out,
                                const std::string& pdx_path,
                                const std::vector<std::uint32_t>& node_ids) {
    paths::PathIndexReader index(pdx_path);
    const auto runs = paths::find_subpaths_for_node_ids(index, node_ids);
    if (runs.empty()) {
        warn_get_subgraph("No indexed P/W subpaths overlapped the extracted node set");
        return 0;
    }

    std::uint64_t emitted = 0;
    for (const auto& run : runs) {
        const auto info = index.get_path_info(run.path_id);
        const auto base_name = (info.record_type == 'W') ? info.seq_id : info.name;
        const std::string subpath_name = std::string(base_name) + "#subpath_" +
            std::to_string(run.start_step) + "_" +
            std::to_string(run.start_step + run.step_count - 1);
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

    parser.add_argument("--debug_trace").default_value(false)
      .implicit_value(true)
      .help("enable temporary cross-file tracing for get_subgraph debugging");
}

int run_get_subgraph(const argparse::ArgumentParser& program) {
    const auto input_gz = program.get<std::string>("in_gz");
    if (!file_exists(input_gz.c_str())) {
        std::cerr << "Input file does not exist: " << input_gz << std::endl;
        return 1;
    }

    const auto start_node = program.get<std::string>("start_node");
    const auto output_gfa = program.get<std::string>("out_gfa");
    const auto max_nodes_str = program.get<std::string>("max_nodes");
    const auto debug_trace = program.get<bool>("debug_trace");

    try {
        // Export the trace flag before opening indexes so helper code in other
        // translation units can participate in the same debug run.
        if (debug_trace) {
            ::setenv("GFAIDX_DEBUG_SUBGRAPH", "1", 1);
            debug::log_subgraph_trace("Enabled temporary get_subgraph trace logging");
        }
        const auto index_paths = resolve_index_paths(program, input_gz);
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
        std::unordered_map<std::string, std::uint32_t> node_id_cache;
        std::vector<std::uint32_t> node_ids;
        std::vector<std::uint32_t> touched_communities;

        const auto max_nodes = parse_required_u32(max_nodes_str, "--max_nodes");

        std::uint32_t start_node_id = 0;
        if (!node_index.lookup_rank(start_node, start_node_id)) {
            throw std::runtime_error("Start node was not found in .ndx: " + start_node);
        }
        // Log the seed rank and node-index size up front so later failures can
        // be compared against the original start-node resolution.
        if (debug::subgraph_trace_enabled()) {
            std::ostringstream oss;
            oss << "Start node '" << start_node << "' resolved to rank "
                << start_node_id << " with .ndx size " << node_index.size();
            debug::log_subgraph_trace(oss.str());
        }

        std::ofstream out(output_gfa);
        if (!out) {
            throw std::runtime_error("Failed to open output GFA file for writing: " + output_gfa);
        }

        NeighborhoodState state{
            input_gz,
            spans,
            node_index,
            spans.size() >= 2 ? static_cast<std::uint32_t>(spans.size() - 1)
                              : std::numeric_limits<std::uint32_t>::max(),
            {},
            {},
            std::vector<std::uint8_t>(spans.size(), 0),
            {}
        };

        info_get_subgraph("Starting BFS from node " + start_node +
                          " with max_nodes=" + std::to_string(max_nodes));
        node_ids = bfs_collect_node_ids(state, start_node_id, max_nodes);
        touched_communities = std::move(state.touched_communities);
        node_id_cache = std::move(state.node_id_cache);
        info_get_subgraph("BFS finished with " + std::to_string(node_ids.size()) +
                          " nodes across " + std::to_string(touched_communities.size()) +
                          " touched communities");

        if (node_ids.empty()) {
            warn_get_subgraph("No nodes were selected for the requested subgraph");
            return 0;
        }

        std::sort(touched_communities.begin(), touched_communities.end());
        touched_communities.erase(std::unique(touched_communities.begin(), touched_communities.end()),
                                  touched_communities.end());

        std::unordered_set<std::uint32_t> node_set(node_ids.begin(), node_ids.end());

        info_get_subgraph("Starting subgraph materialization into " + output_gfa);
        emit_header_if_present(out, input_gz, spans);
        EmissionStats total_stats{};
        for (const auto community_id : touched_communities) {
            info_get_subgraph("Materializing community " + std::to_string(community_id));
            const auto stats = emit_filtered_member(out, input_gz, spans[community_id], node_set,
                                                    node_index, node_id_cache);
            total_stats.s_lines += stats.s_lines;
            total_stats.l_lines += stats.l_lines;
        }

        if (spans.size() >= 2) {
            const auto shared_chunk_id = static_cast<std::uint32_t>(spans.size() - 1);
            if (std::find(touched_communities.begin(), touched_communities.end(), shared_chunk_id) ==
                touched_communities.end()) {
                info_get_subgraph("Materializing shared-edge member " + std::to_string(shared_chunk_id));
                const auto stats = emit_filtered_member(out, input_gz, spans[shared_chunk_id], node_set,
                                                        node_index, node_id_cache);
                total_stats.s_lines += stats.s_lines;
                total_stats.l_lines += stats.l_lines;
            }
        }
        info_get_subgraph("Finished subgraph materialization with " +
                          std::to_string(total_stats.s_lines) + " S lines and " +
                          std::to_string(total_stats.l_lines) + " L lines");

        if (index_paths.has_pdx) {
            info_get_subgraph("Starting indexed subpath extraction from " + index_paths.pdx_path);
            const auto subpath_count = emit_subpaths_if_available(out, index_paths.pdx_path, node_ids);
            info_get_subgraph("Finished indexed subpath extraction with " +
                              std::to_string(subpath_count) + " P/W records");
        }
        info_get_subgraph("Finished writing extracted subgraph to " + output_gfa);
    } catch (const std::exception& err) {
        std::cerr << err.what() << std::endl;
        return 1;
    }

    return 0;
}

}  // namespace gfaidx::chunk

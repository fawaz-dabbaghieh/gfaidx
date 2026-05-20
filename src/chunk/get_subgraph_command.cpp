#include "chunk/get_subgraph_command.h"

#include <algorithm>
#include <cstdint>
#include <deque>
#include <fstream>
#include <iostream>
#include <limits>
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
                                std::string_view node_name) {
    std::string key(node_name);
    const auto it = cache.find(key);
    if (it != cache.end()) {
        return it->second;
    }

    std::uint32_t rank = 0;
    if (!node_index.lookup_rank(key, rank)) {
        throw std::runtime_error("Node id not found in .ndx: " + key);
    }
    cache.emplace(std::move(key), rank);
    return rank;
}

void add_undirected_edge(std::unordered_map<std::uint32_t, std::vector<std::uint32_t>>& adjacency,
                         std::uint32_t left,
                         std::uint32_t right) {
    adjacency[left].push_back(right);
    if (left != right) {
        adjacency[right].push_back(left);
    }
}

std::uint32_t parse_required_u32(const std::string& value, const char* flag_name) {
    try {
        const auto parsed = std::stoull(value);
        if (parsed > std::numeric_limits<std::uint32_t>::max()) {
            throw std::out_of_range("value does not fit in uint32_t");
        }
        return static_cast<std::uint32_t>(parsed);
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
// rescanned lazily when needed so BFS can cross community boundaries.
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

    const auto span = state.spans[community_id];
    if (span.gz_size > 0) {
        stream_community_lines_from_gz_range(
            state.gz_path,
            span.gz_offset,
            span.gz_size,
            [&](const std::string& line) -> bool {
                if (line.empty() || line[0] != 'L') return true;

                auto [left_name, right_name] = extract_L_nodes(line);
                const auto left_id = resolve_node_rank(state.node_index, state.node_id_cache, left_name);
                const auto right_id = resolve_node_rank(state.node_index, state.node_id_cache, right_name);
                add_undirected_edge(state.adjacency, left_id, right_id);
                return true;
            });
    }

    if (state.shared_chunk_id < state.spans.size() && state.shared_chunk_id != community_id) {
        const auto shared_span = state.spans[state.shared_chunk_id];
        if (shared_span.gz_size > 0) {
            // TODO: rescanning the shared-edge member for every newly loaded
            // community is simple and acceptable for small neighborhood queries,
            // but can be replaced later with a shared-edge cache if needed.
            stream_community_lines_from_gz_range(
                state.gz_path,
                shared_span.gz_offset,
                shared_span.gz_size,
                [&](const std::string& line) -> bool {
                    if (line.empty() || line[0] != 'L') return true;

                    auto [left_name, right_name] = extract_L_nodes(line);
                    const auto left_id = resolve_node_rank(state.node_index, state.node_id_cache, left_name);
                    const auto right_id = resolve_node_rank(state.node_index, state.node_id_cache, right_name);
                    if (state.node_index.community_id_by_rank(left_id) == community_id ||
                        state.node_index.community_id_by_rank(right_id) == community_id) {
                        add_undirected_edge(state.adjacency, left_id, right_id);
                    }
                    return true;
                });
        }
    }
}

std::vector<std::uint32_t> bfs_collect_node_ids(NeighborhoodState& state,
                                                std::uint32_t start_node_id,
                                                std::uint32_t max_nodes) {
    std::deque<std::uint32_t> queue;
    std::unordered_set<std::uint32_t> discovered;
    discovered.reserve(max_nodes * 2);

    load_community_adjacency(state, state.node_index.community_id_by_rank(start_node_id));
    queue.push_back(start_node_id);
    discovered.insert(start_node_id);

    while (!queue.empty() && discovered.size() < max_nodes) {
        const auto current = queue.front();
        queue.pop_front();

        const auto it = state.adjacency.find(current);
        if (it == state.adjacency.end()) {
            continue;
        }

        for (const auto neighbor : it->second) {
            if (discovered.size() >= max_nodes) break;

            if (discovered.insert(neighbor).second) {
                queue.push_back(neighbor);
                load_community_adjacency(state, state.node_index.community_id_by_rank(neighbor));
                if (discovered.size() % 500 == 0) {
                    std::cerr << get_time() << ": BFS neighborhood currently has "
                              << discovered.size() << " nodes across "
                              << state.touched_communities.size() << " loaded communities"
                              << std::endl;
                }
            }
        }
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
                const auto node_id = resolve_node_rank(node_index, cache, extract_s_node_id_only(line));
                if (node_set.find(node_id) != node_set.end()) {
                    out << line << '\n';
                    ++stats.s_lines;
                }
                return true;
            }

            if (line[0] == 'L') {
                auto [left_name, right_name] = extract_L_nodes(line);
                const auto left_id = resolve_node_rank(node_index, cache, left_name);
                const auto right_id = resolve_node_rank(node_index, cache, right_name);
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

    try {
        const auto index_paths = resolve_index_paths(program, input_gz);
        const auto spans = load_all_community_spans_tsv(index_paths.idx_path);
        if (spans.empty()) {
            throw std::runtime_error("The .idx file does not contain any community spans");
        }

        indexer::NodeHashIndex node_index(index_paths.ndx_path);
        std::unordered_map<std::string, std::uint32_t> node_id_cache;
        std::vector<std::uint32_t> node_ids;
        std::vector<std::uint32_t> touched_communities;

        const auto max_nodes = parse_required_u32(max_nodes_str, "--max_nodes");
        if (max_nodes == 0) {
            throw std::runtime_error("--max_nodes must be greater than zero");
        }

        std::uint32_t start_node_id = 0;
        if (!node_index.lookup_rank(start_node, start_node_id)) {
            throw std::runtime_error("Start node was not found in .ndx: " + start_node);
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

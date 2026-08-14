#include "gfaidx/indexed_graph.hpp"

#include <algorithm>
#include <condition_variable>
#include <deque>
#include <exception>
#include <filesystem>
#include <limits>
#include <memory>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "chunk/chunk_reader.h"
#include "coordinates/coordinate_index.h"
#include "coordinates/path_coordinate_query.h"
#include "coordinates/path_haplotype_query.h"
#include "indexer/node_hash_index.h"
#include "indexer/node_length_index.h"
#include "paths/path_index.h"
#include "paths/walk_coords.h"

namespace gfaidx {
namespace {

namespace fs = std::filesystem;

bool exists(const std::string& path) {
    return !path.empty() && fs::exists(path);
}

std::string companion(const std::string& graph_path, const char* suffix) {
    return graph_path + suffix;
}

void require_continue(const QueryCallbacks& callbacks) {
    if (callbacks.keep_going && !callbacks.keep_going()) {
        throw std::runtime_error("gfaidx query was cancelled");
    }
}

void warn(const QueryCallbacks& callbacks, const std::string& message) {
    if (callbacks.warning) callbacks.warning(message);
}

std::vector<std::string_view> fields(std::string_view line) {
    std::vector<std::string_view> result;
    for (std::size_t begin = 0;;) {
        const auto end = line.find('\t', begin);
        result.push_back(line.substr(begin, end - begin));
        if (end == std::string_view::npos) break;
        begin = end + 1;
    }
    return result;
}

std::pair<std::string, std::string> link_nodes(std::string_view line) {
    const auto f = fields(line);
    if (f.size() < 6 || f[0] != "L") throw std::runtime_error("Malformed L record");
    return {std::string(f[1]), std::string(f[3])};
}

std::string segment_name(std::string_view line) {
    const auto f = fields(line);
    if (f.size() < 3 || f[0] != "S") throw std::runtime_error("Malformed S record");
    return std::string(f[1]);
}

Node parse_node_line(const std::string& line) {
    std::istringstream in(line + "\n");
    const auto graph = Graph::from_gfa(in);
    if (graph.node_count() != 1) throw std::runtime_error("Expected one S record");
    return graph.nodes().front();
}

Edge parse_edge_line(const std::string& line) {
    const auto f = fields(line);
    if (f.size() < 6) throw std::runtime_error("Malformed L record");
    Edge edge;
    edge.from = Endpoint{std::string(f[1]), f[2] == "-"};
    edge.to = Endpoint{std::string(f[3]), f[4] == "-"};
    edge.overlap = std::string(f[5]);
    for (std::size_t i = 6; i < f.size(); ++i) edge.tags.emplace_back(f[i]);
    return edge;
}

std::string tags_string_to_field(std::string_view tags) {
    return std::string(tags);
}

std::string path_key(const paths::PathInfo& info) {
    return std::string(info.name);
}

PathDescriptor describe_path(const paths::PathInfo& info) {
    PathDescriptor result;
    result.id = info.path_id;
    result.record_type = info.record_type;
    result.key = path_key(info);
    result.name = info.record_type == 'P' ? std::string(info.name) : std::string{};
    result.sample = std::string(info.sample_id);
    result.haplotype = info.hap_index;
    result.sequence_name = std::string(info.seq_id);
    result.sequence_start = info.seq_start;
    result.sequence_end = info.seq_end;
    result.step_count = info.step_count;
    const auto raw_tags = tags_string_to_field(info.tags);
    if (!raw_tags.empty()) {
        const auto parsed = fields(raw_tags);
        for (const auto tag : parsed) if (!tag.empty()) result.tags.emplace_back(tag);
    }
    return result;
}

std::string subpath_name(const paths::PathInfo& info,
                         const paths::SubpathRun& run) {
    const auto base = info.record_type == 'W' ? info.seq_id : info.name;
    return std::string(base) + "#subpath_" + std::to_string(run.start_step) +
           "_" + std::to_string(run.start_step + run.step_count - 1);
}

struct SelectionData {
    std::vector<std::string> names;
    std::vector<std::uint32_t> ranks;
    std::vector<std::uint32_t> communities;
    std::vector<paths::SubpathRun> exact_runs;
    bool preserve_exact_runs{false};
};

}  // namespace

class IndexedGraph::Impl {
public:
    Impl(std::string graph, IndexPaths overrides)
        : graph_path(std::move(graph)) {
        if (!exists(graph_path)) throw std::runtime_error("Indexed graph does not exist: " + graph_path);
        paths.idx = overrides.idx.empty() ? companion(graph_path, ".idx") : std::move(overrides.idx);
        paths.ndx = overrides.ndx.empty() ? companion(graph_path, ".ndx") : std::move(overrides.ndx);
        paths.pdx = overrides.pdx.empty() ? companion(graph_path, ".pdx") : std::move(overrides.pdx);
        paths.lnx = overrides.lnx.empty() ? companion(graph_path, ".lnx") : std::move(overrides.lnx);
        paths.pcx = overrides.pcx.empty() ? companion(graph_path, ".pcx") : std::move(overrides.pcx);
        paths.cdx = overrides.cdx.empty() ? companion(graph_path, ".cdx") : std::move(overrides.cdx);
        if (!exists(paths.idx)) throw std::runtime_error("Chunk index does not exist: " + paths.idx);
        if (!exists(paths.ndx)) throw std::runtime_error("Node index does not exist: " + paths.ndx);
        spans = load_all_community_spans_tsv(paths.idx);
        if (spans.empty()) throw std::runtime_error("Chunk index contains no communities: " + paths.idx);
        node_index = std::make_unique<indexer::NodeHashIndex>(paths.ndx);
        capabilities.paths = exists(paths.pdx);
        capabilities.node_lengths = exists(paths.lnx);
        capabilities.path_checkpoints = exists(paths.pcx);
        capabilities.coordinates = exists(paths.cdx);
        if (capabilities.paths) {
            path_index = std::make_unique<paths::PathIndexReader>(paths.pdx);
            if (path_index->node_count() != node_index->size()) {
                throw std::runtime_error(".pdx and .ndx node counts differ");
            }
        }
        if (capabilities.coordinates) {
            coordinate_index = std::make_unique<coordinates::CoordinateIndexReader>(paths.cdx);
            if (coordinate_index->node_count() != node_index->size()) {
                throw std::runtime_error(".cdx and .ndx node counts differ");
            }
        }
    }

    [[nodiscard]] std::uint32_t shared_community() const {
        return spans.size() >= 2 ? static_cast<std::uint32_t>(spans.size() - 1)
                                 : std::numeric_limits<std::uint32_t>::max();
    }

    void stream_member(std::uint32_t id,
                       const std::function<bool(const std::string&)>& visitor) const {
        if (id >= spans.size()) throw std::out_of_range("Community id is out of range");
        stream_community_lines_from_gz_range(graph_path, spans[id].gz_offset,
                                             spans[id].gz_size, visitor);
    }

    SelectionData bfs(const std::vector<std::string>& seeds,
                      std::uint32_t max_nodes,
                      const QueryCallbacks& callbacks) const {
        if (seeds.empty()) throw std::invalid_argument("At least one seed node is required");
        if (max_nodes == 0) throw std::invalid_argument("max_nodes must be greater than zero");
        std::vector<std::string> unique = seeds;
        std::sort(unique.begin(), unique.end());
        unique.erase(std::unique(unique.begin(), unique.end()), unique.end());
        if (unique.size() > max_nodes) throw std::runtime_error("Seed count exceeds max_nodes");

        // Query-local caches make const IndexedGraph queries safe to run in
        // parallel without sharing mutable stream or adjacency state.
        std::unordered_map<std::uint32_t, std::vector<std::pair<std::string, std::string>>> community_edges;
        std::unordered_set<std::uint32_t> loaded;
        std::unordered_map<std::string, std::vector<std::string>> adjacency;

        auto load_community = [&](std::uint32_t community_id) {
            if (!loaded.insert(community_id).second) return;
            require_continue(callbacks);
            stream_member(community_id, [&](const std::string& line) {
                if (!line.empty() && line[0] == 'L') {
                    auto endpoints = link_nodes(line);
                    adjacency[endpoints.first].push_back(endpoints.second);
                    if (endpoints.first != endpoints.second) {
                        adjacency[endpoints.second].push_back(endpoints.first);
                    }
                }
                return true;
            });
        };

        // Cross-community links live in the final member.  They are indexed by
        // endpoint for this query, mirroring the current optimized CLI logic.
        const auto shared = shared_community();
        if (shared < spans.size()) {
            stream_member(shared, [&](const std::string& line) {
                if (!line.empty() && line[0] == 'L') {
                    auto endpoints = link_nodes(line);
                    adjacency[endpoints.first].push_back(endpoints.second);
                    if (endpoints.first != endpoints.second) adjacency[endpoints.second].push_back(endpoints.first);
                }
                return true;
            });
        }

        std::deque<std::string> queue(unique.begin(), unique.end());
        std::unordered_set<std::string> queued(unique.begin(), unique.end());
        std::unordered_set<std::string> selected;
        std::vector<std::string> ordered;
        ordered.reserve(max_nodes);
        while (!queue.empty() && ordered.size() < max_nodes) {
            require_continue(callbacks);
            auto current = std::move(queue.front());
            queue.pop_front();
            queued.erase(current);
            if (!selected.insert(current).second) continue;
            ordered.push_back(current);
            std::uint32_t community = 0;
            if (!node_index->lookup(current, community)) {
                throw std::out_of_range("Node does not exist: " + current);
            }
            load_community(community);
            for (const auto& neighbor : adjacency[current]) {
                if (selected.count(neighbor) == 0 && queued.insert(neighbor).second) {
                    queue.push_back(neighbor);
                }
            }
        }
        return selection_from_names(std::move(ordered));
    }

    SelectionData selection_from_names(std::vector<std::string> names) const {
        SelectionData result;
        result.names = std::move(names);
        std::unordered_set<std::uint32_t> communities;
        for (const auto& name : result.names) {
            std::uint32_t rank = 0;
            std::uint32_t community = 0;
            if (!node_index->lookup_rank(name, rank) || !node_index->lookup(name, community)) {
                throw std::out_of_range("Node does not exist: " + name);
            }
            result.ranks.push_back(rank);
            communities.insert(community);
        }
        result.communities.assign(communities.begin(), communities.end());
        std::sort(result.communities.begin(), result.communities.end());
        return result;
    }

    SelectionData selection_from_ranks(std::vector<std::uint32_t> ranks,
                                       std::vector<paths::SubpathRun> exact_runs = {}) const {
        if (!path_index) throw std::runtime_error("Rank selections require a path index");
        std::sort(ranks.begin(), ranks.end());
        ranks.erase(std::unique(ranks.begin(), ranks.end()), ranks.end());
        SelectionData result;
        result.ranks = std::move(ranks);
        result.exact_runs = std::move(exact_runs);
        result.preserve_exact_runs = !result.exact_runs.empty();
        std::unordered_set<std::uint32_t> communities;
        for (const auto rank : result.ranks) {
            if (rank >= path_index->node_count()) throw std::out_of_range("Node rank is out of range");
            result.names.push_back(path_index->copy_node_name(rank));
            communities.insert(node_index->community_id_by_rank(rank));
        }
        result.communities.assign(communities.begin(), communities.end());
        std::sort(result.communities.begin(), result.communities.end());
        return result;
    }

    void stream_selection(const SelectionData& selection,
                          const GfaLineVisitor& visitor,
                          const ExtractionOptions& options,
                          const QueryCallbacks& callbacks) const {
        if (!visitor) throw std::invalid_argument("A GFA line visitor is required");
        if (options.threads == 0 || options.threads > 256) {
            throw std::invalid_argument("threads must be between 1 and 256");
        }
        if (options.include_coordinates && !options.include_paths) {
            throw std::invalid_argument(
                "Coordinate-bearing output requires path output");
        }
        std::unordered_set<std::string> selected(selection.names.begin(), selection.names.end());
        bool continue_output = true;
        if (!spans.empty()) {
            stream_member(0, [&](const std::string& line) {
                if (!line.empty() && line[0] == 'H') continue_output = visitor(line);
                return continue_output && (line.empty() || line[0] == 'H');
            });
        }
        if (!continue_output) return;

        auto emit_member = [&](std::uint32_t community) {
            stream_member(community, [&](const std::string& line) {
                require_continue(callbacks);
                if (line.empty() || line[0] == 'H') return true;
                if (line[0] == 'S') {
                    if (selected.count(segment_name(line)) != 0) continue_output = visitor(line);
                } else if (line[0] == 'L') {
                    const auto endpoints = link_nodes(line);
                    if (selected.count(endpoints.first) && selected.count(endpoints.second)) {
                        continue_output = visitor(line);
                    }
                }
                return continue_output;
            });
        };
        for (const auto community : selection.communities) {
            if (!continue_output) return;
            emit_member(community);
        }
        const auto shared = shared_community();
        if (continue_output && shared < spans.size()) emit_member(shared);
        if (!continue_output || !options.include_paths) return;
        if (!path_index) {
            warn(callbacks, "No .pdx path index is available; returning S/L records only");
            return;
        }

        std::vector<paths::SubpathRun> discovered;
        const std::vector<paths::SubpathRun>* runs = &selection.exact_runs;
        if (!selection.preserve_exact_runs) {
            discovered = paths::find_subpaths_for_node_ids(*path_index, selection.ranks);
            runs = &discovered;
        }
        paths::SelectedNodeNameLookup names(path_index->node_count(), selection.ranks,
                                            selection.names);
        paths::WalkCoordState coordinates;
        if (options.include_coordinates && !runs->empty()) {
            coordinates = paths::load_node_lengths_by_index(
                *path_index, *node_index, graph_path,
                capabilities.node_lengths ? paths.lnx : std::string{},
                capabilities.path_checkpoints ? paths.pcx : std::string{},
                [&](const std::string& message) { warn(callbacks, message); });
        }
        // Format one interval with an explicitly supplied reader. Parallel
        // workers each own a reader because seek/read mutate std::ifstream
        // state even though the high-level query is logically const.
        const auto format_run = [&](paths::PathIndexReader& reader,
                                    const paths::SubpathRun& run,
                                    const std::function<void(const std::string&)>&
                                        warning) {
            const auto info = reader.get_path_info(run.path_id);
            std::string line;
            bool formatted = false;
            if (options.include_coordinates && coordinates.usable) {
                formatted = info.record_type == 'W'
                    ? paths::format_w_subpath_with_coords_bounded(line, reader, names,
                          run.path_id, coordinates, run.start_step, run.step_count, warning)
                    : paths::format_p_subpath_with_coords_bounded(line, reader, names,
                          run.path_id, coordinates, run.start_step, run.step_count, warning);
            }
            if (!formatted) {
                line = paths::format_subpath_as_gfa_line(reader, names, run.path_id,
                    run.start_step, run.step_count, subpath_name(info, run));
            }
            if (!line.empty() && line.back() == '\n') line.pop_back();
            return line;
        };

        if (options.threads == 1 || runs->size() < 2) {
            for (const auto& run : *runs) {
                require_continue(callbacks);
                auto line = format_run(*path_index, run,
                    [&](const std::string& message) { warn(callbacks, message); });
                if (!visitor(line)) return;
            }
            return;
        }

        // A one-slot-per-worker ring bounds formatted output memory while the
        // main thread drains results in original path-run order.
        struct Slot {
            std::size_t sequence{std::numeric_limits<std::size_t>::max()};
            bool ready{false};
            std::string line;
            std::vector<std::string> warnings;
            std::exception_ptr error;
        };
        const std::size_t worker_count =
            std::min<std::size_t>(options.threads, runs->size());
        std::vector<Slot> slots(worker_count);
        std::vector<std::thread> workers;
        workers.reserve(worker_count);
        std::mutex state_mutex;
        std::condition_variable work_available;
        std::condition_variable result_available;
        std::size_t next_job = 0;
        std::size_t next_output = 0;
        bool stop = false;
        std::exception_ptr startup_error;

        const auto stop_and_join = [&]() {
            {
                const std::lock_guard<std::mutex> lock(state_mutex);
                stop = true;
            }
            work_available.notify_all();
            result_available.notify_all();
            for (auto& worker : workers) if (worker.joinable()) worker.join();
        };

        try {
            for (std::size_t worker_id = 0; worker_id < worker_count; ++worker_id) {
                workers.emplace_back([&]() {
                    try {
                        paths::PathIndexReader reader(paths.pdx);
                        while (true) {
                            std::size_t sequence = 0;
                            {
                                std::unique_lock<std::mutex> lock(state_mutex);
                                work_available.wait(lock, [&]() {
                                    return stop || next_job >= runs->size() ||
                                           next_job < next_output + slots.size();
                                });
                                if (stop || next_job >= runs->size()) return;
                                sequence = next_job++;
                            }

                            Slot result;
                            result.sequence = sequence;
                            try {
                                result.line = format_run(
                                    reader, (*runs)[sequence],
                                    [&](const std::string& message) {
                                        result.warnings.push_back(message);
                                    });
                            } catch (...) {
                                result.error = std::current_exception();
                            }
                            const bool has_error =
                                static_cast<bool>(result.error);

                            {
                                const std::lock_guard<std::mutex> lock(state_mutex);
                                if (stop) return;
                                auto& slot = slots[sequence % slots.size()];
                                slot = std::move(result);
                                slot.ready = true;
                            }
                            result_available.notify_one();
                            if (has_error) return;
                        }
                    } catch (...) {
                        {
                            const std::lock_guard<std::mutex> lock(state_mutex);
                            if (!startup_error) startup_error = std::current_exception();
                            stop = true;
                        }
                        work_available.notify_all();
                        result_available.notify_all();
                    }
                });
            }

            while (next_output < runs->size()) {
                Slot result;
                {
                    std::unique_lock<std::mutex> lock(state_mutex);
                    result_available.wait(lock, [&]() {
                        const auto& slot = slots[next_output % slots.size()];
                        return startup_error ||
                               (slot.ready && slot.sequence == next_output);
                    });
                    if (startup_error) std::rethrow_exception(startup_error);
                    auto& slot = slots[next_output % slots.size()];
                    result = std::move(slot);
                    slot = Slot{};
                    ++next_output;
                }
                work_available.notify_one();
                if (result.error) std::rethrow_exception(result.error);
                for (const auto& message : result.warnings) warn(callbacks, message);
                require_continue(callbacks);
                if (!visitor(result.line)) {
                    stop_and_join();
                    return;
                }
            }
        } catch (...) {
            stop_and_join();
            throw;
        }
        stop_and_join();
    }

    Graph materialize(const SelectionData& selection,
                      const ExtractionOptions& options,
                      const QueryCallbacks& callbacks) const {
        std::ostringstream buffer;
        stream_selection(selection, [&](std::string_view line) {
            buffer.write(line.data(), static_cast<std::streamsize>(line.size()));
            buffer.put('\n');
            return true;
        }, options, callbacks);
        std::istringstream input(buffer.str());
        return Graph::from_gfa(input);
    }

    std::string graph_path;
    IndexPaths paths;
    Capabilities capabilities;
    std::vector<CommunitySpan> spans;
    std::unique_ptr<indexer::NodeHashIndex> node_index;
    std::unique_ptr<paths::PathIndexReader> path_index;
    std::unique_ptr<coordinates::CoordinateIndexReader> coordinate_index;
    // Several established readers use mutable std::ifstream state for seeks.
    // Serialize operations that touch those readers until they are converted
    // to positional I/O; mmap-only node lookups remain lock-free.
    mutable std::recursive_mutex reader_mutex;
};

IndexedGraph::IndexedGraph(std::string graph_path, IndexPaths overrides)
    : impl_(std::make_unique<Impl>(std::move(graph_path), std::move(overrides))) {}
IndexedGraph::~IndexedGraph() = default;
IndexedGraph::IndexedGraph(IndexedGraph&&) noexcept = default;
IndexedGraph& IndexedGraph::operator=(IndexedGraph&&) noexcept = default;

const std::string& IndexedGraph::graph_path() const noexcept { return impl_->graph_path; }
const IndexPaths& IndexedGraph::index_paths() const noexcept { return impl_->paths; }
Capabilities IndexedGraph::capabilities() const noexcept { return impl_->capabilities; }
std::uint64_t IndexedGraph::node_count() const noexcept { return impl_->node_index->size(); }
std::uint32_t IndexedGraph::community_count() const noexcept {
    const auto count = impl_->spans.size() - (impl_->spans.size() >= 2 ? 1 : 0);
    return static_cast<std::uint32_t>(count);
}

bool IndexedGraph::node_exists(std::string_view node_id) const noexcept {
    std::uint32_t ignored = 0;
    return impl_->node_index->lookup(node_id, ignored);
}

std::uint32_t IndexedGraph::community_id(std::string_view node_id) const {
    std::uint32_t result = 0;
    if (!impl_->node_index->lookup(node_id, result)) {
        throw std::out_of_range("Node does not exist: " + std::string(node_id));
    }
    return result;
}

Node IndexedGraph::get_node(std::string_view node_id) const {
    const auto community = community_id(node_id);
    std::optional<Node> result;
    impl_->stream_member(community, [&](const std::string& line) {
        if (!line.empty() && line[0] == 'S' && segment_name(line) == node_id) {
            result = parse_node_line(line);
            return false;
        }
        return true;
    });
    if (!result) throw std::runtime_error("Node index and graph disagree for " + std::string(node_id));
    return std::move(*result);
}

std::vector<Edge> IndexedGraph::incident_edges(std::string_view node_id) const {
    const auto community = community_id(node_id);
    std::vector<Edge> result;
    auto scan = [&](std::uint32_t id) {
        impl_->stream_member(id, [&](const std::string& line) {
            if (!line.empty() && line[0] == 'L') {
                auto edge = parse_edge_line(line);
                if (edge.from.node_id == node_id || edge.to.node_id == node_id) {
                    edge.id = result.size() + 1;
                    result.push_back(std::move(edge));
                }
            }
            return true;
        });
    };
    scan(community);
    const auto shared = impl_->shared_community();
    if (shared < impl_->spans.size()) scan(shared);
    return result;
}

std::vector<std::string> IndexedGraph::neighbors(std::string_view node_id) const {
    std::vector<std::string> result;
    std::unordered_set<std::string> seen;
    for (const auto& edge : incident_edges(node_id)) {
        const auto& other = edge.from.node_id == node_id ? edge.to.node_id : edge.from.node_id;
        if (seen.insert(other).second) result.push_back(other);
    }
    return result;
}

Graph IndexedGraph::get_community(std::uint32_t id) const {
    if (id >= community_count()) throw std::out_of_range("Community id is out of range");
    std::vector<std::string> names;
    impl_->stream_member(id, [&](const std::string& line) {
        if (!line.empty() && line[0] == 'S') names.push_back(segment_name(line));
        return true;
    });
    ExtractionOptions options;
    options.include_paths = false;
    return impl_->materialize(impl_->selection_from_names(std::move(names)), options, {});
}

std::vector<PathDescriptor> IndexedGraph::paths() const {
    const std::lock_guard<std::recursive_mutex> lock(impl_->reader_mutex);
    if (!impl_->path_index) return {};
    std::vector<PathDescriptor> result;
    result.reserve(impl_->path_index->path_count());
    for (std::uint32_t id = 0; id < impl_->path_index->path_count(); ++id) {
        result.push_back(describe_path(impl_->path_index->get_path_info(id)));
    }
    return result;
}

PathDescriptor IndexedGraph::get_path(std::string_view key) const {
    const std::lock_guard<std::recursive_mutex> lock(impl_->reader_mutex);
    if (!impl_->path_index) throw std::runtime_error("No .pdx path index is available");
    std::uint32_t id = 0;
    if (!impl_->path_index->lookup_path_id(std::string(key), id)) {
        throw std::out_of_range("Path does not exist: " + std::string(key));
    }
    return describe_path(impl_->path_index->get_path_info(id));
}

std::vector<CoordinateTrack> IndexedGraph::coordinate_tracks() const {
    const std::lock_guard<std::recursive_mutex> lock(impl_->reader_mutex);
    if (!impl_->coordinate_index) return {};
    std::vector<CoordinateTrack> result;
    result.reserve(impl_->coordinate_index->tracks().size());
    for (const auto& track : impl_->coordinate_index->tracks()) {
        result.push_back(CoordinateTrack{track.source_type, track.reference_name,
            track.sequence_name, track.haplotype, track.sequence_start,
            track.sequence_end, track.entry_count});
    }
    return result;
}

std::vector<PathStep> IndexedGraph::path_steps(std::string_view key,
                                               std::uint64_t start,
                                               std::uint64_t maximum) const {
    const std::lock_guard<std::recursive_mutex> lock(impl_->reader_mutex);
    if (!impl_->path_index) throw std::runtime_error("No .pdx path index is available");
    std::uint32_t id = 0;
    if (!impl_->path_index->lookup_path_id(std::string(key), id)) {
        throw std::out_of_range("Path does not exist: " + std::string(key));
    }
    std::vector<PathStep> result;
    for (const auto& step : impl_->path_index->read_steps(id, start, maximum)) {
        result.push_back(PathStep{impl_->path_index->copy_node_name(step.node_id), step.is_reverse});
    }
    return result;
}

Graph IndexedGraph::get_subgraph(const std::vector<std::string>& seeds,
                                 const ExtractionOptions& options,
                                 const QueryCallbacks& callbacks) const {
    const std::lock_guard<std::recursive_mutex> lock(impl_->reader_mutex);
    return impl_->materialize(impl_->bfs(seeds, options.max_nodes, callbacks), options, callbacks);
}

void IndexedGraph::stream_subgraph(const std::vector<std::string>& seeds,
                                   const GfaLineVisitor& visitor,
                                   const ExtractionOptions& options,
                                   const QueryCallbacks& callbacks) const {
    const std::lock_guard<std::recursive_mutex> lock(impl_->reader_mutex);
    impl_->stream_selection(impl_->bfs(seeds, options.max_nodes, callbacks), visitor,
                            options, callbacks);
}

void IndexedGraph::stream_region(std::string reference, std::string sequence,
                                 std::uint64_t begin, std::uint64_t end,
                                 const GfaLineVisitor& visitor,
                                 const RegionOptions& options,
                                 const QueryCallbacks& callbacks) const {
    const std::lock_guard<std::recursive_mutex> lock(impl_->reader_mutex);
    if (end <= begin) throw std::invalid_argument("Region end must be greater than begin");
    if (!impl_->path_index) throw std::runtime_error("Region queries require a .pdx path index");
    std::vector<std::uint32_t> ranks;
    std::vector<paths::SubpathRun> exact_runs;
    if (impl_->coordinate_index) {
        try {
            auto query = impl_->coordinate_index->query_region(reference, sequence, begin, end);
            ranks = std::move(query.node_ranks);
            if (options.mode == RegionMode::all_haplotypes) {
                for (const auto& slice : query.slices) {
                    if (slice.track.source_type == 'S') continue;
                    const auto key = slice.track.source_type == 'P'
                        ? slice.track.sequence_name
                        : slice.track.reference_name + "|" + std::to_string(slice.track.haplotype) +
                          "|" + slice.track.sequence_name + "|" +
                          std::to_string(slice.track.sequence_start) + "|" +
                          std::to_string(slice.track.sequence_end);
                    std::uint32_t id = 0;
                    if (!impl_->path_index->lookup_path_id(key, id)) {
                        throw std::runtime_error("Coordinate track is not aligned with .pdx: " + key);
                    }
                    exact_runs.push_back({id, slice.start_step, slice.step_count});
                }
            }
        } catch (const std::exception& error) {
            warn(callbacks, std::string("Coordinate index lookup failed; using path scan: ") + error.what());
        }
    }
    if (ranks.empty()) {
        const auto fallback = coordinates::query_path_coordinates_on_the_fly(
            *impl_->path_index,
            impl_->capabilities.node_lengths ? impl_->paths.lnx : std::string{},
            reference, sequence, begin, end);
        ranks = fallback.node_ranks;
        exact_runs = fallback.reference_path_runs;
    }
    if (ranks.empty()) throw std::out_of_range("No nodes overlap the requested region");
    if (ranks.size() > options.max_nodes) throw std::runtime_error("Region seed count exceeds max_nodes");

    if (options.mode == RegionMode::all_haplotypes) {
        coordinates::PathHaplotypeQueryOptions query_options;
        std::unique_ptr<indexer::NodeLengthIndexReader> lengths;
        if (options.haplotype_gap) {
            if (!impl_->capabilities.node_lengths) {
                throw std::runtime_error("haplotype_gap requires a .lnx node-length index");
            }
            lengths = std::make_unique<indexer::NodeLengthIndexReader>(impl_->paths.lnx);
            query_options.max_gap_bases = options.haplotype_gap;
            query_options.node_lengths = lengths.get();
        }
        const auto selected = coordinates::query_path_haplotype_nodes(
            *impl_->path_index, ranks, exact_runs, query_options);
        if (selected.node_ranks.size() > options.max_nodes) {
            throw std::runtime_error("All-haplotype result exceeds max_nodes");
        }
        impl_->stream_selection(impl_->selection_from_ranks(selected.node_ranks,
            selected.path_runs), visitor, options, callbacks);
        return;
    }

    std::vector<std::string> seeds;
    seeds.reserve(ranks.size());
    for (const auto rank : ranks) seeds.push_back(impl_->path_index->copy_node_name(rank));
    impl_->stream_selection(impl_->bfs(seeds, options.max_nodes, callbacks), visitor,
                            options, callbacks);
}

Graph IndexedGraph::get_region(std::string reference, std::string sequence,
                               std::uint64_t begin, std::uint64_t end,
                               const RegionOptions& options,
                               const QueryCallbacks& callbacks) const {
    std::ostringstream buffer;
    stream_region(std::move(reference), std::move(sequence), begin, end,
        [&](std::string_view line) {
            buffer << line << '\n';
            return true;
        }, options, callbacks);
    std::istringstream input(buffer.str());
    return Graph::from_gfa(input);
}

void IndexedGraph::stream_path_subgraph(std::string_view key,
                                        const GfaLineVisitor& visitor,
                                        std::uint64_t start,
                                        std::uint64_t maximum,
                                        const ExtractionOptions& options,
                                        const QueryCallbacks& callbacks) const {
    const std::lock_guard<std::recursive_mutex> lock(impl_->reader_mutex);
    if (!impl_->path_index) throw std::runtime_error("No .pdx path index is available");
    std::uint32_t id = 0;
    if (!impl_->path_index->lookup_path_id(std::string(key), id)) {
        throw std::out_of_range("Path does not exist: " + std::string(key));
    }
    const auto info = impl_->path_index->get_path_info(id);
    if (start > info.step_count) throw std::out_of_range("Path start step is out of range");
    const auto count = std::min(maximum, info.step_count - start);
    if (count == 0) throw std::invalid_argument("Path subgraph must contain at least one step");
    std::vector<std::uint32_t> ranks;
    impl_->path_index->for_each_step(id, start, count,
        [&](const paths::StepRecord& step, std::uint64_t) { ranks.push_back(step.node_id); });
    auto extraction = impl_->selection_from_ranks(std::move(ranks), {{id, start, count}});
    if (extraction.names.size() > options.max_nodes) throw std::runtime_error("Path result exceeds max_nodes");
    impl_->stream_selection(extraction, visitor, options, callbacks);
}

Graph IndexedGraph::get_path_subgraph(std::string_view key, std::uint64_t start,
                                      std::uint64_t maximum,
                                      const ExtractionOptions& options,
                                      const QueryCallbacks& callbacks) const {
    std::ostringstream buffer;
    stream_path_subgraph(key, [&](std::string_view line) {
        buffer << line << '\n';
        return true;
    }, start, maximum, options, callbacks);
    std::istringstream input(buffer.str());
    return Graph::from_gfa(input);
}

}  // namespace gfaidx

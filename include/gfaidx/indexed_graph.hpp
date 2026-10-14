#ifndef GFAIDX_PUBLIC_INDEXED_GRAPH_HPP
#define GFAIDX_PUBLIC_INDEXED_GRAPH_HPP

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "gfaidx/graph.hpp"

namespace gfaidx {

// Explicit paths override the conventional sidecars adjacent to graph_path.
// Empty fields retain automatic discovery.
struct IndexPaths {
    std::string idx;
    std::string ndx;
    std::string pdx;
    std::string lnx;
    std::string pcx;
    std::string cdx;
};

// Capabilities distinguish required graph indexes from optional path and
// coordinate indexes without making callers probe the filesystem themselves.
struct Capabilities {
    bool paths{false};
    bool node_lengths{false};
    bool path_checkpoints{false};
    bool coordinates{false};
};

struct PathDescriptor {
    std::uint32_t id{};
    char record_type{'P'};
    std::string key;
    std::string name;
    std::string sample;
    std::uint64_t haplotype{};
    std::string sequence_name;
    std::int64_t sequence_start{-1};
    std::int64_t sequence_end{-1};
    std::uint64_t step_count{};
    Tags tags;
};

struct CoordinateTrack {
    char source_type{};
    std::string reference;
    std::string sequence;
    std::uint64_t haplotype{};
    std::uint64_t begin{};
    std::uint64_t end{};
    std::uint64_t entry_count{};
};

enum class RegionMode {
    // Graph neighborhood around the reference interval: a hop-count BFS
    // bounded by max_nodes, or a base-pair context search when
    // RegionOptions::context is set. Emits every P/W run inside the result.
    bfs,
    all_haplotypes,
    // The same base-pair context subgraph as bfs with a context, but only the
    // queried reference path is emitted; no other haplotype is formatted.
    // This matches gbz-base's --reference-only subgraph query.
    reference,
};

// Context length used by RegionMode::reference when RegionOptions::context is
// not set. Matches gbz-base's default query context.
inline constexpr std::uint64_t kDefaultRegionContext = 100;

struct ExtractionOptions {
    // Bounds get_subgraph()'s BFS neighborhood (seeds plus expanded nodes).
    // For get_region(), it only applies under RegionMode::bfs without a
    // context; context searches and RegionMode::all_haplotypes are sized by
    // base-pair distance or haplotype spans instead of a node budget.
    std::uint32_t max_nodes{100};
    bool include_paths{true};
    bool include_coordinates{false};
    std::uint32_t threads{1};
};

struct RegionOptions : ExtractionOptions {
    RegionMode mode{RegionMode::bfs};
    std::optional<std::uint64_t> haplotype_gap;
    // Base-pair context around the reference interval, using gbz-base's
    // distance rules: every node with a side reachable within this many bases
    // of the interval is included. Optional for bfs (replaces max_nodes),
    // defaults to kDefaultRegionContext for reference, and is rejected for
    // all_haplotypes.
    std::optional<std::uint64_t> context;
};

// A false return requests cooperative cancellation.  Warning and progress
// callbacks let a CLI or GUI surface fallback notices and phase-by-phase
// status without library-side printing.  progress reports normal operation
// (phase timings, resolved/selected counts); warning reports an anomaly
// (such as falling back from an optional sidecar).  Messages carry no
// timestamp or other formatting of their own; callers render them however
// fits their output (the CLI prefixes a timestamp, for example).
struct QueryCallbacks {
    std::function<void(std::string_view)> warning;
    std::function<void(std::string_view)> progress;
    std::function<bool()> keep_going;
};

using GfaLineVisitor = std::function<bool(std::string_view)>;

// IndexedGraph is an immutable view over an indexed graph bundle.  It owns no
// extracted topology; materialized queries return independent mutable Graphs.
class IndexedGraph {
public:
    explicit IndexedGraph(std::string graph_path,
                          IndexPaths overrides = {});
    ~IndexedGraph();

    IndexedGraph(IndexedGraph&&) noexcept;
    IndexedGraph& operator=(IndexedGraph&&) noexcept;
    IndexedGraph(const IndexedGraph&) = delete;
    IndexedGraph& operator=(const IndexedGraph&) = delete;

    [[nodiscard]] const std::string& graph_path() const noexcept;
    [[nodiscard]] const IndexPaths& index_paths() const noexcept;
    [[nodiscard]] Capabilities capabilities() const noexcept;
    [[nodiscard]] std::uint64_t node_count() const noexcept;
    [[nodiscard]] std::uint32_t community_count() const noexcept;

    [[nodiscard]] bool node_exists(std::string_view node_id) const noexcept;
    [[nodiscard]] std::uint32_t community_id(std::string_view node_id) const;
    [[nodiscard]] Node get_node(std::string_view node_id) const;
    [[nodiscard]] std::vector<std::string> neighbors(std::string_view node_id) const;
    [[nodiscard]] std::vector<Edge> incident_edges(std::string_view node_id) const;
    [[nodiscard]] Graph get_community(std::uint32_t community_id) const;

    [[nodiscard]] std::vector<PathDescriptor> paths() const;
    [[nodiscard]] PathDescriptor get_path(std::string_view path_key) const;
    [[nodiscard]] std::vector<CoordinateTrack> coordinate_tracks() const;
    [[nodiscard]] std::vector<PathStep> path_steps(
        std::string_view path_key,
        std::uint64_t start_step = 0,
        std::uint64_t max_steps = ~std::uint64_t{0}) const;

    [[nodiscard]] Graph get_subgraph(
        const std::vector<std::string>& seeds,
        const ExtractionOptions& options = {},
        const QueryCallbacks& callbacks = {}) const;
    void stream_subgraph(const std::vector<std::string>& seeds,
                         const GfaLineVisitor& visitor,
                         const ExtractionOptions& options = {},
                         const QueryCallbacks& callbacks = {}) const;

    [[nodiscard]] Graph get_region(
        std::string reference,
        std::string sequence,
        std::uint64_t begin,
        std::uint64_t end,
        const RegionOptions& options = {},
        const QueryCallbacks& callbacks = {}) const;
    void stream_region(std::string reference,
                       std::string sequence,
                       std::uint64_t begin,
                       std::uint64_t end,
                       const GfaLineVisitor& visitor,
                       const RegionOptions& options = {},
                       const QueryCallbacks& callbacks = {}) const;

    [[nodiscard]] Graph get_path_subgraph(
        std::string_view path_key,
        std::uint64_t start_step = 0,
        std::uint64_t max_steps = ~std::uint64_t{0},
        const ExtractionOptions& options = {},
        const QueryCallbacks& callbacks = {}) const;
    void stream_path_subgraph(std::string_view path_key,
                              const GfaLineVisitor& visitor,
                              std::uint64_t start_step = 0,
                              std::uint64_t max_steps = ~std::uint64_t{0},
                              const ExtractionOptions& options = {},
                              const QueryCallbacks& callbacks = {}) const;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace gfaidx

#endif  // GFAIDX_PUBLIC_INDEXED_GRAPH_HPP

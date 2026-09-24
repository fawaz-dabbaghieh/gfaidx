#ifndef GFAIDX_PUBLIC_GRAPH_HPP
#define GFAIDX_PUBLIC_GRAPH_HPP

#include <cstdint>
#include <functional>
#include <iosfwd>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace gfaidx {

// Optional GFA fields are retained verbatim (for example "LN:i:42").  This
// preserves fields unknown to gfaidx and avoids imposing a lossy tag schema on
// applications that build domain-specific tools on top of the library.
using Tags = std::vector<std::string>;

// An owned GFA segment.  A sequence of "*" means that bases are unavailable.
struct Node {
    std::string id;
    std::string sequence{"*"};
    Tags tags;
};

// One endpoint of a bidirected GFA link.  reverse maps directly to the '-' GFA
// orientation; false maps to '+'.
struct Endpoint {
    std::string node_id;
    bool reverse{false};
};

// An owned link.  ids are stable for the lifetime of a Graph and allow callers
// to distinguish parallel links with otherwise identical fields.
struct Edge {
    std::uint64_t id{};
    Endpoint from;
    Endpoint to;
    std::string overlap{"*"};
    Tags tags;
};

// A path step names a segment and records the orientation in which it is used.
struct PathStep {
    std::string node_id;
    bool reverse{false};
};

// P and W records share an ordered step list but retain their different GFA
// metadata.  name identifies P records.  W records use sample/haplotype/
// sequence/start/end and receive a deterministic composite key from key().
struct Path {
    std::uint64_t id{};
    char record_type{'P'};
    std::string name;
    std::string sample;
    std::uint64_t haplotype{};
    std::string sequence_name;
    std::int64_t sequence_start{-1};
    std::int64_t sequence_end{-1};
    std::vector<PathStep> steps;
    std::string overlaps{"*"};
    Tags tags;

    [[nodiscard]] std::string key() const;
};

// ValidationIssue is deliberately data-only so C++, Python, and future
// bindings can present validation failures in their native style.
struct ValidationIssue {
    std::string code;
    std::string message;
};

// Graph is an owned, mutable extraction.  It never writes through to an
// IndexedGraph.  Values returned from accessors are references in C++; Python
// bindings expose copies so mutations must pass through these checked methods.
class Graph {
public:
    Graph() = default;

    [[nodiscard]] std::size_t node_count() const noexcept { return nodes_.size(); }
    [[nodiscard]] std::size_t edge_count() const noexcept { return edges_.size(); }
    [[nodiscard]] std::size_t path_count() const noexcept { return paths_.size(); }

    [[nodiscard]] const std::vector<std::string>& headers() const noexcept { return headers_; }
    [[nodiscard]] const std::vector<Node>& nodes() const noexcept { return nodes_; }
    [[nodiscard]] const std::vector<Edge>& edges() const noexcept { return edges_; }
    [[nodiscard]] const std::vector<Path>& paths() const noexcept { return paths_; }

    [[nodiscard]] bool node_exists(std::string_view node_id) const noexcept;
    [[nodiscard]] const Node& get_node(std::string_view node_id) const;
    [[nodiscard]] const Edge& get_edge(std::uint64_t edge_id) const;
    [[nodiscard]] const Path& get_path(std::string_view path_key) const;

    // neighbors() is an orientation-agnostic convenience view.  incident_edges()
    // retains complete bidirected endpoint information for orientation-aware tools.
    [[nodiscard]] std::vector<std::string> neighbors(std::string_view node_id) const;
    [[nodiscard]] std::vector<Edge> incident_edges(std::string_view node_id) const;

    void add_header(std::string line);
    void add_node(Node node);
    void update_node(Node node);
    void remove_node(std::string_view node_id);

    [[nodiscard]] std::uint64_t add_edge(Edge edge);
    void update_edge(Edge edge);
    void remove_edge(std::uint64_t edge_id);

    [[nodiscard]] std::uint64_t add_path(Path path);
    void update_path(Path path);
    void replace_path_steps(std::string_view path_key,
                            std::vector<PathStep> steps);
    void remove_path(std::string_view path_key);

    [[nodiscard]] std::vector<ValidationIssue> validate() const;

    // Emit records in deterministic H/S/L/P/W order.  visit_gfa_lines avoids a
    // large aggregate string and stops early when the callback returns false.
    void visit_gfa_lines(const std::function<bool(std::string_view)>& visitor) const;
    void write_gfa(std::ostream& out) const;
    void write_gfa(const std::string& path) const;

    // Parse records produced by gfaidx or supplied by an application.  Blank
    // and comment lines are ignored; unsupported record types are rejected.
    static Graph from_gfa(std::istream& in);
    static Graph from_gfa(const std::string& path);

private:
    void rebuild_indexes();
    void validate_steps(const std::vector<PathStep>& steps) const;

    std::vector<std::string> headers_;
    std::vector<Node> nodes_;
    std::vector<Edge> edges_;
    std::vector<Path> paths_;
    std::unordered_map<std::string, std::size_t> node_index_;
    std::unordered_map<std::uint64_t, std::size_t> edge_index_;
    std::unordered_map<std::string, std::size_t> path_index_;
    std::uint64_t next_edge_id_{1};
    std::uint64_t next_path_id_{1};
};

}  // namespace gfaidx

#endif  // GFAIDX_PUBLIC_GRAPH_HPP

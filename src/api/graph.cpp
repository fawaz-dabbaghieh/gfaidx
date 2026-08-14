#include "gfaidx/graph.hpp"

#include <algorithm>
#include <fstream>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <unordered_set>
#include <utility>

namespace gfaidx {
namespace {

// Split a tab-delimited GFA record without interpreting optional tag values.
std::vector<std::string_view> split_tabs(std::string_view line) {
    std::vector<std::string_view> fields;
    for (std::size_t begin = 0;;) {
        const auto end = line.find('\t', begin);
        fields.emplace_back(line.substr(begin, end - begin));
        if (end == std::string_view::npos) break;
        begin = end + 1;
    }
    return fields;
}

Tags copy_tags(const std::vector<std::string_view>& fields, std::size_t begin) {
    Tags tags;
    tags.reserve(fields.size() > begin ? fields.size() - begin : 0);
    for (std::size_t i = begin; i < fields.size(); ++i) {
        tags.emplace_back(fields[i]);
    }
    return tags;
}

std::uint64_t parse_u64(std::string_view value, const char* field) {
    try {
        std::size_t consumed = 0;
        const auto parsed = std::stoull(std::string(value), &consumed);
        if (consumed != value.size()) throw std::invalid_argument("trailing");
        return parsed;
    } catch (...) {
        throw std::runtime_error(std::string("Invalid ") + field + ": " +
                                 std::string(value));
    }
}

std::int64_t parse_optional_i64(std::string_view value, const char* field) {
    if (value == "*") return -1;
    const auto parsed = parse_u64(value, field);
    if (parsed > static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max())) {
        throw std::runtime_error(std::string(field) + " is too large");
    }
    return static_cast<std::int64_t>(parsed);
}

std::vector<PathStep> parse_p_steps(std::string_view encoded) {
    std::vector<PathStep> steps;
    if (encoded == "*" || encoded.empty()) return steps;
    for (std::size_t begin = 0;;) {
        const auto end = encoded.find(',', begin);
        const auto token = encoded.substr(begin, end - begin);
        if (token.size() < 2 || (token.back() != '+' && token.back() != '-')) {
            throw std::runtime_error("Malformed P step: " + std::string(token));
        }
        steps.push_back(PathStep{std::string(token.substr(0, token.size() - 1)),
                                 token.back() == '-'});
        if (end == std::string_view::npos) break;
        begin = end + 1;
    }
    return steps;
}

std::vector<PathStep> parse_w_steps(std::string_view encoded) {
    std::vector<PathStep> steps;
    if (encoded == "*" || encoded.empty()) return steps;
    std::size_t begin = 0;
    while (begin < encoded.size()) {
        const char direction = encoded[begin];
        if (direction != '>' && direction != '<') {
            throw std::runtime_error("Malformed W walk: " + std::string(encoded));
        }
        const auto end = encoded.find_first_of("><", begin + 1);
        const auto name = encoded.substr(begin + 1, end - (begin + 1));
        if (name.empty()) {
            throw std::runtime_error("Malformed W walk with an empty step");
        }
        steps.push_back(PathStep{std::string(name), direction == '<'});
        if (end == std::string_view::npos) break;
        begin = end;
    }
    return steps;
}

std::string join_tags(const Tags& tags) {
    std::string result;
    for (const auto& tag : tags) {
        result.push_back('\t');
        result.append(tag);
    }
    return result;
}

std::string format_node(const Node& node) {
    return "S\t" + node.id + "\t" + node.sequence + join_tags(node.tags);
}

std::string format_edge(const Edge& edge) {
    return "L\t" + edge.from.node_id + "\t" +
           (edge.from.reverse ? "-" : "+") + "\t" + edge.to.node_id +
           "\t" + (edge.to.reverse ? "-" : "+") + "\t" + edge.overlap +
           join_tags(edge.tags);
}

std::string format_path(const Path& path) {
    std::string line;
    if (path.record_type == 'P') {
        line = "P\t" + path.name + "\t";
        if (path.steps.empty()) {
            line.push_back('*');
        } else {
            for (std::size_t i = 0; i < path.steps.size(); ++i) {
                if (i != 0) line.push_back(',');
                line += path.steps[i].node_id;
                line.push_back(path.steps[i].reverse ? '-' : '+');
            }
        }
        line += "\t" + path.overlaps;
    } else {
        line = "W\t" + path.sample + "\t" + std::to_string(path.haplotype) +
               "\t" + path.sequence_name + "\t";
        line += path.sequence_start < 0 ? "*" : std::to_string(path.sequence_start);
        line.push_back('\t');
        line += path.sequence_end < 0 ? "*" : std::to_string(path.sequence_end);
        line.push_back('\t');
        if (path.steps.empty()) {
            line.push_back('*');
        } else {
            for (const auto& step : path.steps) {
                line.push_back(step.reverse ? '<' : '>');
                line += step.node_id;
            }
        }
    }
    line += join_tags(path.tags);
    return line;
}

}  // namespace

std::string Path::key() const {
    if (record_type == 'P') return name;
    return sample + "|" + std::to_string(haplotype) + "|" + sequence_name +
           "|" + (sequence_start < 0 ? "*" : std::to_string(sequence_start)) +
           "|" + (sequence_end < 0 ? "*" : std::to_string(sequence_end));
}

bool Graph::node_exists(std::string_view node_id) const noexcept {
    return node_index_.find(std::string(node_id)) != node_index_.end();
}

const Node& Graph::get_node(std::string_view node_id) const {
    const auto found = node_index_.find(std::string(node_id));
    if (found == node_index_.end()) {
        throw std::out_of_range("Node does not exist: " + std::string(node_id));
    }
    return nodes_[found->second];
}

const Edge& Graph::get_edge(std::uint64_t edge_id) const {
    const auto found = edge_index_.find(edge_id);
    if (found == edge_index_.end()) {
        throw std::out_of_range("Edge does not exist: " + std::to_string(edge_id));
    }
    return edges_[found->second];
}

const Path& Graph::get_path(std::string_view path_key) const {
    const auto found = path_index_.find(std::string(path_key));
    if (found == path_index_.end()) {
        throw std::out_of_range("Path does not exist: " + std::string(path_key));
    }
    return paths_[found->second];
}

std::vector<std::string> Graph::neighbors(std::string_view node_id) const {
    if (!node_exists(node_id)) {
        throw std::out_of_range("Node does not exist: " + std::string(node_id));
    }
    std::vector<std::string> result;
    std::unordered_set<std::string> seen;
    for (const auto& edge : edges_) {
        if (edge.from.node_id == node_id && seen.insert(edge.to.node_id).second) {
            result.push_back(edge.to.node_id);
        }
        if (edge.to.node_id == node_id && seen.insert(edge.from.node_id).second) {
            result.push_back(edge.from.node_id);
        }
    }
    return result;
}

std::vector<Edge> Graph::incident_edges(std::string_view node_id) const {
    if (!node_exists(node_id)) {
        throw std::out_of_range("Node does not exist: " + std::string(node_id));
    }
    std::vector<Edge> result;
    for (const auto& edge : edges_) {
        if (edge.from.node_id == node_id || edge.to.node_id == node_id) {
            result.push_back(edge);
        }
    }
    return result;
}

void Graph::add_header(std::string line) {
    if (line.rfind("H\t", 0) != 0 && line != "H") {
        throw std::invalid_argument("A GFA header must begin with H followed by a tab");
    }
    headers_.push_back(std::move(line));
}

void Graph::add_node(Node node) {
    if (node.id.empty()) throw std::invalid_argument("Node id cannot be empty");
    if (node_exists(node.id)) throw std::invalid_argument("Duplicate node: " + node.id);
    node_index_.emplace(node.id, nodes_.size());
    nodes_.push_back(std::move(node));
}

void Graph::update_node(Node node) {
    const auto found = node_index_.find(node.id);
    if (found == node_index_.end()) throw std::out_of_range("Node does not exist: " + node.id);
    nodes_[found->second] = std::move(node);
}

void Graph::remove_node(std::string_view node_id) {
    const std::string key(node_id);
    const auto found = node_index_.find(key);
    if (found == node_index_.end()) throw std::out_of_range("Node does not exist: " + key);
    for (const auto& path : paths_) {
        for (const auto& step : path.steps) {
            if (step.node_id == node_id) {
                throw std::invalid_argument("Cannot remove node referenced by path " + path.key());
            }
        }
    }
    edges_.erase(std::remove_if(edges_.begin(), edges_.end(), [&](const Edge& edge) {
        return edge.from.node_id == node_id || edge.to.node_id == node_id;
    }), edges_.end());
    nodes_.erase(nodes_.begin() + static_cast<std::ptrdiff_t>(found->second));
    rebuild_indexes();
}

std::uint64_t Graph::add_edge(Edge edge) {
    if (!node_exists(edge.from.node_id) || !node_exists(edge.to.node_id)) {
        throw std::invalid_argument("Both edge endpoints must exist in the graph");
    }
    if (edge.id == 0) edge.id = next_edge_id_++;
    if (edge_index_.count(edge.id) != 0) throw std::invalid_argument("Duplicate edge id");
    next_edge_id_ = std::max(next_edge_id_, edge.id + 1);
    const auto id = edge.id;
    edge_index_.emplace(id, edges_.size());
    edges_.push_back(std::move(edge));
    return id;
}

void Graph::update_edge(Edge edge) {
    const auto found = edge_index_.find(edge.id);
    if (found == edge_index_.end()) throw std::out_of_range("Edge does not exist");
    if (!node_exists(edge.from.node_id) || !node_exists(edge.to.node_id)) {
        throw std::invalid_argument("Both edge endpoints must exist in the graph");
    }
    edges_[found->second] = std::move(edge);
}

void Graph::remove_edge(std::uint64_t edge_id) {
    const auto found = edge_index_.find(edge_id);
    if (found == edge_index_.end()) throw std::out_of_range("Edge does not exist");
    edges_.erase(edges_.begin() + static_cast<std::ptrdiff_t>(found->second));
    rebuild_indexes();
}

void Graph::validate_steps(const std::vector<PathStep>& steps) const {
    for (const auto& step : steps) {
        if (!node_exists(step.node_id)) {
            throw std::invalid_argument("Path step references missing node: " + step.node_id);
        }
    }
}

std::uint64_t Graph::add_path(Path path) {
    if (path.record_type != 'P' && path.record_type != 'W') {
        throw std::invalid_argument("Path record_type must be P or W");
    }
    validate_steps(path.steps);
    const auto key = path.key();
    if (key.empty()) throw std::invalid_argument("Path key cannot be empty");
    if (path_index_.count(key) != 0) throw std::invalid_argument("Duplicate path: " + key);
    if (path.id == 0) path.id = next_path_id_++;
    next_path_id_ = std::max(next_path_id_, path.id + 1);
    const auto id = path.id;
    path_index_.emplace(key, paths_.size());
    paths_.push_back(std::move(path));
    return id;
}

void Graph::update_path(Path path) {
    if (path.record_type != 'P' && path.record_type != 'W') {
        throw std::invalid_argument("Path record_type must be P or W");
    }
    validate_steps(path.steps);
    auto by_id = std::find_if(paths_.begin(), paths_.end(), [&](const Path& candidate) {
        return candidate.id == path.id;
    });
    if (by_id == paths_.end()) throw std::out_of_range("Path does not exist");
    const auto old_key = by_id->key();
    const auto new_key = path.key();
    const auto conflict = path_index_.find(new_key);
    if (conflict != path_index_.end() && &paths_[conflict->second] != &*by_id) {
        throw std::invalid_argument("Duplicate path: " + new_key);
    }
    *by_id = std::move(path);
    path_index_.erase(old_key);
    path_index_[new_key] = static_cast<std::size_t>(by_id - paths_.begin());
}

void Graph::replace_path_steps(std::string_view path_key,
                               std::vector<PathStep> steps) {
    validate_steps(steps);
    const auto found = path_index_.find(std::string(path_key));
    if (found == path_index_.end()) throw std::out_of_range("Path does not exist");
    paths_[found->second].steps = std::move(steps);
}

void Graph::remove_path(std::string_view path_key) {
    const auto found = path_index_.find(std::string(path_key));
    if (found == path_index_.end()) throw std::out_of_range("Path does not exist");
    paths_.erase(paths_.begin() + static_cast<std::ptrdiff_t>(found->second));
    rebuild_indexes();
}

std::vector<ValidationIssue> Graph::validate() const {
    std::vector<ValidationIssue> issues;
    for (const auto& edge : edges_) {
        if (!node_exists(edge.from.node_id) || !node_exists(edge.to.node_id)) {
            issues.push_back({"missing_edge_endpoint", "Edge " + std::to_string(edge.id) +
                              " references a missing node"});
        }
    }
    for (const auto& path : paths_) {
        for (const auto& step : path.steps) {
            if (!node_exists(step.node_id)) {
                issues.push_back({"missing_path_node", "Path " + path.key() +
                                  " references missing node " + step.node_id});
            }
        }
        // GFA permits a path to mention consecutive segments without an L
        // record, so this is a validation issue rather than a mutation error.
        for (std::size_t i = 1; i < path.steps.size(); ++i) {
            const auto& left = path.steps[i - 1].node_id;
            const auto& right = path.steps[i].node_id;
            const bool connected = std::any_of(
                edges_.begin(), edges_.end(), [&](const Edge& edge) {
                    return (edge.from.node_id == left && edge.to.node_id == right) ||
                           (edge.from.node_id == right && edge.to.node_id == left);
                });
            if (!connected) {
                issues.push_back({
                    "missing_path_link",
                    "Path " + path.key() + " has no link between " + left +
                        " and " + right,
                });
            }
        }
    }
    return issues;
}

void Graph::visit_gfa_lines(const std::function<bool(std::string_view)>& visitor) const {
    for (const auto& header : headers_) if (!visitor(header)) return;
    std::string line;
    for (const auto& node : nodes_) {
        line = format_node(node);
        if (!visitor(line)) return;
    }
    for (const auto& edge : edges_) {
        line = format_edge(edge);
        if (!visitor(line)) return;
    }
    for (const auto& path : paths_) {
        line = format_path(path);
        if (!visitor(line)) return;
    }
}

void Graph::write_gfa(std::ostream& out) const {
    visit_gfa_lines([&](std::string_view line) {
        out.write(line.data(), static_cast<std::streamsize>(line.size()));
        out.put('\n');
        if (!out) throw std::runtime_error("Failed while writing GFA output");
        return true;
    });
}

void Graph::write_gfa(const std::string& path) const {
    std::ofstream out(path);
    if (!out) throw std::runtime_error("Failed to open GFA output: " + path);
    write_gfa(out);
}

Graph Graph::from_gfa(std::istream& in) {
    Graph graph;
    std::string line;
    std::vector<std::string> records;
    while (std::getline(in, line)) {
        if (!line.empty() && line.back() == '\r') line.pop_back();
        if (line.empty() || line[0] == '#') continue;
        records.push_back(std::move(line));
    }
    if (!in.eof()) throw std::runtime_error("Failed while reading GFA input");

    // GFA producers normally emit S before L/P/W, but the format does not make
    // callers rely on that convention. Stable grouping lets checked mutators
    // validate endpoints and steps even when an input places records earlier.
    const auto rank = [](const std::string& record) {
        if (record.empty()) return 5;
        if (record[0] == 'H') return 0;
        if (record[0] == 'S') return 1;
        if (record[0] == 'L') return 2;
        if (record[0] == 'P' || record[0] == 'W') return 3;
        return 4;
    };
    std::stable_sort(records.begin(), records.end(),
                     [&](const std::string& lhs, const std::string& rhs) {
                         return rank(lhs) < rank(rhs);
                     });

    for (const auto& record : records) {
        line = record;
        const auto fields = split_tabs(line);
        if (fields.empty()) continue;
        if (fields[0] == "H") {
            graph.add_header(line);
        } else if (fields[0] == "S") {
            if (fields.size() < 3) throw std::runtime_error("Malformed S record: " + line);
            graph.add_node(Node{std::string(fields[1]), std::string(fields[2]), copy_tags(fields, 3)});
        } else if (fields[0] == "L") {
            if (fields.size() < 6 || (fields[2] != "+" && fields[2] != "-") ||
                (fields[4] != "+" && fields[4] != "-")) {
                throw std::runtime_error("Malformed L record: " + line);
            }
            Edge edge;
            edge.from = Endpoint{std::string(fields[1]), fields[2] == "-"};
            edge.to = Endpoint{std::string(fields[3]), fields[4] == "-"};
            edge.overlap = std::string(fields[5]);
            edge.tags = copy_tags(fields, 6);
            (void)graph.add_edge(std::move(edge));
        } else if (fields[0] == "P") {
            if (fields.size() < 4) throw std::runtime_error("Malformed P record: " + line);
            Path path;
            path.record_type = 'P';
            path.name = std::string(fields[1]);
            path.steps = parse_p_steps(fields[2]);
            path.overlaps = std::string(fields[3]);
            path.tags = copy_tags(fields, 4);
            (void)graph.add_path(std::move(path));
        } else if (fields[0] == "W") {
            if (fields.size() < 7) throw std::runtime_error("Malformed W record: " + line);
            Path path;
            path.record_type = 'W';
            path.sample = std::string(fields[1]);
            path.haplotype = parse_u64(fields[2], "W haplotype");
            path.sequence_name = std::string(fields[3]);
            path.sequence_start = parse_optional_i64(fields[4], "W start");
            path.sequence_end = parse_optional_i64(fields[5], "W end");
            if (path.sequence_start >= 0 && path.sequence_end >= 0 &&
                path.sequence_end < path.sequence_start) {
                throw std::runtime_error("W end is before W start: " + line);
            }
            path.steps = parse_w_steps(fields[6]);
            path.tags = copy_tags(fields, 7);
            (void)graph.add_path(std::move(path));
        } else {
            throw std::runtime_error("Unsupported GFA record type: " + std::string(fields[0]));
        }
    }
    return graph;
}

Graph Graph::from_gfa(const std::string& path) {
    std::ifstream in(path);
    if (!in) throw std::runtime_error("Failed to open GFA input: " + path);
    return from_gfa(in);
}

void Graph::rebuild_indexes() {
    node_index_.clear();
    edge_index_.clear();
    path_index_.clear();
    for (std::size_t i = 0; i < nodes_.size(); ++i) node_index_[nodes_[i].id] = i;
    for (std::size_t i = 0; i < edges_.size(); ++i) edge_index_[edges_[i].id] = i;
    for (std::size_t i = 0; i < paths_.size(); ++i) path_index_[paths_[i].key()] = i;
}

}  // namespace gfaidx

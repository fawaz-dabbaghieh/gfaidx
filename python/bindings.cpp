#include <fstream>
#include <string>
#include <utility>

#include <pybind11/functional.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include "gfaidx/gfaidx.hpp"

namespace py = pybind11;

namespace {

// Convert a Python file-like object into the callback expected by IndexedGraph.
// The native query runs without the GIL; each small write reacquires it only for
// the duration of destination.write().
template<class StreamFunction>
void stream_to_python(py::object destination, StreamFunction&& stream) {
    {
        py::gil_scoped_release release;
        stream([destination](std::string_view line) {
            py::gil_scoped_acquire acquire;
            // Construct one owned string because py::str does not provide a
            // C++ concatenation overload for string literals.
            destination.attr("write")(py::str(std::string(line) + "\n"));
            return true;
        });
    }
}

gfaidx::ExtractionOptions extraction_options(std::uint32_t max_nodes,
                                             bool include_paths,
                                             bool include_coordinates,
                                             std::uint32_t threads) {
    gfaidx::ExtractionOptions result;
    result.max_nodes = max_nodes;
    result.include_paths = include_paths;
    result.include_coordinates = include_coordinates;
    result.threads = threads;
    return result;
}

gfaidx::RegionOptions region_options(std::uint32_t max_nodes,
                                     bool include_paths,
                                     bool include_coordinates,
                                     std::uint32_t threads,
                                     gfaidx::RegionMode mode,
                                     std::optional<std::uint64_t> haplotype_gap) {
    gfaidx::RegionOptions result;
    static_cast<gfaidx::ExtractionOptions&>(result) =
        extraction_options(max_nodes, include_paths, include_coordinates, threads);
    result.mode = mode;
    result.haplotype_gap = haplotype_gap;
    return result;
}

}  // namespace

PYBIND11_MODULE(_pygfaidx, module) {
    module.doc() =
        "Disk-backed indexed GFA queries and owned mutable genome graphs";

    py::class_<gfaidx::Node>(module, "Node")
        .def(py::init<>())
        .def(py::init<std::string, std::string, gfaidx::Tags>(),
             py::arg("id"), py::arg("sequence") = "*",
             py::arg("tags") = gfaidx::Tags{})
        .def_readwrite("id", &gfaidx::Node::id)
        .def_readwrite("sequence", &gfaidx::Node::sequence)
        .def_readwrite("tags", &gfaidx::Node::tags);

    py::class_<gfaidx::Endpoint>(module, "Endpoint")
        .def(py::init<>())
        .def(py::init<std::string, bool>(), py::arg("node_id"),
             py::arg("reverse") = false)
        .def_readwrite("node_id", &gfaidx::Endpoint::node_id)
        .def_readwrite("reverse", &gfaidx::Endpoint::reverse);

    py::class_<gfaidx::Edge>(module, "Edge")
        .def(py::init<>())
        .def_readwrite("id", &gfaidx::Edge::id)
        .def_readwrite("from_endpoint", &gfaidx::Edge::from)
        .def_readwrite("to_endpoint", &gfaidx::Edge::to)
        .def_readwrite("overlap", &gfaidx::Edge::overlap)
        .def_readwrite("tags", &gfaidx::Edge::tags);

    py::class_<gfaidx::PathStep>(module, "PathStep")
        .def(py::init<>())
        .def(py::init<std::string, bool>(), py::arg("node_id"),
             py::arg("reverse") = false)
        .def_readwrite("node_id", &gfaidx::PathStep::node_id)
        .def_readwrite("reverse", &gfaidx::PathStep::reverse);

    py::class_<gfaidx::Path>(module, "Path")
        .def(py::init<>())
        .def_readwrite("id", &gfaidx::Path::id)
        .def_readwrite("record_type", &gfaidx::Path::record_type)
        .def_readwrite("name", &gfaidx::Path::name)
        .def_readwrite("sample", &gfaidx::Path::sample)
        .def_readwrite("haplotype", &gfaidx::Path::haplotype)
        .def_readwrite("sequence_name", &gfaidx::Path::sequence_name)
        .def_readwrite("sequence_start", &gfaidx::Path::sequence_start)
        .def_readwrite("sequence_end", &gfaidx::Path::sequence_end)
        .def_readwrite("steps", &gfaidx::Path::steps)
        .def_readwrite("overlaps", &gfaidx::Path::overlaps)
        .def_readwrite("tags", &gfaidx::Path::tags)
        .def_property_readonly("key", &gfaidx::Path::key);

    py::class_<gfaidx::ValidationIssue>(module, "ValidationIssue")
        .def_readonly("code", &gfaidx::ValidationIssue::code)
        .def_readonly("message", &gfaidx::ValidationIssue::message);

    py::class_<gfaidx::Graph>(module, "Graph")
        .def(py::init<>())
        .def_property_readonly("node_count", &gfaidx::Graph::node_count)
        .def_property_readonly("edge_count", &gfaidx::Graph::edge_count)
        .def_property_readonly("path_count", &gfaidx::Graph::path_count)
        .def_property_readonly("headers", &gfaidx::Graph::headers)
        // Returning STL vectors through pybind11 produces snapshots.  Users
        // edit through Graph methods, which keeps internal lookup tables valid.
        .def_property_readonly("nodes", &gfaidx::Graph::nodes)
        .def_property_readonly("edges", &gfaidx::Graph::edges)
        .def_property_readonly("paths", &gfaidx::Graph::paths)
        .def("node_exists", &gfaidx::Graph::node_exists, py::arg("node_id"))
        .def("get_node",
             [](const gfaidx::Graph& graph, const std::string& id) {
                 return gfaidx::Node(graph.get_node(id));
             },
             py::arg("node_id"))
        .def("get_edge",
             [](const gfaidx::Graph& graph, std::uint64_t id) {
                 return gfaidx::Edge(graph.get_edge(id));
             },
             py::arg("edge_id"))
        .def("get_path",
             [](const gfaidx::Graph& graph, const std::string& key) {
                 return gfaidx::Path(graph.get_path(key));
             },
             py::arg("path_key"))
        .def("neighbors", &gfaidx::Graph::neighbors, py::arg("node_id"))
        .def("incident_edges", &gfaidx::Graph::incident_edges,
             py::arg("node_id"))
        .def("add_header", &gfaidx::Graph::add_header, py::arg("line"))
        .def("add_node",
             [](gfaidx::Graph& graph, std::string id, std::string sequence,
                gfaidx::Tags tags) {
                 graph.add_node({std::move(id), std::move(sequence),
                                 std::move(tags)});
             },
             py::arg("node_id"), py::arg("sequence") = "*",
             py::arg("tags") = gfaidx::Tags{})
        .def("update_node", &gfaidx::Graph::update_node, py::arg("node"))
        .def("remove_node", &gfaidx::Graph::remove_node, py::arg("node_id"))
        .def("add_edge", &gfaidx::Graph::add_edge, py::arg("edge"))
        .def("update_edge", &gfaidx::Graph::update_edge, py::arg("edge"))
        .def("remove_edge", &gfaidx::Graph::remove_edge, py::arg("edge_id"))
        .def("add_path", &gfaidx::Graph::add_path, py::arg("path"))
        .def("update_path", &gfaidx::Graph::update_path, py::arg("path"))
        .def("replace_path_steps", &gfaidx::Graph::replace_path_steps,
             py::arg("path_key"), py::arg("steps"))
        .def("remove_path", &gfaidx::Graph::remove_path, py::arg("path_key"))
        .def("validate", &gfaidx::Graph::validate)
        .def("write_gfa",
             py::overload_cast<const std::string&>(&gfaidx::Graph::write_gfa,
                                                   py::const_),
             py::arg("path"), py::call_guard<py::gil_scoped_release>())
        .def_static("read_gfa",
             py::overload_cast<const std::string&>(&gfaidx::Graph::from_gfa),
             py::arg("path"), py::call_guard<py::gil_scoped_release>());

    py::class_<gfaidx::IndexPaths>(module, "IndexPaths")
        .def(py::init<>())
        .def_readwrite("idx", &gfaidx::IndexPaths::idx)
        .def_readwrite("ndx", &gfaidx::IndexPaths::ndx)
        .def_readwrite("pdx", &gfaidx::IndexPaths::pdx)
        .def_readwrite("lnx", &gfaidx::IndexPaths::lnx)
        .def_readwrite("pcx", &gfaidx::IndexPaths::pcx)
        .def_readwrite("cdx", &gfaidx::IndexPaths::cdx);

    py::class_<gfaidx::Capabilities>(module, "Capabilities")
        .def_readonly("paths", &gfaidx::Capabilities::paths)
        .def_readonly("node_lengths", &gfaidx::Capabilities::node_lengths)
        .def_readonly("path_checkpoints",
                      &gfaidx::Capabilities::path_checkpoints)
        .def_readonly("coordinates", &gfaidx::Capabilities::coordinates);

    py::class_<gfaidx::PathDescriptor>(module, "PathDescriptor")
        .def_readonly("id", &gfaidx::PathDescriptor::id)
        .def_readonly("record_type", &gfaidx::PathDescriptor::record_type)
        .def_readonly("key", &gfaidx::PathDescriptor::key)
        .def_readonly("name", &gfaidx::PathDescriptor::name)
        .def_readonly("sample", &gfaidx::PathDescriptor::sample)
        .def_readonly("haplotype", &gfaidx::PathDescriptor::haplotype)
        .def_readonly("sequence_name",
                      &gfaidx::PathDescriptor::sequence_name)
        .def_readonly("sequence_start",
                      &gfaidx::PathDescriptor::sequence_start)
        .def_readonly("sequence_end", &gfaidx::PathDescriptor::sequence_end)
        .def_readonly("step_count", &gfaidx::PathDescriptor::step_count)
        .def_readonly("tags", &gfaidx::PathDescriptor::tags);

    py::class_<gfaidx::CoordinateTrack>(module, "CoordinateTrack")
        .def_readonly("source_type", &gfaidx::CoordinateTrack::source_type)
        .def_readonly("reference", &gfaidx::CoordinateTrack::reference)
        .def_readonly("sequence", &gfaidx::CoordinateTrack::sequence)
        .def_readonly("haplotype", &gfaidx::CoordinateTrack::haplotype)
        .def_readonly("begin", &gfaidx::CoordinateTrack::begin)
        .def_readonly("end", &gfaidx::CoordinateTrack::end)
        .def_readonly("entry_count", &gfaidx::CoordinateTrack::entry_count);

    py::enum_<gfaidx::RegionMode>(module, "RegionMode")
        .value("BFS", gfaidx::RegionMode::bfs)
        .value("ALL_HAPLOTYPES", gfaidx::RegionMode::all_haplotypes);

    py::class_<gfaidx::IndexedGraph>(module, "IndexedGraph")
        .def(py::init<std::string, gfaidx::IndexPaths>(), py::arg("graph_path"),
             py::arg("index_paths") = gfaidx::IndexPaths{})
        .def_property_readonly("graph_path", &gfaidx::IndexedGraph::graph_path)
        .def_property_readonly("index_paths", &gfaidx::IndexedGraph::index_paths)
        .def_property_readonly("capabilities",
                               &gfaidx::IndexedGraph::capabilities)
        .def_property_readonly("node_count", &gfaidx::IndexedGraph::node_count)
        .def_property_readonly("community_count",
                               &gfaidx::IndexedGraph::community_count)
        .def("node_exists", &gfaidx::IndexedGraph::node_exists,
             py::arg("node_id"), py::call_guard<py::gil_scoped_release>())
        .def("community_id", &gfaidx::IndexedGraph::community_id,
             py::arg("node_id"), py::call_guard<py::gil_scoped_release>())
        .def("get_node", &gfaidx::IndexedGraph::get_node, py::arg("node_id"),
             py::call_guard<py::gil_scoped_release>())
        .def("neighbors", &gfaidx::IndexedGraph::neighbors,
             py::arg("node_id"), py::call_guard<py::gil_scoped_release>())
        .def("incident_edges", &gfaidx::IndexedGraph::incident_edges,
             py::arg("node_id"), py::call_guard<py::gil_scoped_release>())
        .def("get_community", &gfaidx::IndexedGraph::get_community,
             py::arg("community_id"), py::call_guard<py::gil_scoped_release>())
        .def("paths", &gfaidx::IndexedGraph::paths,
             py::call_guard<py::gil_scoped_release>())
        .def("get_path", &gfaidx::IndexedGraph::get_path,
             py::arg("path_key"), py::call_guard<py::gil_scoped_release>())
        .def("coordinate_tracks", &gfaidx::IndexedGraph::coordinate_tracks,
             py::call_guard<py::gil_scoped_release>())
        .def("path_steps", &gfaidx::IndexedGraph::path_steps,
             py::arg("path_key"), py::arg("start_step") = 0,
             py::arg("max_steps") = ~std::uint64_t{0},
             py::call_guard<py::gil_scoped_release>())
        .def("get_subgraph",
             [](const gfaidx::IndexedGraph& graph,
                const std::vector<std::string>& seeds, std::uint32_t max_nodes,
                bool include_paths, bool include_coordinates,
                std::uint32_t threads) {
                 return graph.get_subgraph(seeds, extraction_options(
                     max_nodes, include_paths, include_coordinates, threads));
             },
             py::arg("seeds"), py::arg("max_nodes") = 100,
             py::arg("include_paths") = true,
             py::arg("include_coordinates") = false,
             py::arg("threads") = 1,
             py::call_guard<py::gil_scoped_release>())
        .def("get_region",
             [](const gfaidx::IndexedGraph& graph, std::string reference,
                std::string sequence, std::uint64_t begin, std::uint64_t end,
                gfaidx::RegionMode mode,
                std::optional<std::uint64_t> haplotype_gap,
                std::uint32_t max_nodes, bool include_paths,
                bool include_coordinates, std::uint32_t threads) {
                 return graph.get_region(std::move(reference),
                     std::move(sequence), begin, end,
                     region_options(max_nodes, include_paths,
                                    include_coordinates, threads, mode,
                                    haplotype_gap));
             },
             py::arg("reference"), py::arg("sequence"), py::arg("begin"),
             py::arg("end"), py::arg("mode") = gfaidx::RegionMode::bfs,
             py::arg("haplotype_gap") = py::none(),
             py::arg("max_nodes") = 10000, py::arg("include_paths") = true,
             py::arg("include_coordinates") = false,
             py::arg("threads") = 1,
             py::call_guard<py::gil_scoped_release>())
        .def("get_path_subgraph",
             [](const gfaidx::IndexedGraph& graph, std::string key,
                std::uint64_t start, std::uint64_t maximum,
                std::uint32_t max_nodes, bool include_coordinates) {
                 auto options = extraction_options(max_nodes, true,
                                                   include_coordinates, 1);
                 return graph.get_path_subgraph(key, start, maximum, options);
             },
             py::arg("path_key"), py::arg("start_step") = 0,
             py::arg("max_steps") = ~std::uint64_t{0},
             py::arg("max_nodes") = 10000,
             py::arg("include_coordinates") = false,
             py::call_guard<py::gil_scoped_release>())
        .def("stream_subgraph",
             [](const gfaidx::IndexedGraph& graph,
                const std::vector<std::string>& seeds, py::object destination,
                std::uint32_t max_nodes, bool include_paths,
                bool include_coordinates, std::uint32_t threads) {
                 auto options = extraction_options(max_nodes, include_paths,
                                                   include_coordinates, threads);
                 stream_to_python(destination, [&](const auto& visitor) {
                     graph.stream_subgraph(seeds, visitor, options);
                 });
             },
             py::arg("seeds"), py::arg("destination"),
             py::arg("max_nodes") = 100,
             py::arg("include_paths") = true,
             py::arg("include_coordinates") = false,
             py::arg("threads") = 1)
        .def("stream_region",
             [](const gfaidx::IndexedGraph& graph, std::string reference,
                std::string sequence, std::uint64_t begin, std::uint64_t end,
                py::object destination, gfaidx::RegionMode mode,
                std::optional<std::uint64_t> haplotype_gap,
                std::uint32_t max_nodes, bool include_paths,
                bool include_coordinates, std::uint32_t threads) {
                 auto options = region_options(max_nodes, include_paths,
                     include_coordinates, threads, mode, haplotype_gap);
                 stream_to_python(destination, [&](const auto& visitor) {
                     graph.stream_region(reference, sequence, begin, end,
                                         visitor, options);
                 });
             },
             py::arg("reference"), py::arg("sequence"), py::arg("begin"),
             py::arg("end"), py::arg("destination"),
             py::arg("mode") = gfaidx::RegionMode::bfs,
             py::arg("haplotype_gap") = py::none(),
             py::arg("max_nodes") = 10000, py::arg("include_paths") = true,
             py::arg("include_coordinates") = false,
             py::arg("threads") = 1);
}

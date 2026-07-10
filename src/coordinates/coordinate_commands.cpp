#include "coordinates/coordinate_commands.h"

#include <algorithm>
#include <cstdint>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

#include "chunk/get_subgraph_command.h"
#include "coordinates/coordinate_index.h"
#include "fs/fs_helpers.h"
#include "paths/path_index.h"
#include "utils/Timer.h"

namespace gfaidx::coordinates {
namespace {

struct ParsedRegion {
    std::string sequence;
    std::uint64_t begin{};
    std::uint64_t end{};
};

std::string infer_companion_path(const std::string& graph_path, std::string_view suffix) {
    return graph_path + std::string(suffix);
}

std::uint64_t parse_u64_arg(const std::string& value, std::string_view field_name) {
    // Permit visual comma separators in CLI regions such as chr22:1,000-2,000.
    std::string normalized = value;
    normalized.erase(std::remove(normalized.begin(), normalized.end(), ','), normalized.end());
    try {
        if (normalized.empty() || normalized.front() == '-') {
            throw std::invalid_argument("value must be non-negative");
        }
        std::size_t consumed = 0;
        const auto parsed = std::stoull(normalized, &consumed);
        if (consumed != normalized.size()) throw std::invalid_argument("trailing characters");
        return parsed;
    } catch (const std::exception& err) {
        throw std::runtime_error("Invalid " + std::string(field_name) +
                                 " value '" + value + "': " + err.what());
    }
}

std::uint32_t parse_max_nodes(const std::string& value) {
    const auto parsed = parse_u64_arg(value, "--max_nodes");
    if (parsed == 0 || parsed > std::numeric_limits<std::uint32_t>::max()) {
        throw std::runtime_error("--max_nodes must be in the uint32 range and greater than zero");
    }
    return static_cast<std::uint32_t>(parsed);
}

ParsedRegion parse_region(const std::string& region) {
    // Region strings use the same 0-based, half-open coordinates stored by W
    // records: sequence:start-end.
    const auto colon = region.rfind(':');
    const auto dash = colon == std::string::npos
        ? std::string::npos
        : region.find('-', colon + 1);
    if (colon == std::string::npos || dash == std::string::npos ||
        colon == 0 || dash <= colon + 1 || dash + 1 >= region.size()) {
        throw std::runtime_error("Region must have the form sequence:start-end");
    }

    ParsedRegion out;
    out.sequence = region.substr(0, colon);
    out.begin = parse_u64_arg(region.substr(colon + 1, dash - (colon + 1)),
                              "region start");
    out.end = parse_u64_arg(region.substr(dash + 1), "region end");
    if (out.end <= out.begin) {
        throw std::runtime_error("Region end must be greater than region start");
    }
    return out;
}

Reader::Options parse_reader_options(const argparse::ArgumentParser& program) {
    Reader::Options options;
    const auto value = program.get<std::string>("progress_every");
    try {
        options.progress_every = parse_u64_arg(value, "--progress_every");
    } catch (const std::exception& err) {
        std::cerr << "Warning: " << err.what()
                  << "; using default 1000000" << std::endl;
        options.progress_every = 1000000;
    }
    return options;
}

}  // namespace

void configure_index_coordinates_parser(argparse::ArgumentParser& parser) {
    parser.add_argument("in_gfa")
      .help("input GFA or indexed GFA gzip; S lines provide lengths, and W/P records may come from the GFA or .pdx");

    parser.add_argument("out_index")
      .help("output standalone coordinate index (.cdx)");

    parser.add_argument("--ndx")
      .default_value(std::string(""))
      .nargs(1)
      .help("node hash index whose sorted ranks must align with the coordinate index; defaults to <in_gfa>.ndx when present");

    parser.add_argument("--pdx")
      .default_value(std::string(""))
      .nargs(1)
      .help("optional path index to source selected P/W records or fallback reference W records; defaults to <in_gfa>.pdx when present");

    parser.add_argument("--reference")
      .default_value(std::string(""))
      .nargs(1)
      .help("optional RS:Z reference sample to index; defaults to every listed reference");

    parser.add_argument("--path_names_file")
      .default_value(std::string(""))
      .nargs(1)
      .help("optional file produced by get_path --print_path_names; selected P paths and W walks are indexed from the .pdx");

    parser.add_argument("--progress_every")
      .default_value(std::string("1000000"))
      .nargs(1)
      .help("print reader progress every N lines; 0 disables progress logging");
}

int run_index_coordinates(const argparse::ArgumentParser& program) {
    const auto input_gfa = program.get<std::string>("in_gfa");
    const auto output_index = program.get<std::string>("out_index");
    auto node_index = program.get<std::string>("ndx");
    auto path_index = program.get<std::string>("pdx");
    const auto reference = program.get<std::string>("reference");
    const auto path_names_file = program.get<std::string>("path_names_file");

    if (!file_exists(input_gfa.c_str())) {
        std::cerr << "Input GFA does not exist: " << input_gfa << std::endl;
        return 1;
    }
    if (node_index.empty()) {
        const auto inferred = infer_companion_path(input_gfa, ".ndx");
        if (file_exists(inferred.c_str())) node_index = inferred;
    }
    if (path_index.empty()) {
        const auto inferred = infer_companion_path(input_gfa, ".pdx");
        if (file_exists(inferred.c_str())) path_index = inferred;
    }
    if (node_index.empty() || !file_exists(node_index.c_str())) {
        std::cerr << "Provide an existing --ndx aligned to the input GFA" << std::endl;
        return 1;
    }
    if (!path_index.empty() && !file_exists(path_index.c_str())) {
        std::cerr << "Path index does not exist: " << path_index << std::endl;
        return 1;
    }
    if (!path_names_file.empty() && !file_exists(path_names_file.c_str())) {
        std::cerr << "Path/walk names file does not exist: " << path_names_file << std::endl;
        return 1;
    }
    if (!path_names_file.empty() && path_index.empty()) {
        std::cerr << "--path_names_file requires an existing --pdx or companion <in_gfa>.pdx" << std::endl;
        return 1;
    }
    if (!path_names_file.empty() && !reference.empty()) {
        std::cerr << "Use either --path_names_file or --reference; the names file is already an explicit selection" << std::endl;
        return 1;
    }
    if (file_exists(output_index.c_str())) {
        std::cerr << "Output coordinate index already exists: " << output_index << std::endl;
        return 1;
    }

    try {
        Timer timer;
        std::cout << "Building coordinate index " << output_index << std::endl;
        build_coordinate_index(input_gfa,
                               output_index,
                               node_index,
                               reference,
                               parse_reader_options(program),
                               path_index,
                               path_names_file);

        // Reopen the completed file to validate its header and report exactly
        // what was published without retaining builder-only vectors in memory.
        CoordinateIndexReader index(output_index);
        std::uint64_t entries = 0;
        for (const auto& track : index.tracks()) entries += track.entry_count;
        std::cout << "Indexed " << index.tracks().size() << " coordinate tracks with "
                  << entries << " reference steps in " << timer.elapsed()
                  << " seconds" << std::endl;
    } catch (const std::exception& err) {
        std::cerr << err.what() << std::endl;
        return 1;
    }
    return 0;
}

void configure_get_region_parser(argparse::ArgumentParser& parser) {
    parser.add_argument("in_gz")
      .help("input indexed multi-member GFA gzip");

    parser.add_argument("region")
      .help("0-based half-open reference interval in sequence:start-end form");

    parser.add_argument("out_gfa")
      .help("output extracted GFA subgraph");

    parser.add_argument("--reference")
      .default_value(std::string(""))
      .nargs(1)
      .help("reference sample name; may be omitted when the sequence is unambiguous");

    parser.add_argument("--cdx")
      .default_value(std::string(""))
      .nargs(1)
      .help("coordinate index; defaults to <in_gz>.cdx");

    parser.add_argument("--idx")
      .default_value(std::string(""))
      .nargs(1)
      .help("chunk index override; defaults to <in_gz>.idx");

    parser.add_argument("--ndx")
      .default_value(std::string(""))
      .nargs(1)
      .help("node index override; defaults to <in_gz>.ndx");

    parser.add_argument("--pdx")
      .default_value(std::string(""))
      .nargs(1)
      .help("path index used for rank-to-name conversion; defaults to <in_gz>.pdx");

    parser.add_argument("--lnx")
      .default_value(std::string(""))
      .nargs(1)
      .help("node length index for coordinate-bearing path output; defaults to <in_gz>.lnx when present");

    parser.add_argument("--max_nodes")
      .default_value(std::string("10000"))
      .nargs(1)
      .help("maximum total seeds plus BFS neighborhood nodes; defaults to 10000");

    parser.add_argument("--no_paths").default_value(false)
      .implicit_value(true)
      .help("skip P/W subpath output; .pdx is still required to resolve coordinate ranks");

    parser.add_argument("--with_walk_coordinates", "--with_walk_coords").default_value(false)
      .implicit_value(true)
      .help("emit W subpaths with SeqStart/SeqEnd and P subpaths with path-local coordinate names");

    parser.add_argument("--debug_trace").default_value(false)
      .implicit_value(true)
      .help("enable detailed cross-index tracing for subgraph extraction");
}

int run_get_region(const argparse::ArgumentParser& program) {
    try {
        const auto input_gz = program.get<std::string>("in_gz");
        const auto region = parse_region(program.get<std::string>("region"));
        const auto reference = program.get<std::string>("reference");
        const bool no_paths = program.get<bool>("no_paths");
        const bool with_walk_coordinates = program.get<bool>("with_walk_coordinates");
        if (no_paths && with_walk_coordinates) {
            throw std::runtime_error("--with_walk_coordinates requires path output; remove --no_paths");
        }

        auto cdx_path = program.get<std::string>("cdx");
        auto pdx_path = program.get<std::string>("pdx");
        auto lnx_path = program.get<std::string>("lnx");
        const bool lnx_explicit = !lnx_path.empty();
        if (cdx_path.empty()) cdx_path = infer_companion_path(input_gz, ".cdx");
        if (pdx_path.empty()) pdx_path = infer_companion_path(input_gz, ".pdx");
        if (lnx_path.empty()) lnx_path = infer_companion_path(input_gz, ".lnx");
        if (!file_exists(cdx_path.c_str())) {
            throw std::runtime_error("Coordinate index does not exist: " + cdx_path);
        }
        if (!file_exists(pdx_path.c_str())) {
            throw std::runtime_error("Path index required for coordinate rank lookup does not exist: " +
                                     pdx_path);
        }
        if (with_walk_coordinates && lnx_explicit && !file_exists(lnx_path.c_str())) {
            throw std::runtime_error("Node length index does not exist: " + lnx_path);
        }

        CoordinateIndexReader coordinate_index(cdx_path);
        paths::PathIndexReader path_index(pdx_path);
        if (coordinate_index.node_count() != path_index.node_count()) {
            throw std::runtime_error(".cdx and .pdx node counts differ; rebuild them against the same .ndx");
        }

        const auto ranks = coordinate_index.query_node_ranks(reference,
                                                              region.sequence,
                                                              region.begin,
                                                              region.end);
        if (ranks.empty()) {
            throw std::runtime_error("No reference nodes overlap the requested coordinate interval");
        }

        // PathIndexReader owns the rank-aligned node names already present in
        // .pdx, so the coordinate sidecar never duplicates those strings.
        std::vector<std::string> seed_nodes;
        seed_nodes.reserve(ranks.size());
        for (const auto rank : ranks) {
            seed_nodes.emplace_back(path_index.get_node_name(rank));
        }
        std::cout << "Coordinate query selected " << seed_nodes.size()
                  << " reference seed nodes" << std::endl;

        chunk::SubgraphExtractionOptions options;
        options.input_gz = input_gz;
        options.output_gfa = program.get<std::string>("out_gfa");
        options.idx_path = program.get<std::string>("idx");
        options.ndx_path = program.get<std::string>("ndx");
        options.pdx_path = pdx_path;
        // Empty means "fall back to the old S-line scan" inside the shared
        // subgraph extractor. Explicit missing --lnx is rejected above.
        options.lnx_path = file_exists(lnx_path.c_str()) ? lnx_path : std::string{};
        options.max_nodes = parse_max_nodes(program.get<std::string>("max_nodes"));
        options.include_paths = !no_paths;
        options.with_walk_coordinates = with_walk_coordinates;
        options.debug_trace = program.get<bool>("debug_trace");
        return chunk::extract_subgraph_from_seeds(options, seed_nodes);
    } catch (const std::exception& err) {
        std::cerr << err.what() << std::endl;
        return 1;
    }
}

}  // namespace gfaidx::coordinates

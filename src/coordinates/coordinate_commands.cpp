#include "coordinates/coordinate_commands.h"

#include <cstdint>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "chunk/get_subgraph_command.h"
#include "coordinates/coordinate_index.h"
#include "coordinates/path_haplotype_query.h"
#include "coordinates/path_coordinate_query.h"
#include "fs/fs_helpers.h"
#include "paths/path_index.h"
#include "utils/Timer.h"
#include "utils/cli_helpers.h"

namespace gfaidx::coordinates {
namespace {

struct ParsedRegion {
    std::string sequence;
    std::uint64_t begin{};
    std::uint64_t end{};
};

std::string coordinate_walk_key(const CoordinateTrackInfo& track) {
    return track.reference_name + "|" + std::to_string(track.haplotype) + "|" +
           track.sequence_name + "|" + std::to_string(track.sequence_start) +
           "|" + std::to_string(track.sequence_end);
}

std::vector<paths::SubpathRun> resolve_coordinate_path_runs(
    const paths::PathIndexReader& path_index,
    const CoordinateQueryResult& query) {
    std::vector<paths::SubpathRun> runs;
    runs.reserve(query.slices.size());

    for (const auto& slice : query.slices) {
        if (slice.track.source_type == 'S') {
            // rGFA segment tracks have a coordinate namespace but do not
            // necessarily correspond to one indexed P/W record.
            continue;
        }

        const auto lookup_name = slice.track.source_type == 'P'
            ? slice.track.sequence_name
            : coordinate_walk_key(slice.track);
        std::uint32_t path_id = 0;
        if (!path_index.lookup_path_id(lookup_name, path_id)) {
            throw std::runtime_error(
                "Coordinate P/W track does not exist in the supplied .pdx: " +
                lookup_name);
        }

        const auto info = path_index.get_path_info(path_id);
        if (info.record_type != slice.track.source_type ||
            info.step_count != slice.track.entry_count ||
            slice.start_step > info.step_count ||
            slice.node_ranks.size() > info.step_count - slice.start_step) {
            throw std::runtime_error(
                "Coordinate P/W track is not aligned with the supplied .pdx: " +
                lookup_name);
        }

        // CDX entries for P/W tracks are written in original path-step order,
        // so the binary-search offsets are already exact .pdx subpath bounds.
        runs.push_back(paths::SubpathRun{
            path_id,
            slice.start_step,
            static_cast<std::uint64_t>(slice.node_ranks.size()),
        });
    }
    return runs;
}

void write_coordinate_tracks(std::ostream& out,
                             const CoordinateIndexReader& index,
                             bool with_header) {
    if (with_header) {
        out << "source\treference\thaplotype\tsequence\tstart\tend\tentries\n";
    }

    // The .cdx stores one row per continuous coordinate track fragment. W rows
    // use the original walk sample/haplotype/sequence namespace, P rows use the
    // path name as the sequence, and S rows come from rGFA SN/SO/SR tags.
    for (const auto& track : index.tracks()) {
        out << track.source_type << '\t'
            << track.reference_name << '\t'
            << track.haplotype << '\t'
            << track.sequence_name << '\t'
            << track.sequence_start << '\t'
            << track.sequence_end << '\t'
            << track.entry_count << '\n';
    }
}

std::uint32_t parse_max_nodes(const std::string& value) {
    return utils::parse_u32_strict(value,
                                   "--max_nodes",
                                   1,
                                   std::numeric_limits<std::uint32_t>::max(),
                                   true);
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
    out.begin = utils::parse_u64_strict(region.substr(colon + 1, dash - (colon + 1)),
                                        "region start",
                                        true);
    out.end = utils::parse_u64_strict(region.substr(dash + 1), "region end", true);
    if (out.end <= out.begin) {
        throw std::runtime_error("Region end must be greater than region start");
    }
    return out;
}

Reader::Options parse_reader_options(const argparse::ArgumentParser& program) {
    Reader::Options options;
    const auto value = program.get<std::string>("progress_every");
    try {
        options.progress_every = utils::parse_u64_strict(value, "--progress_every");
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
        const auto inferred = utils::companion_path(input_gfa, ".ndx");
        if (file_exists(inferred.c_str())) node_index = inferred;
    }
    if (path_index.empty()) {
        const auto inferred = utils::companion_path(input_gfa, ".pdx");
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
      .default_value(std::string(""))
      .nargs(argparse::nargs_pattern::optional)
      .help("0-based half-open reference interval in sequence:start-end form");

    parser.add_argument("out_gfa")
      .default_value(std::string(""))
      .nargs(argparse::nargs_pattern::optional)
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

    parser.add_argument("--pcx")
      .default_value(std::string(""))
      .nargs(1)
      .help("path coordinate checkpoints for faster coordinate-bearing path output; defaults to <in_gz>.pcx when present");

    parser.add_argument("--max_nodes")
      .default_value(std::string("10000"))
      .nargs(1)
      .help("maximum total seeds plus BFS nodes; not used with --all_haplotypes");

    parser.add_argument("--all_haplotypes").default_value(false)
      .implicit_value(true)
      .help("select the exact reference interval and anchor-supported P/W spans instead of BFS");

    parser.add_argument("--no_paths").default_value(false)
      .implicit_value(true)
      .help("skip P/W subpath output; .pdx is still required to resolve coordinate ranks");

    parser.add_argument("--with_coords").default_value(false)
      .implicit_value(true)
      .help("emit coordinate-bearing W/P subpaths");

    parser.add_argument("--print_path_names").default_value(false)
      .implicit_value(true)
      .help("print coordinate tracks available in the .cdx, then exit");

    parser.add_argument("--no_header").default_value(false)
      .implicit_value(true)
      .help("omit the TSV header when used with --print_path_names");

    parser.add_argument("--debug_trace").default_value(false)
      .implicit_value(true)
      .help("enable detailed cross-index tracing for subgraph extraction");
}

int run_get_region(const argparse::ArgumentParser& program) {
    try {
        const auto input_gz = program.get<std::string>("in_gz");
        const auto reference = program.get<std::string>("reference");
        const bool print_path_names = program.get<bool>("print_path_names");
        const bool no_paths = program.get<bool>("no_paths");
        const bool with_coords = program.get<bool>("with_coords");
        const bool all_haplotypes = program.get<bool>("all_haplotypes");

        auto cdx_path = program.get<std::string>("cdx");
        auto pdx_path = program.get<std::string>("pdx");
        auto lnx_path = program.get<std::string>("lnx");
        auto pcx_path = program.get<std::string>("pcx");
        const bool lnx_explicit = !lnx_path.empty();
        const bool pcx_explicit = !pcx_path.empty();
        if (cdx_path.empty()) {
            cdx_path = utils::resolve_sidecar_path(input_gz, cdx_path, ".cdx", true);
        }
        if (pdx_path.empty()) pdx_path = utils::companion_path(input_gz, ".pdx");
        if (lnx_path.empty()) lnx_path = utils::companion_path(input_gz, ".lnx");
        if (pcx_path.empty()) pcx_path = utils::companion_path(input_gz, ".pcx");
        if (print_path_names) {
            if (!file_exists(cdx_path.c_str())) {
                throw std::runtime_error("Coordinate index does not exist: " + cdx_path);
            }
            CoordinateIndexReader coordinate_index(cdx_path);
            write_coordinate_tracks(std::cout, coordinate_index, !program.get<bool>("no_header"));
            return 0;
        }

        if (no_paths && with_coords) {
            throw std::runtime_error("--with_coords requires path output; remove --no_paths");
        }

        const auto region_arg = program.get<std::string>("region");
        const auto output_gfa = program.get<std::string>("out_gfa");
        if (region_arg.empty() || output_gfa.empty()) {
            throw std::runtime_error("get_region requires <sequence:start-end> and <out_gfa> unless --print_path_names is used");
        }

        const auto region = parse_region(region_arg);
        if (!file_exists(pdx_path.c_str())) {
            throw std::runtime_error("Path index required for coordinate rank lookup does not exist: " +
                                     pdx_path);
        }
        if (with_coords && lnx_explicit && !file_exists(lnx_path.c_str())) {
            throw std::runtime_error("Node length index does not exist: " + lnx_path);
        }
        if (with_coords && pcx_explicit && !file_exists(pcx_path.c_str())) {
            throw std::runtime_error("Path checkpoint index does not exist: " + pcx_path);
        }

        paths::PathIndexReader path_index(pdx_path);

        std::vector<std::uint32_t> ranks;
        std::vector<std::uint32_t> ordered_reference_ranks;
        std::vector<paths::SubpathRun> exact_reference_path_runs;
        bool used_coordinate_index = false;
        bool coordinate_index_returned_empty = false;
        std::string coordinate_index_error;
        if (file_exists(cdx_path.c_str())) {
            CoordinateIndexReader coordinate_index(cdx_path);
            if (coordinate_index.node_count() != path_index.node_count()) {
                throw std::runtime_error(".cdx and .pdx node counts differ; rebuild them against the same .ndx");
            }
            try {
                auto query = coordinate_index.query_region(reference,
                                                            region.sequence,
                                                            region.begin,
                                                            region.end);
                std::vector<std::uint32_t> query_ordered_ranks;
                for (const auto& slice : query.slices) {
                    query_ordered_ranks.insert(
                        query_ordered_ranks.end(),
                        slice.node_ranks.begin(),
                        slice.node_ranks.end());
                }
                std::vector<paths::SubpathRun> query_reference_runs;
                if (all_haplotypes) {
                    query_reference_runs =
                        resolve_coordinate_path_runs(path_index, query);
                }

                // Publish the query state only after P/W-to-PDX validation has
                // also succeeded, so a mismatched sidecar cannot leave a
                // partially usable rank result behind.
                ranks = std::move(query.node_ranks);
                ordered_reference_ranks = std::move(query_ordered_ranks);
                exact_reference_path_runs = std::move(query_reference_runs);
                used_coordinate_index = !ranks.empty();
                coordinate_index_returned_empty = ranks.empty();
            } catch (const std::exception& err) {
                coordinate_index_error = err.what();
            }
        }

        if (ranks.empty()) {
            try {
                const auto fallback = query_path_coordinates_on_the_fly(path_index,
                                                                        file_exists(lnx_path.c_str()) ? lnx_path : std::string{},
                                                                        reference,
                                                                        region.sequence,
                                                                        region.begin,
                                                                        region.end);
                ranks = fallback.node_ranks;
                ordered_reference_ranks = fallback.ordered_node_ranks;
                exact_reference_path_runs = fallback.reference_path_runs;
                std::cout << "On-the-fly path coordinate query selected "
                          << ranks.size() << " seed nodes from "
                          << fallback.matched_path_count << " indexed P/W records" << std::endl;
            } catch (const std::exception& err) {
                if (coordinate_index_returned_empty) {
                    throw std::runtime_error("No reference nodes overlap the requested coordinate interval");
                }
                if (!coordinate_index_error.empty()) {
                    throw std::runtime_error(std::string(err.what()) +
                                             "; .cdx lookup also failed: " +
                                             coordinate_index_error);
                }
                throw;
            }
        }

        if (used_coordinate_index) {
            std::cout << get_time() << "Coordinate query selected " << ranks.size()
                      << " reference seed nodes" << std::endl;
        }

        // Both selection modes share the same index overrides and output
        // controls. Only the node-selection strategy changes below.
        chunk::SubgraphExtractionOptions options;
        options.input_gz = input_gz;
        options.output_gfa = output_gfa;
        options.idx_path = program.get<std::string>("idx");
        options.ndx_path = program.get<std::string>("ndx");
        options.pdx_path = pdx_path;
        // Empty means "fall back to the old S-line scan" inside the shared
        // subgraph extractor. Explicit missing --lnx is rejected above.
        options.lnx_path = file_exists(lnx_path.c_str()) ? lnx_path : std::string{};
        // Missing inferred .pcx files preserve compatibility with existing
        // indexes by using the previous bounded path-prefix scan.
        options.pcx_path = file_exists(pcx_path.c_str()) ? pcx_path : std::string{};
        options.max_nodes = parse_max_nodes(program.get<std::string>("max_nodes"));
        options.include_paths = !no_paths;
        options.with_walk_coordinates = with_coords;
        options.debug_trace = program.get<bool>("debug_trace");

        if (all_haplotypes) {
            // Use the .pdx posting table as an inverted index from every
            // ordered reference interval anchor to its path occurrences. Exact
            // source bounds and repeat-aware path bounds avoid BFS.
            const auto selection =
                query_path_haplotype_nodes(path_index,
                                           ordered_reference_ranks,
                                           exact_reference_path_runs);
            std::cout << get_time() << "All-haplotype path selection read "
                      << selection.posting_count << " postings across "
                      << selection.matched_path_count << " P/W records and selected "
                      << selection.node_ranks.size() << " unique nodes from "
                      << selection.selected_path_step_count << " path steps"
                      << std::endl;
            if (selection.exact_reference_path_count > 0 ||
                selection.repeat_chained_path_count > 0 ||
                selection.repeat_fallback_path_count > 0) {
                std::cout << get_time() << "All-haplotype interval resolution preserved "
                          << selection.exact_reference_path_count
                          << " exact coordinate path(s), chained "
                          << selection.repeat_chained_path_count
                          << " path(s) with repeated anchors, and retained broad bounds for "
                          << selection.repeat_fallback_path_count
                          << " underdetermined path(s)" << std::endl;
            }
            return chunk::extract_subgraph_from_node_ranks(
                options,
                selection.node_ranks,
                selection.path_runs,
                path_index);
        }

        // BFS still uses original node-name strings. Convert only the reference
        // seed ranks selected by the coordinate query.
        std::vector<std::string> seed_nodes;
        seed_nodes.reserve(ranks.size());
        for (const auto rank : ranks) {
            seed_nodes.emplace_back(path_index.get_node_name(rank));
        }
        return chunk::extract_subgraph_from_seeds(options, seed_nodes);
    } catch (const std::exception& err) {
        std::cerr << err.what() << std::endl;
        return 1;
    }
}

}  // namespace gfaidx::coordinates

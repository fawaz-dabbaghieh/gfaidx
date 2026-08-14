#include "coordinates/coordinate_commands.h"

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "chunk/get_subgraph_command.h"
#include "coordinates/coordinate_index.h"
#include "coordinates/path_coordinate_query.h"
#include "coordinates/path_haplotype_query.h"
#include "fs/fs_helpers.h"
#include "indexer/node_length_index.h"
#include "paths/p_path_coordinates.h"
#include "paths/path_index.h"
#include "utils/Timer.h"
#include "utils/cli_helpers.h"
#include "gfaidx/indexed_graph.hpp"

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

std::string coordinate_track_path_key(const CoordinateTrackInfo& track) {
    if (track.source_type == 'P') {
        return "P\t" + track.sequence_name;
    }
    if (track.source_type == 'W') {
        return "W\t" + coordinate_walk_key(track);
    }
    return {};
}

std::string indexed_path_key(const paths::PathInfo& info) {
    return std::string(1, info.record_type) + "\t" + std::string(info.name);
}

struct CoordinateTrackLookup {
    std::string path_key;
    std::size_t track_index{};
};

std::string_view coordinate_sequence_name(const CoordinateTrackInfo& track) {
    // Keep the raw P name in .cdx for exact .pdx identity, but present and query
    // its terminal :start-end suffix as one fragment of the base namespace.
    if (track.source_type == 'P') {
        return paths::parse_p_path_coordinate_name(track.sequence_name)
            .coordinate_name;
    }
    return track.sequence_name;
}

bool coordinate_track_uses_current_p_semantics(
    const CoordinateTrackInfo& track) {
    if (track.source_type != 'P') return true;
    const auto parsed =
        paths::parse_p_path_coordinate_name(track.sequence_name);
    return !parsed.has_coordinates ||
           (track.sequence_start == parsed.start &&
            track.sequence_end == parsed.end);
}

void write_coordinate_value(std::ostream& out, std::int64_t value) {
    if (value >= 0) {
        out << value;
    } else {
        out << '*';
    }
}

void write_available_coordinate_paths(
    std::ostream& out,
    const paths::PathIndexReader* path_index,
    const CoordinateIndexReader* coordinate_index,
    bool on_the_fly_available,
    bool with_header) {

    if (with_header) {
        out << "source\treference\thaplotype\tsequence\tstart\tend\tentries"
               "\tcoordinate_access\n";
    }

    // Keep a sorted vector instead of a hash table because coordinate-track
    // metadata is normally small and each P/W path needs only one lookup.
    std::vector<CoordinateTrackLookup> track_lookup;
    if (coordinate_index != nullptr) {
        track_lookup.reserve(coordinate_index->tracks().size());
        for (std::size_t i = 0; i < coordinate_index->tracks().size(); ++i) {
            const auto key =
                coordinate_track_path_key(coordinate_index->tracks()[i]);
            if (!key.empty()) {
                track_lookup.push_back(CoordinateTrackLookup{key, i});
            }
        }
        std::sort(track_lookup.begin(),
                  track_lookup.end(),
                  [](const auto& lhs, const auto& rhs) {
                      return lhs.path_key < rhs.path_key;
                  });
    }

    std::vector<std::string> emitted_cdx_keys;
    emitted_cdx_keys.reserve(track_lookup.size());
    if (path_index != nullptr) {
        for (std::uint32_t path_id = 0;
             path_id < path_index->path_count();
             ++path_id) {
            const auto info = path_index->get_path_info(path_id);
            const auto path_key = indexed_path_key(info);
            const auto found = std::lower_bound(
                track_lookup.begin(),
                track_lookup.end(),
                path_key,
                [](const CoordinateTrackLookup& entry, const std::string& key) {
                    return entry.path_key < key;
                });
            const CoordinateTrackInfo* accelerated_track = nullptr;
            if (found != track_lookup.end() && found->path_key == path_key) {
                const auto& candidate =
                    coordinate_index->tracks()[found->track_index];
                // An older .cdx may have stored a suffixed P path from zero.
                // Leave it unaccelerated so queries use the corrected fallback.
                if (coordinate_track_uses_current_p_semantics(candidate)) {
                    accelerated_track = &candidate;
                }
                emitted_cdx_keys.push_back(path_key);
            }

            if (accelerated_track != nullptr) {
                // Reuse the exact coordinate bounds already stored in .cdx.
                const auto& track = *accelerated_track;
                out << track.source_type << '\t'
                    << track.reference_name << '\t'
                    << track.haplotype << '\t'
                    << coordinate_sequence_name(track) << '\t'
                    << track.sequence_start << '\t'
                    << track.sequence_end << '\t'
                    << track.entry_count << "\tcdx\n";
                continue;
            }

            if (info.record_type == 'W') {
                out << "W\t" << info.sample_id << '\t' << info.hap_index
                    << '\t' << info.seq_id << '\t';
                write_coordinate_value(out, info.seq_start);
                out << '\t';
                write_coordinate_value(out, info.seq_end);
                out << '\t' << info.step_count << '\t';
                const bool has_walk_coordinates =
                    info.seq_start >= 0 && info.seq_end >= info.seq_start;
                out << (on_the_fly_available && has_walk_coordinates
                            ? "on_the_fly"
                            : "unavailable")
                    << '\n';
            } else {
                // A terminal coordinate suffix supplies exact bounds without a
                // step scan. Unsuffixed paths retain the local start of zero and
                // an unknown end because .pdx does not store their total length.
                const auto parsed =
                    paths::parse_p_path_coordinate_name(info.name);
                out << "P\t\t0\t" << parsed.coordinate_name << '\t'
                    << parsed.start << '\t';
                if (parsed.has_coordinates) {
                    out << parsed.end;
                } else {
                    out << '*';
                }
                out << '\t' << info.step_count << '\t'
                    << (on_the_fly_available ? "on_the_fly" : "unavailable")
                    << '\n';
            }
        }
    }

    // Preserve .cdx-only coordinate namespaces, primarily rGFA S/SN/SO
    // tracks. P/W tracks absent from the supplied .pdx are also shown rather
    // than silently disappearing from the previous listing behavior.
    std::sort(emitted_cdx_keys.begin(), emitted_cdx_keys.end());
    emitted_cdx_keys.erase(
        std::unique(emitted_cdx_keys.begin(), emitted_cdx_keys.end()),
        emitted_cdx_keys.end());
    if (coordinate_index != nullptr) {
        for (const auto& track : coordinate_index->tracks()) {
            const auto path_key = coordinate_track_path_key(track);
            if (!path_key.empty() &&
                std::binary_search(emitted_cdx_keys.begin(),
                                   emitted_cdx_keys.end(),
                                   path_key)) {
                continue;
            }
            out << track.source_type << '\t'
                << track.reference_name << '\t'
                << track.haplotype << '\t'
                << coordinate_sequence_name(track) << '\t'
                << track.sequence_start << '\t'
                << track.sequence_end << '\t'
                << track.entry_count << '\t'
                << (coordinate_track_uses_current_p_semantics(track)
                        ? "cdx"
                        : "stale_cdx")
                << '\n';
        }
    }
}

std::uint32_t parse_max_nodes(const std::string& value) {
    return utils::parse_u32_strict(value,
                                   "--max_nodes",
                                   1,
                                   std::numeric_limits<std::uint32_t>::max(),
                                   true);
}

std::optional<std::uint64_t> parse_haplotype_gap_bases(
    const std::string& value) {
    // An empty parser default distinguishes an omitted flag from an explicit
    // zero, which means that only directly adjacent anchor occurrences join.
    if (value.empty()) return std::nullopt;

    std::string normalized = value;
    std::transform(normalized.begin(), normalized.end(), normalized.begin(),
                   [](const unsigned char ch) {
                       return static_cast<char>(std::tolower(ch));
                   });

    // Use decimal SI units, matching common genomic command-line notation.
    // Bare values and the bp suffix are already measured in bases.
    std::uint64_t multiplier = 1;
    std::size_t suffix_size = 0;
    if (utils::has_suffix(normalized, "kb")) {
        multiplier = 1000ULL;
        suffix_size = 2;
    } else if (utils::has_suffix(normalized, "mb")) {
        multiplier = 1000ULL * 1000ULL;
        suffix_size = 2;
    } else if (utils::has_suffix(normalized, "gb")) {
        multiplier = 1000ULL * 1000ULL * 1000ULL;
        suffix_size = 2;
    } else if (utils::has_suffix(normalized, "bp")) {
        suffix_size = 2;
    }

    const auto amount = utils::parse_u64_strict(
        normalized.substr(0, normalized.size() - suffix_size),
        "--haplotype_gap",
        true);
    if (amount > std::numeric_limits<std::uint64_t>::max() / multiplier) {
        throw std::runtime_error("--haplotype_gap is too large");
    }
    return amount * multiplier;
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

    parser.add_argument("--threads")
      .default_value(std::string("1"))
      .nargs(1)
      .help("number of ordered P/W formatting workers (default: 1)");

    parser.add_argument("--all_haplotypes").default_value(false)
      .implicit_value(true)
      .help("select the exact reference interval and anchor-supported P/W spans instead of BFS");

    parser.add_argument("--haplotype_gap")
      .default_value(std::string(""))
      .nargs(1)
      .help("optional maximum unanchored gap for local --all_haplotypes runs; accepts bases or bp/kb/mb/gb suffixes");

    parser.add_argument("--no_paths").default_value(false)
      .implicit_value(true)
      .help("skip P/W subpath output; .pdx is still required to resolve coordinate ranks");

    parser.add_argument("--with_coords").default_value(false)
      .implicit_value(true)
      .help("emit coordinate-bearing W/P subpaths");

    parser.add_argument("--print_path_names").default_value(false)
      .implicit_value(true)
      .help("print coordinate-queryable P/W paths and rGFA .cdx tracks, then exit");

    parser.add_argument("--list_coordinates").default_value(false)
      .implicit_value(true)
      .help("alias for --print_path_names that lists all coordinate-queryable tracks");

    parser.add_argument("--no_header").default_value(false)
      .implicit_value(true)
      .help("omit the TSV header when listing coordinate tracks");

    parser.add_argument("--debug_trace").default_value(false)
      .implicit_value(true)
      .help("enable detailed cross-index tracing for subgraph extraction");
}

int run_get_region(const argparse::ArgumentParser& program) {
    try {
        const auto input_gz = program.get<std::string>("in_gz");
        const auto reference = program.get<std::string>("reference");
        const bool list_coordinates =
            program.get<bool>("print_path_names") ||
            program.get<bool>("list_coordinates");
        const bool no_paths = program.get<bool>("no_paths");
        const bool with_coords = program.get<bool>("with_coords");
        const bool all_haplotypes = program.get<bool>("all_haplotypes");
        const auto haplotype_gap_bases = parse_haplotype_gap_bases(
            program.get<std::string>("haplotype_gap"));

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
        if (list_coordinates) {
            const bool has_pdx = file_exists(pdx_path.c_str());
            const bool has_cdx = file_exists(cdx_path.c_str());
            if (!has_pdx && !has_cdx) {
                throw std::runtime_error(
                    "No path or coordinate index is available. Expected " +
                    pdx_path + " or " + cdx_path +
                    ". Build rGFA/reference coordinates with: gfaidx "
                    "index_coordinates " + input_gz + " " + cdx_path);
            }

            std::unique_ptr<paths::PathIndexReader> path_index;
            if (has_pdx) {
                path_index =
                    std::make_unique<paths::PathIndexReader>(pdx_path);
            }
            std::unique_ptr<CoordinateIndexReader> coordinate_index;
            if (has_cdx) {
                coordinate_index =
                    std::make_unique<CoordinateIndexReader>(cdx_path);
                if (path_index != nullptr &&
                    coordinate_index->node_count() !=
                        path_index->node_count()) {
                    throw std::runtime_error(
                        ".cdx and .pdx node counts differ; rebuild them "
                        "against the same .ndx");
                }
            }
            write_available_coordinate_paths(
                std::cout,
                path_index.get(),
                coordinate_index.get(),
                file_exists(lnx_path.c_str()),
                !program.get<bool>("no_header"));
            return 0;
        }

        if (no_paths && with_coords) {
            throw std::runtime_error("--with_coords requires path output; remove --no_paths");
        }

        if (haplotype_gap_bases.has_value() && !all_haplotypes) {
            throw std::runtime_error(
                "--haplotype_gap requires --all_haplotypes");
        }
        if (haplotype_gap_bases.has_value() &&
            !file_exists(lnx_path.c_str())) {
            throw std::runtime_error(
                "--haplotype_gap requires a node length index: " + lnx_path);
        }

        const auto region_arg = program.get<std::string>("region");
        const auto output_gfa = program.get<std::string>("out_gfa");
        if (region_arg.empty() || output_gfa.empty()) {
            throw std::runtime_error(
                "get_region requires <sequence:start-end> and <out_gfa> "
                "unless --list_coordinates or --print_path_names is used");
        }

        const auto region = parse_region(region_arg);
        if (!file_exists(pdx_path.c_str())) {
            throw std::runtime_error(
                "Path index required for rank-to-node-name conversion does "
                "not exist: " + pdx_path +
                ". get_region currently requires the .pdx node table even "
                "for an rGFA .cdx query; build index_gfa without --no_paths");
        }
        if (with_coords && lnx_explicit && !file_exists(lnx_path.c_str())) {
            throw std::runtime_error("Node length index does not exist: " + lnx_path);
        }
        if (with_coords && pcx_explicit && !file_exists(pcx_path.c_str())) {
            throw std::runtime_error("Path checkpoint index does not exist: " + pcx_path);
        }

        // Extraction is delegated to the same immutable IndexedGraph API used
        // by C++ and Python callers. The CLI remains responsible only for
        // parsing flags, formatting warnings, and choosing the output sink.
        gfaidx::IndexPaths overrides;
        overrides.idx = program.get<std::string>("idx");
        overrides.ndx = program.get<std::string>("ndx");
        overrides.pdx = pdx_path;
        overrides.lnx = file_exists(lnx_path.c_str()) ? lnx_path : std::string{};
        overrides.pcx = file_exists(pcx_path.c_str()) ? pcx_path : std::string{};
        overrides.cdx = cdx_path;
        gfaidx::IndexedGraph graph(input_gz, std::move(overrides));
        if (!graph.capabilities().coordinates && graph.paths().empty()) {
            throw std::runtime_error(
                "No coordinate index was found at " + cdx_path +
                ", and " + pdx_path +
                " contains no P or W records. For an rGFA, build the SR:i:0 "
                "coordinate index with: gfaidx index_coordinates " +
                input_gz + " " + cdx_path);
        }

        gfaidx::RegionOptions options;
        options.max_nodes = parse_max_nodes(program.get<std::string>("max_nodes"));
        options.threads = utils::parse_u32_strict(
            program.get<std::string>("threads"),
            "--threads",
            1,
            chunk::kMaxExtractionThreads);
        options.include_paths = !no_paths;
        options.include_coordinates = with_coords;
        options.mode = all_haplotypes
            ? gfaidx::RegionMode::all_haplotypes
            : gfaidx::RegionMode::bfs;
        options.haplotype_gap = haplotype_gap_bases;

        std::ofstream out(output_gfa);
        if (!out) {
            throw std::runtime_error(
                "Failed to open output GFA file for writing: " + output_gfa);
        }
        gfaidx::QueryCallbacks callbacks;
        callbacks.warning = [](std::string_view message) {
            std::cerr << get_time() << ": Warning: " << message << std::endl;
        };
        graph.stream_region(
            reference,
            region.sequence,
            region.begin,
            region.end,
            [&](std::string_view line) {
                out << line << '\n';
                return static_cast<bool>(out);
            },
            options,
            callbacks);
        if (!out) {
            throw std::runtime_error(
                "Failed while writing output GFA: " + output_gfa);
        }
        return 0;
    } catch (const std::exception& err) {
        std::cerr << err.what() << std::endl;
        return 1;
    }
}

}  // namespace gfaidx::coordinates

#include "coordinates/coordinate_index.h"

#include <algorithm>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <utility>

#include "fs/fs_helpers.h"
#include "indexer/node_hash_index.h"
#include "paths/path_index.h"

namespace gfaidx::coordinates {
namespace {

// The standalone file contains a fixed header, a small track table, one
// fixed-width entry table, and a trailing string blob for track names.
constexpr char kCoordinateIndexMagic[8] = {'G', 'F', 'C', 'O', 'O', 'R', 'D', '1'};
constexpr std::uint32_t kCoordinateIndexVersion = 1;
constexpr std::uint64_t kMissingLength = std::numeric_limits<std::uint64_t>::max();

struct CoordinateIndexHeaderDisk {
    char magic[8]{};
    std::uint32_t version{};
    std::uint32_t reserved{};
    std::uint64_t node_count{};
    std::uint64_t track_count{};
    std::uint64_t entry_count{};
    std::uint64_t track_table_offset{};
    std::uint64_t entry_table_offset{};
    std::uint64_t strings_offset{};
    std::uint64_t strings_size{};
};

struct CoordinateTrackDisk {
    char source_type{};
    char reserved[7]{};
    std::uint64_t reference_offset{};
    std::uint64_t reference_length{};
    std::uint64_t sequence_offset{};
    std::uint64_t sequence_length{};
    std::uint64_t haplotype{};
    std::uint64_t sequence_start{};
    std::uint64_t sequence_end{};
    std::uint64_t entry_begin{};
    std::uint64_t entry_count{};
};

struct CoordinateEntryDisk {
    std::uint64_t start{};
    std::uint32_t node_rank{};
    std::uint32_t reserved{};
};

static_assert(sizeof(CoordinateIndexHeaderDisk) == 72,
              "Unexpected coordinate-index header size");
static_assert(sizeof(CoordinateTrackDisk) == 80,
              "Unexpected coordinate-index track size");
static_assert(sizeof(CoordinateEntryDisk) == 16,
              "Unexpected coordinate-index entry size");

struct BuildEntry {
    std::uint64_t start{};
    std::uint64_t end{};
    std::uint32_t node_rank{};
};

struct BuildTrack {
    char source_type{};
    std::string reference_name;
    std::string sequence_name;
    std::uint64_t haplotype{};
    std::uint64_t sequence_start{};
    std::uint64_t sequence_end{};
    std::vector<BuildEntry> entries;
};

struct ParsedSegment {
    std::string name;
    std::uint64_t length{};
    bool has_rgfa_coordinates{false};
    std::string stable_sequence;
    std::uint64_t stable_offset{};
    std::int64_t stable_rank{-1};
};

struct ParsedWalk {
    std::string sample;
    std::uint64_t haplotype{};
    std::string sequence;
    std::uint64_t sequence_start{};
    std::uint64_t sequence_end{};
    std::string_view walk;
};

std::vector<std::string_view> split_tab_fields(std::string_view line) {
    // A vector is sufficient here because GFA records contain only a small
    // number of tab-separated fields and no keyed lookup is needed.
    std::vector<std::string_view> fields;
    std::size_t start = 0;
    while (start <= line.size()) {
        const auto tab = line.find('\t', start);
        const auto end = tab == std::string_view::npos ? line.size() : tab;
        fields.push_back(line.substr(start, end - start));
        if (tab == std::string_view::npos) break;
        start = tab + 1;
    }
    return fields;
}

std::uint64_t parse_u64(std::string_view value, std::string_view field_name) {
    try {
        if (value.empty() || value.front() == '-') {
            throw std::invalid_argument("value must be non-negative");
        }
        std::size_t consumed = 0;
        const auto parsed = std::stoull(std::string(value), &consumed);
        if (consumed != value.size()) {
            throw std::invalid_argument("trailing characters");
        }
        return parsed;
    } catch (const std::exception& err) {
        throw std::runtime_error("Invalid " + std::string(field_name) +
                                 " value '" + std::string(value) + "': " + err.what());
    }
}

std::int64_t parse_i64(std::string_view value, std::string_view field_name) {
    try {
        std::size_t consumed = 0;
        const auto parsed = std::stoll(std::string(value), &consumed);
        if (consumed != value.size()) {
            throw std::invalid_argument("trailing characters");
        }
        return parsed;
    } catch (const std::exception& err) {
        throw std::runtime_error("Invalid " + std::string(field_name) +
                                 " value '" + std::string(value) + "': " + err.what());
    }
}

std::vector<std::string> parse_reference_samples(std::string_view line) {
    const auto fields = split_tab_fields(line);
    std::vector<std::string> references;
    for (std::size_t i = 1; i < fields.size(); ++i) {
        if (fields[i].substr(0, 5) != "RS:Z:") continue;
        const auto value = fields[i].substr(5);
        for (std::size_t start = 0; start < value.size();) {
            const auto space = value.find(' ', start);
            const auto end = space == std::string_view::npos ? value.size() : space;
            if (end > start) references.emplace_back(value.substr(start, end - start));
            if (space == std::string_view::npos) break;
            start = space + 1;
        }
    }
    return references;
}

ParsedSegment parse_segment(std::string_view line) {
    const auto fields = split_tab_fields(line);
    if (fields.size() < 3 || fields[0] != "S") {
        throw std::runtime_error("Malformed S line while building coordinate index");
    }

    ParsedSegment out;
    out.name = std::string(fields[1]);
    if (fields[2] != "*") {
        out.length = fields[2].size();
    } else {
        out.length = kMissingLength;
    }

    bool has_sn = false;
    bool has_so = false;
    bool has_sr = false;
    for (std::size_t i = 3; i < fields.size(); ++i) {
        const auto field = fields[i];
        if (field.substr(0, 5) == "LN:i:") {
            const auto tagged_length = parse_u64(field.substr(5), "LN");
            if (out.length != kMissingLength && out.length != tagged_length) {
                throw std::runtime_error("Segment '" + out.name +
                                         "' sequence length disagrees with LN:i");
            }
            out.length = tagged_length;
        } else if (field.substr(0, 5) == "SN:Z:") {
            out.stable_sequence = std::string(field.substr(5));
            has_sn = true;
        } else if (field.substr(0, 5) == "SO:i:") {
            out.stable_offset = parse_u64(field.substr(5), "SO");
            has_so = true;
        } else if (field.substr(0, 5) == "SR:i:") {
            out.stable_rank = parse_i64(field.substr(5), "SR");
            has_sr = true;
        }
    }

    out.has_rgfa_coordinates = has_sn && has_so && has_sr;
    return out;
}

ParsedWalk parse_walk(std::string_view line) {
    const auto fields = split_tab_fields(line);
    if (fields.size() < 7 || fields[0] != "W") {
        throw std::runtime_error("Malformed W line while building coordinate index");
    }
    if (fields[4] == "*" || fields[5] == "*") {
        throw std::runtime_error("Reference W line is missing SeqStart or SeqEnd");
    }

    ParsedWalk out;
    out.sample = std::string(fields[1]);
    out.haplotype = parse_u64(fields[2], "W HapIndex");
    out.sequence = std::string(fields[3]);
    out.sequence_start = parse_u64(fields[4], "W SeqStart");
    out.sequence_end = parse_u64(fields[5], "W SeqEnd");
    out.walk = fields[6];
    if (out.sequence_end < out.sequence_start) {
        throw std::runtime_error("Reference W line has SeqEnd before SeqStart");
    }
    return out;
}

bool vector_contains(const std::vector<std::string>& values, std::string_view query) {
    // Reference sample lists are normally tiny, so a vector scan avoids a map
    // or set allocation without affecting indexing complexity materially.
    return std::find(values.begin(), values.end(), query) != values.end();
}

std::vector<std::string> scan_header_reference_samples(const std::string& input_gfa,
                                                       const Reader::Options& reader_options) {
    std::vector<std::string> references;
    Reader reader(reader_options);
    if (!reader.open(input_gfa)) {
        throw std::runtime_error("Could not open GFA for reference header check: " + input_gfa);
    }

    // Only inspect the leading header block. If there is no RS:Z header, callers
    // must continue to the full scan because W lines or rGFA S tags may still be
    // usable coordinate sources.
    std::string_view line;
    while (reader.read_line(line)) {
        if (line.empty()) continue;
        if (line[0] != 'H') break;
        const auto parsed = parse_reference_samples(line);
        references.insert(references.end(), parsed.begin(), parsed.end());
    }

    std::sort(references.begin(), references.end());
    references.erase(std::unique(references.begin(), references.end()), references.end());
    return references;
}

bool path_index_has_reference_walk(const std::string& path_index_path,
                                   std::string_view reference_name) {
    paths::PathIndexReader path_index(path_index_path);

    // The .pdx metadata table is small compared with the graph body, so checking
    // only W record sample ids here avoids a wasted full S-line scan for a
    // misspelled or absent explicit --reference.
    for (std::uint32_t path_id = 0; path_id < path_index.path_count(); ++path_id) {
        const auto info = path_index.get_path_info(path_id);
        if (info.record_type == 'W' && info.sample_id == reference_name) {
            return true;
        }
    }
    return false;
}

void validate_explicit_reference_before_full_scan(const std::string& input_gfa,
                                                  const std::string& path_index_path,
                                                  const std::string& reference_filter,
                                                  const Reader::Options& reader_options) {
    if (reference_filter.empty()) return;

    if (!path_index_path.empty()) {
        // When a .pdx is available it is the cheapest authoritative source for
        // indexed W metadata. Fail before loading .ndx or scanning graph records
        // if the requested reference sample does not exist there.
        if (!path_index_has_reference_walk(path_index_path, reference_filter)) {
            throw std::runtime_error("Reference sample '" + reference_filter +
                                     "' was not found in the path index: " +
                                     path_index_path);
        }
        return;
    }

    const auto header_references = scan_header_reference_samples(input_gfa, reader_options);
    if (!header_references.empty() && !vector_contains(header_references, reference_filter)) {
        // An RS:Z header explicitly lists the reference namespace, so a missing
        // requested sample can be rejected without scanning all S lines.
        throw std::runtime_error("Reference sample '" + reference_filter +
                                 "' is not listed by an H-line RS:Z tag");
    }
}

void append_reference_walks_from_path_index(const std::string& path_index_path,
                                            const std::vector<std::string>& selected_references,
                                            const std::vector<std::uint64_t>& node_lengths,
                                            std::vector<BuildTrack>& tracks) {
    if (path_index_path.empty() || selected_references.empty()) return;

    paths::PathIndexReader path_index(path_index_path);
    if (path_index.node_count() != node_lengths.size()) {
        throw std::runtime_error(".pdx and .ndx node counts differ; rebuild them together");
    }

    // Reuse the rank-aligned .pdx step table when the GFA being indexed no
    // longer carries original W records, as in the chunked multi-member output.
    for (std::uint32_t path_id = 0; path_id < path_index.path_count(); ++path_id) {
        const auto info = path_index.get_path_info(path_id);
        if (info.record_type != 'W') continue;
        if (!vector_contains(selected_references, info.sample_id)) continue;
        if (info.seq_start < 0 || info.seq_end < 0) {
            throw std::runtime_error("Reference W path in .pdx is missing SeqStart/SeqEnd: " +
                                     std::string(info.name));
        }
        if (info.seq_end < info.seq_start) {
            throw std::runtime_error("Reference W path in .pdx has SeqEnd before SeqStart: " +
                                     std::string(info.name));
        }

        BuildTrack track;
        track.source_type = 'W';
        track.reference_name = std::string(info.sample_id);
        track.sequence_name = std::string(info.seq_id);
        track.haplotype = info.hap_index;
        track.sequence_start = static_cast<std::uint64_t>(info.seq_start);
        track.sequence_end = static_cast<std::uint64_t>(info.seq_end);

        const auto steps = path_index.read_steps(path_id);
        track.entries.reserve(steps.size());
        std::uint64_t coordinate = track.sequence_start;
        for (const auto& step : steps) {
            if (step.node_id >= node_lengths.size() ||
                node_lengths[step.node_id] == kMissingLength) {
                throw std::runtime_error("Missing segment length for reference W step in .pdx path: " +
                                         std::string(info.name));
            }
            const auto length = node_lengths[step.node_id];
            if (length > std::numeric_limits<std::uint64_t>::max() - coordinate) {
                throw std::runtime_error("Reference W coordinate overflow in .pdx path: " +
                                         std::string(info.name));
            }
            track.entries.push_back(BuildEntry{coordinate, coordinate + length, step.node_id});
            coordinate += length;
        }
        if (coordinate != track.sequence_end) {
            throw std::runtime_error("Reference W span in .pdx is inconsistent with segment lengths: " +
                                     std::string(info.name));
        }
        tracks.push_back(std::move(track));
    }
}

std::uint64_t append_string(std::string& blob, std::string_view value) {
    const auto offset = static_cast<std::uint64_t>(blob.size());
    blob.append(value.data(), value.size());
    return offset;
}

template <typename T>
void write_vector(std::ofstream& out, const std::vector<T>& values) {
    if (values.empty()) return;
    out.write(reinterpret_cast<const char*>(values.data()),
              static_cast<std::streamsize>(values.size() * sizeof(T)));
}

}  // namespace

bool build_coordinate_index(const std::string& input_gfa,
                            const std::string& output_index,
                            const std::string& node_index_path,
                            const std::string& reference_filter,
                            const Reader::Options& reader_options,
                            const std::string& path_index_path) {
    validate_explicit_reference_before_full_scan(input_gfa,
                                                 path_index_path,
                                                 reference_filter,
                                                 reader_options);

    indexer::NodeHashIndex node_index(node_index_path);
    std::vector<std::uint64_t> node_lengths(static_cast<std::size_t>(node_index.size()),
                                            kMissingLength);
    std::vector<std::string> reference_samples;
    std::vector<BuildTrack> rgfa_tracks;

    // Pass 1 records segment lengths by stable .ndx rank and collects SR:i:0
    // coordinates as a fallback for rGFA files without reference W records.
    Reader first_pass(reader_options);
    if (!first_pass.open(input_gfa)) {
        throw std::runtime_error("Could not open GFA for coordinate indexing: " + input_gfa);
    }
    std::string_view line;
    while (first_pass.read_line(line)) {
        if (line.empty()) continue;
        if (line[0] == 'H') {
            const auto parsed = parse_reference_samples(line);
            reference_samples.insert(reference_samples.end(), parsed.begin(), parsed.end());
            continue;
        }
        if (line[0] != 'S') continue;

        const auto segment = parse_segment(line);
        std::uint32_t rank = 0;
        if (!node_index.lookup_rank(segment.name, rank)) {
            throw std::runtime_error("Segment '" + segment.name +
                                     "' is missing from the supplied .ndx");
        }
        node_lengths[rank] = segment.length;

        if (!segment.has_rgfa_coordinates || segment.stable_rank != 0) continue;
        if (segment.length == kMissingLength) {
            throw std::runtime_error("SR:i:0 segment '" + segment.name +
                                     "' has neither sequence nor LN:i length");
        }
        auto track_it = std::find_if(
            rgfa_tracks.begin(), rgfa_tracks.end(),
            [&](const BuildTrack& track) {
                return track.sequence_name == segment.stable_sequence;
            });
        if (track_it == rgfa_tracks.end()) {
            BuildTrack track;
            track.source_type = 'S';
            track.sequence_name = segment.stable_sequence;
            track.haplotype = 0;
            rgfa_tracks.push_back(std::move(track));
            track_it = std::prev(rgfa_tracks.end());
        }
        if (segment.length > std::numeric_limits<std::uint64_t>::max() -
                                 segment.stable_offset) {
            throw std::runtime_error("rGFA coordinate overflow for segment '" +
                                     segment.name + "'");
        }
        track_it->entries.push_back(BuildEntry{
            segment.stable_offset,
            segment.stable_offset + segment.length,
            rank,
        });
    }

    std::sort(reference_samples.begin(), reference_samples.end());
    reference_samples.erase(std::unique(reference_samples.begin(), reference_samples.end()),
                            reference_samples.end());
    if (!reference_filter.empty() && !reference_samples.empty() &&
        !vector_contains(reference_samples, reference_filter)) {
        throw std::runtime_error("Reference sample '" + reference_filter +
                                 "' is not listed by an H-line RS:Z tag");
    }

    std::vector<std::string> selected_references;
    if (!reference_filter.empty()) {
        selected_references.push_back(reference_filter);
    } else {
        selected_references = reference_samples;
    }

    std::vector<BuildTrack> tracks;
    if (!selected_references.empty()) {
        // Pass 2 parses only W records belonging to selected reference samples;
        // non-reference haplotype walks never enter the coordinate index.
        Reader second_pass(reader_options);
        if (!second_pass.open(input_gfa)) {
            throw std::runtime_error("Could not reopen GFA for W-line indexing: " + input_gfa);
        }
        while (second_pass.read_line(line)) {
            if (line.empty() || line[0] != 'W') continue;
            // Check the sample field before strict W parsing so unrelated
            // haplotype records with '*' coordinates are safely ignored.
            const auto fields = split_tab_fields(line);
            if (fields.size() < 2 || !vector_contains(selected_references, fields[1])) continue;
            const auto walk = parse_walk(line);

            BuildTrack track;
            track.source_type = 'W';
            track.reference_name = walk.sample;
            track.sequence_name = walk.sequence;
            track.haplotype = walk.haplotype;
            track.sequence_start = walk.sequence_start;
            track.sequence_end = walk.sequence_end;

            std::uint64_t coordinate = walk.sequence_start;
            for (std::size_t pos = 0; pos < walk.walk.size();) {
                const char orientation = walk.walk[pos];
                if (orientation != '>' && orientation != '<') {
                    throw std::runtime_error("Malformed orientation in reference W walk");
                }
                const auto next = walk.walk.find_first_of("><", pos + 1);
                const auto end = next == std::string_view::npos ? walk.walk.size() : next;
                if (end <= pos + 1) {
                    throw std::runtime_error("Malformed empty node in reference W walk");
                }
                const auto node_name = walk.walk.substr(pos + 1, end - (pos + 1));
                std::uint32_t rank = 0;
                if (!node_index.lookup_rank(node_name, rank)) {
                    throw std::runtime_error("Reference W walk node '" +
                                             std::string(node_name) +
                                             "' is missing from the supplied .ndx");
                }
                if (rank >= node_lengths.size() || node_lengths[rank] == kMissingLength) {
                    throw std::runtime_error("Missing segment length for reference W node '" +
                                             std::string(node_name) + "'");
                }
                const auto length = node_lengths[rank];
                if (length > std::numeric_limits<std::uint64_t>::max() - coordinate) {
                    throw std::runtime_error("Reference W coordinate overflow");
                }
                track.entries.push_back(BuildEntry{coordinate, coordinate + length, rank});
                coordinate += length;
                pos = end;
            }

            if (coordinate != walk.sequence_end) {
                throw std::runtime_error("Reference W span for " + walk.sample + ":" +
                                         walk.sequence + " is inconsistent with segment lengths");
            }
            tracks.push_back(std::move(track));
        }
    }

    if (tracks.empty()) {
        append_reference_walks_from_path_index(path_index_path,
                                               selected_references,
                                               node_lengths,
                                               tracks);
    }

    if (tracks.empty() && !reference_filter.empty()) {
        throw std::runtime_error("No W records were found in the GFA or .pdx for reference sample '" +
                                 reference_filter + "'");
    }

    if (tracks.empty()) {
        // Convert sorted SR:i:0 entries into continuous fragments. Splitting at
        // gaps lets the compact entry format derive each end from the next start
        // without incorrectly assigning uncovered coordinates to a node.
        for (auto& rgfa_track : rgfa_tracks) {
            auto& entries = rgfa_track.entries;
            std::sort(entries.begin(), entries.end(), [](const auto& lhs, const auto& rhs) {
                if (lhs.start != rhs.start) return lhs.start < rhs.start;
                return lhs.node_rank < rhs.node_rank;
            });
            if (entries.empty()) continue;

            BuildTrack fragment;
            fragment.source_type = 'S';
            fragment.sequence_name = rgfa_track.sequence_name;
            fragment.haplotype = 0;
            fragment.sequence_start = entries.front().start;
            fragment.sequence_end = entries.front().end;
            fragment.entries.push_back(entries.front());

            for (std::size_t i = 1; i < entries.size(); ++i) {
                if (entries[i].start < fragment.sequence_end) {
                    throw std::runtime_error("Overlapping SR:i:0 segments on stable sequence '" +
                                             rgfa_track.sequence_name + "'");
                }
                if (entries[i].start != fragment.sequence_end) {
                    tracks.push_back(std::move(fragment));
                    fragment = BuildTrack{};
                    fragment.source_type = 'S';
                    fragment.sequence_name = rgfa_track.sequence_name;
                    fragment.haplotype = 0;
                    fragment.sequence_start = entries[i].start;
                }
                fragment.entries.push_back(entries[i]);
                fragment.sequence_end = entries[i].end;
            }
            tracks.push_back(std::move(fragment));
        }
    }

    if (tracks.empty()) {
        throw std::runtime_error("No reference W records or SR:i:0 segments were available to index");
    }

    // Sorting metadata makes queries deterministic and keeps fragments for the
    // same reference sequence adjacent without requiring a lookup map on disk.
    std::sort(tracks.begin(), tracks.end(), [](const auto& lhs, const auto& rhs) {
        if (lhs.reference_name != rhs.reference_name) {
            return lhs.reference_name < rhs.reference_name;
        }
        if (lhs.sequence_name != rhs.sequence_name) {
            return lhs.sequence_name < rhs.sequence_name;
        }
        if (lhs.haplotype != rhs.haplotype) return lhs.haplotype < rhs.haplotype;
        return lhs.sequence_start < rhs.sequence_start;
    });

    // The GFA W specification requires fragments for one sample/haplotype/
    // sequence to be non-overlapping; enforce that invariant before binary data
    // is published so one coordinate never resolves through ambiguous fragments.
    for (std::size_t i = 1; i < tracks.size(); ++i) {
        const auto& previous = tracks[i - 1];
        const auto& current = tracks[i];
        if (previous.reference_name == current.reference_name &&
            previous.sequence_name == current.sequence_name &&
            previous.haplotype == current.haplotype &&
            current.sequence_start < previous.sequence_end) {
            throw std::runtime_error("Overlapping reference-coordinate track fragments for '" +
                                     current.reference_name + ":" +
                                     current.sequence_name + "'");
        }
    }

    std::vector<CoordinateTrackDisk> track_records;
    std::vector<CoordinateEntryDisk> entry_records;
    std::string strings;
    track_records.reserve(tracks.size());
    for (const auto& track : tracks) {
        CoordinateTrackDisk record{};
        record.source_type = track.source_type;
        record.reference_offset = append_string(strings, track.reference_name);
        record.reference_length = track.reference_name.size();
        record.sequence_offset = append_string(strings, track.sequence_name);
        record.sequence_length = track.sequence_name.size();
        record.haplotype = track.haplotype;
        record.sequence_start = track.sequence_start;
        record.sequence_end = track.sequence_end;
        record.entry_begin = entry_records.size();
        record.entry_count = track.entries.size();
        track_records.push_back(record);

        for (const auto& entry : track.entries) {
            entry_records.push_back(CoordinateEntryDisk{entry.start, entry.node_rank, 0});
        }
    }

    CoordinateIndexHeaderDisk header{};
    std::memcpy(header.magic, kCoordinateIndexMagic, sizeof(header.magic));
    header.version = kCoordinateIndexVersion;
    header.node_count = node_index.size();
    header.track_count = track_records.size();
    header.entry_count = entry_records.size();
    header.track_table_offset = sizeof(CoordinateIndexHeaderDisk);
    header.entry_table_offset = header.track_table_offset +
                                track_records.size() * sizeof(CoordinateTrackDisk);
    header.strings_offset = header.entry_table_offset +
                            entry_records.size() * sizeof(CoordinateEntryDisk);
    header.strings_size = strings.size();

    // Stage and atomically publish the complete sidecar so interrupted builds
    // cannot leave a truncated coordinate index at the requested path.
    const auto staged_output = make_temp_output_path(output_index);
    try {
        std::ofstream out(staged_output, std::ios::binary | std::ios::trunc);
        if (!out) {
            throw std::runtime_error("Failed to open coordinate index output: " + staged_output);
        }
        out.write(reinterpret_cast<const char*>(&header), sizeof(header));
        write_vector(out, track_records);
        write_vector(out, entry_records);
        if (!strings.empty()) out.write(strings.data(), static_cast<std::streamsize>(strings.size()));
        out.close();
        if (!out) {
            throw std::runtime_error("Failed while writing coordinate index: " + output_index);
        }
        rename_path_or_throw(staged_output, output_index);
    } catch (...) {
        remove_path_if_exists(staged_output);
        throw;
    }
    return true;
}

CoordinateIndexReader::CoordinateIndexReader(const std::string& index_path)
    : index_path_(index_path), in_(index_path, std::ios::binary) {
    if (!in_) throw std::runtime_error("Failed to open coordinate index: " + index_path);

    CoordinateIndexHeaderDisk header{};
    in_.read(reinterpret_cast<char*>(&header), sizeof(header));
    if (!in_) throw std::runtime_error("Failed to read coordinate index header: " + index_path);
    if (std::memcmp(header.magic, kCoordinateIndexMagic, sizeof(header.magic)) != 0) {
        throw std::runtime_error("Invalid coordinate index magic: " + index_path);
    }
    if (header.version != kCoordinateIndexVersion) {
        throw std::runtime_error("Unsupported coordinate index version: " +
                                 std::to_string(header.version));
    }

    in_.seekg(0, std::ios::end);
    const auto file_size = static_cast<std::uint64_t>(in_.tellg());
    const bool track_size_overflow =
        header.track_count > std::numeric_limits<std::uint64_t>::max() /
                                 sizeof(CoordinateTrackDisk);
    const bool entry_size_overflow =
        header.entry_count > std::numeric_limits<std::uint64_t>::max() /
                                 sizeof(CoordinateEntryDisk);
    if (track_size_overflow || entry_size_overflow) {
        throw std::runtime_error("Coordinate index section size overflows uint64");
    }
    const auto track_bytes = header.track_count * sizeof(CoordinateTrackDisk);
    const auto entry_bytes = header.entry_count * sizeof(CoordinateEntryDisk);
    const bool track_offset_overflow =
        track_bytes > std::numeric_limits<std::uint64_t>::max() - header.track_table_offset;
    const bool entry_offset_overflow =
        entry_bytes > std::numeric_limits<std::uint64_t>::max() - header.entry_table_offset;
    if (track_offset_overflow || entry_offset_overflow ||
        header.track_table_offset != sizeof(CoordinateIndexHeaderDisk) ||
        header.entry_table_offset != header.track_table_offset + track_bytes ||
        header.strings_offset != header.entry_table_offset + entry_bytes ||
        header.strings_size > file_size ||
        header.strings_offset > file_size - header.strings_size ||
        header.strings_offset + header.strings_size != file_size) {
        throw std::runtime_error("Coordinate index section offsets are invalid");
    }

    node_count_ = header.node_count;
    entry_count_ = header.entry_count;
    entry_table_offset_ = header.entry_table_offset;

    std::vector<CoordinateTrackDisk> disk_tracks(static_cast<std::size_t>(header.track_count));
    in_.seekg(static_cast<std::streamoff>(header.track_table_offset), std::ios::beg);
    if (!disk_tracks.empty()) {
        in_.read(reinterpret_cast<char*>(disk_tracks.data()),
                 static_cast<std::streamsize>(disk_tracks.size() * sizeof(CoordinateTrackDisk)));
    }
    std::string strings(static_cast<std::size_t>(header.strings_size), '\0');
    in_.seekg(static_cast<std::streamoff>(header.strings_offset), std::ios::beg);
    if (!strings.empty()) in_.read(strings.data(), static_cast<std::streamsize>(strings.size()));
    if (!in_) throw std::runtime_error("Failed to read coordinate index metadata");

    tracks_.reserve(disk_tracks.size());
    for (const auto& record : disk_tracks) {
        if (record.reference_offset > strings.size() ||
            record.reference_length > strings.size() - record.reference_offset ||
            record.sequence_offset > strings.size() ||
            record.sequence_length > strings.size() - record.sequence_offset ||
            record.entry_begin > entry_count_ ||
            record.entry_count > entry_count_ - record.entry_begin ||
            record.sequence_end < record.sequence_start ||
            (record.source_type != 'W' && record.source_type != 'S')) {
            throw std::runtime_error("Coordinate index track metadata is invalid");
        }
        tracks_.push_back(CoordinateTrackInfo{
            record.source_type,
            strings.substr(static_cast<std::size_t>(record.reference_offset),
                           static_cast<std::size_t>(record.reference_length)),
            strings.substr(static_cast<std::size_t>(record.sequence_offset),
                           static_cast<std::size_t>(record.sequence_length)),
            record.haplotype,
            record.sequence_start,
            record.sequence_end,
            record.entry_begin,
            record.entry_count,
        });
    }
}

CoordinateIndexReader::CoordinateEntry CoordinateIndexReader::read_entry(
    std::uint64_t entry_index) const {
    if (entry_index >= entry_count_) {
        throw std::runtime_error("Coordinate entry index is out of range");
    }
    CoordinateEntryDisk disk{};
    in_.clear();
    in_.seekg(static_cast<std::streamoff>(entry_table_offset_ +
                                         entry_index * sizeof(CoordinateEntryDisk)),
              std::ios::beg);
    in_.read(reinterpret_cast<char*>(&disk), sizeof(disk));
    if (!in_) throw std::runtime_error("Failed to read coordinate index entry");
    return CoordinateEntry{disk.start, disk.node_rank};
}

void CoordinateIndexReader::read_entries(std::uint64_t entry_index,
                                          std::uint64_t count,
                                          std::vector<CoordinateEntry>& out) const {
    if (entry_index > entry_count_ || count > entry_count_ - entry_index) {
        throw std::runtime_error("Coordinate entry range is out of bounds");
    }
    std::vector<CoordinateEntryDisk> disk(static_cast<std::size_t>(count));
    in_.clear();
    in_.seekg(static_cast<std::streamoff>(entry_table_offset_ +
                                         entry_index * sizeof(CoordinateEntryDisk)),
              std::ios::beg);
    if (!disk.empty()) {
        in_.read(reinterpret_cast<char*>(disk.data()),
                 static_cast<std::streamsize>(disk.size() * sizeof(CoordinateEntryDisk)));
    }
    if (!in_) throw std::runtime_error("Failed to read coordinate index entry range");
    out.reserve(out.size() + disk.size());
    for (const auto& entry : disk) out.push_back(CoordinateEntry{entry.start, entry.node_rank});
}

std::vector<std::uint32_t> CoordinateIndexReader::query_node_ranks(
    std::string_view reference_name,
    std::string_view sequence_name,
    std::uint64_t begin,
    std::uint64_t end) const {
    if (end <= begin) {
        throw std::runtime_error("Coordinate query end must be greater than start");
    }

    // When the caller omits a reference, accept exactly one reference namespace
    // for the requested sequence and reject ambiguous multi-reference queries.
    std::string inferred_reference;
    if (reference_name.empty()) {
        for (const auto& track : tracks_) {
            if (track.sequence_name != sequence_name) continue;
            if (inferred_reference.empty()) {
                inferred_reference = track.reference_name;
            } else if (track.reference_name != inferred_reference) {
                throw std::runtime_error("Coordinate sequence '" +
                                         std::string(sequence_name) +
                                         "' exists in multiple references; provide --reference");
            }
        }
        reference_name = inferred_reference;
    }

    std::vector<std::uint32_t> ranks;
    bool found_track = false;
    for (const auto& track : tracks_) {
        if (track.reference_name != reference_name || track.sequence_name != sequence_name) {
            continue;
        }
        found_track = true;
        if (track.entry_count == 0 || end <= track.sequence_start || begin >= track.sequence_end) {
            continue;
        }

        // Binary-search the first step whose derived end coordinate is greater
        // than begin. Within one track fragment, the next start is the current
        // step's end; the final step ends at track.sequence_end.
        std::uint64_t low = 0;
        std::uint64_t high = track.entry_count;
        while (low < high) {
            const auto mid = low + (high - low) / 2;
            const auto step_end = mid + 1 < track.entry_count
                ? read_entry(track.entry_begin + mid + 1).start
                : track.sequence_end;
            if (step_end <= begin) low = mid + 1;
            else high = mid;
        }
        if (low >= track.entry_count) continue;

        // Find the exclusive step bound by start coordinate, then read the
        // matching entries in one contiguous disk operation.
        std::uint64_t range_high = low;
        std::uint64_t search_low = low;
        std::uint64_t search_high = track.entry_count;
        while (search_low < search_high) {
            const auto mid = search_low + (search_high - search_low) / 2;
            if (read_entry(track.entry_begin + mid).start < end) search_low = mid + 1;
            else search_high = mid;
        }
        range_high = search_low;

        std::vector<CoordinateEntry> matching;
        read_entries(track.entry_begin + low, range_high - low, matching);
        for (const auto& entry : matching) ranks.push_back(entry.node_rank);
    }

    if (!found_track) {
        throw std::runtime_error("Coordinate track was not found for reference '" +
                                 std::string(reference_name) + "' sequence '" +
                                 std::string(sequence_name) + "'");
    }

    // A node can occur more than once in a walk; sorted vector deduplication is
    // compact and sufficient because coordinate order is not needed after lookup.
    std::sort(ranks.begin(), ranks.end());
    ranks.erase(std::unique(ranks.begin(), ranks.end()), ranks.end());
    return ranks;
}

}  // namespace gfaidx::coordinates

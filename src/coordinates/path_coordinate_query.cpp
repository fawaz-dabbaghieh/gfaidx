#include "coordinates/path_coordinate_query.h"

#include <algorithm>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

#include "indexer/node_length_index.h"
#include "paths/p_path_coordinates.h"

namespace gfaidx::coordinates {
namespace {

struct CandidatePath {
    std::uint32_t path_id{};
    std::uint64_t coordinate_start{};
    std::uint64_t expected_end{};
    bool has_expected_end{false};
};

struct StepCoordinateTable {
    std::vector<std::uint64_t> starts;
    std::vector<std::uint32_t> node_ranks;
    std::uint64_t path_end{};
};

bool has_concrete_walk_coordinates(const paths::PathInfo& info) {
    return info.seq_start >= 0 && info.seq_end >= 0 && info.seq_end >= info.seq_start;
}

std::string path_label(const paths::PathInfo& info) {
    if (info.record_type == 'W') {
        return std::string(info.sample_id) + "|" + std::to_string(info.hap_index) +
               "|" + std::string(info.seq_id) + "|" +
               std::to_string(info.seq_start) + "|" + std::to_string(info.seq_end);
    }
    return std::string(info.name);
}

void add_unique_reference(std::vector<std::string>& references, std::string_view value) {
    const std::string as_string(value);
    if (std::find(references.begin(), references.end(), as_string) == references.end()) {
        references.push_back(as_string);
    }
}

StepCoordinateTable build_step_coordinate_table(
    const paths::PathIndexReader& path_index,
    const indexer::NodeLengthIndexReader& lengths,
    const paths::PathInfo& info,
    std::uint64_t coordinate_base) {

    if (info.step_count > static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max())) {
        throw std::runtime_error("Path has too many steps to coordinate-index on the fly: " +
                                 path_label(info));
    }

    StepCoordinateTable table;
    table.starts.reserve(static_cast<std::size_t>(info.step_count));
    table.node_ranks.reserve(static_cast<std::size_t>(info.step_count));

    std::uint64_t coordinate = coordinate_base;
    path_index.for_each_step(info.path_id, 0, info.step_count,
        [&](const paths::StepRecord& step, std::uint64_t) {
            if (step.node_id >= lengths.node_count()) {
                throw std::runtime_error("Path references a node outside the .lnx length table: " +
                                         path_label(info));
            }
            table.starts.push_back(coordinate);
            table.node_ranks.push_back(step.node_id);

            const auto length = static_cast<std::uint64_t>(lengths.length(step.node_id));
            if (length > std::numeric_limits<std::uint64_t>::max() - coordinate) {
                throw std::runtime_error("Coordinate overflow while scanning path: " +
                                         path_label(info));
            }
            coordinate += length;
        });

    table.path_end = coordinate;
    return table;
}

struct CandidateSpan {
    std::uint64_t start{};
    std::uint64_t end{};
};

CandidateSpan append_overlapping_nodes(
    const paths::PathIndexReader& path_index,
    const indexer::NodeLengthIndexReader& lengths,
    const CandidatePath& candidate,
    std::uint64_t begin,
    std::uint64_t end,
    PathCoordinateQueryResult& result) {
    const auto info = path_index.get_path_info(candidate.path_id);

    // Build one start-coordinate table from the selected fragment base so the
    // expensive path scan is done once, then use binary searches to isolate the
    // requested interval.
    const auto table = build_step_coordinate_table(path_index,
                                                   lengths,
                                                   info,
                                                   candidate.coordinate_start);
    if (candidate.has_expected_end &&
        table.path_end != candidate.expected_end) {
        throw std::runtime_error(
            std::string(1, info.record_type) +
            " path span does not match segment lengths: " + path_label(info));
    }
    if (table.starts.empty() || end <= candidate.coordinate_start ||
        begin >= table.path_end) {
        return CandidateSpan{candidate.coordinate_start, table.path_end};
    }

    auto high_it = std::lower_bound(table.starts.begin(), table.starts.end(), end);
    std::size_t high = static_cast<std::size_t>(high_it - table.starts.begin());
    auto low_it = std::upper_bound(table.starts.begin(), table.starts.end(), begin);
    std::size_t low = static_cast<std::size_t>(low_it - table.starts.begin());
    if (low > 0) --low;

    while (low < high) {
        const auto step_end = (low + 1 < table.starts.size())
            ? table.starts[low + 1]
            : table.path_end;
        if (step_end > begin) break;
        ++low;
    }

    if (low >= high) {
        return CandidateSpan{candidate.coordinate_start, table.path_end};
    }

    // Preserve the exact path occurrence selected by the coordinate binary
    // search. Repeated node ids elsewhere on the same path must not widen it.
    result.reference_path_runs.push_back(paths::SubpathRun{
        candidate.path_id,
        static_cast<std::uint64_t>(low),
        static_cast<std::uint64_t>(high - low),
    });
    for (std::size_t i = low; i < high; ++i) {
        result.node_ranks.push_back(table.node_ranks[i]);
    }
    return CandidateSpan{candidate.coordinate_start, table.path_end};
}

std::vector<CandidatePath> find_candidate_paths(
    const paths::PathIndexReader& path_index,
    std::string_view reference_name,
    std::string_view sequence_name,
    std::uint64_t begin,
    std::uint64_t end) {

    std::vector<CandidatePath> p_candidates;
    std::vector<CandidatePath> w_candidates;
    std::vector<std::string> overlapping_references;
    bool saw_matching_walk_without_coordinates = false;
    for (std::uint32_t id = 0; id < path_index.path_count(); ++id) {
        const auto info = path_index.get_path_info(id);
        if (info.record_type == 'P') {
            const auto parsed =
                paths::parse_p_path_coordinate_name(info.name);
            // The raw name addresses one exact P record, while the suffix-free
            // name addresses every coordinate fragment in that namespace.
            if (info.name != sequence_name &&
                parsed.coordinate_name != sequence_name) {
                continue;
            }
            if (!info.overlap_field.empty() && info.overlap_field != "*") {
                throw std::runtime_error(
                    "Cannot query coordinates on P path '" +
                    std::string(info.name) +
                    "' because its overlap field is not '*'");
            }
            if (parsed.has_coordinates &&
                (end <= parsed.start || begin >= parsed.end)) {
                continue;
            }
            p_candidates.push_back(CandidatePath{
                id,
                parsed.start,
                parsed.end,
                parsed.has_coordinates,
            });
            continue;
        }

        if (info.record_type != 'W' || info.seq_id != sequence_name) continue;
        if (!reference_name.empty() && info.sample_id != reference_name) continue;
        if (!has_concrete_walk_coordinates(info)) {
            saw_matching_walk_without_coordinates = true;
            continue;
        }

        const auto walk_start = static_cast<std::uint64_t>(info.seq_start);
        const auto walk_end = static_cast<std::uint64_t>(info.seq_end);
        if (end <= walk_start || begin >= walk_end) continue;
        w_candidates.push_back(CandidatePath{
            id,
            walk_start,
            walk_end,
            true,
        });
        add_unique_reference(overlapping_references, info.sample_id);
    }

    if (!reference_name.empty()) {
        if (!w_candidates.empty()) return w_candidates;
        if (!p_candidates.empty()) return p_candidates;
    } else {
        // An exact P-path name is already a complete coordinate namespace. If
        // present, prefer it over same-named W sequence fragments.
        if (!p_candidates.empty()) return p_candidates;
        if (overlapping_references.size() > 1) {
            throw std::runtime_error("Indexed W sequence '" + std::string(sequence_name) +
                                     "' overlaps the query in multiple references; provide --reference");
        }
        if (!w_candidates.empty()) return w_candidates;
    }

    if (saw_matching_walk_without_coordinates) {
        throw std::runtime_error("Matching W paths exist in .pdx but do not have concrete SeqStart/SeqEnd coordinates");
    }
    throw std::runtime_error("No indexed P path or W walk in .pdx overlaps the requested interval");
}

}  // namespace

PathCoordinateQueryResult query_path_coordinates_on_the_fly(
    const paths::PathIndexReader& path_index,
    const std::string& length_index_path,
    std::string_view reference_name,
    std::string_view sequence_name,
    std::uint64_t begin,
    std::uint64_t end) {

    if (end <= begin) {
        throw std::runtime_error("Coordinate query end must be greater than start");
    }
    if (length_index_path.empty()) {
        throw std::runtime_error("On-the-fly path coordinate lookup requires a .lnx node length index");
    }

    indexer::NodeLengthIndexReader lengths(length_index_path);
    if (lengths.node_count() != path_index.node_count()) {
        throw std::runtime_error(".lnx and .pdx node counts differ; rebuild them against the same .ndx");
    }

    auto candidates = find_candidate_paths(path_index,
                                           reference_name,
                                           sequence_name,
                                           begin,
                                           end);
    // W fragments need not appear in coordinate order in the original GFA.
    // Sort them so coordinate-selected reference runs remain deterministic.
    std::sort(candidates.begin(), candidates.end(),
              [&](const CandidatePath& lhs, const CandidatePath& rhs) {
                  if (lhs.coordinate_start != rhs.coordinate_start) {
                      return lhs.coordinate_start < rhs.coordinate_start;
                  }
                  return lhs.path_id < rhs.path_id;
              });
    PathCoordinateQueryResult result;
    result.matched_path_count = candidates.size();
    bool have_previous_span = false;
    CandidateSpan previous_span;
    for (const auto& candidate : candidates) {
        const auto span = append_overlapping_nodes(path_index,
                                                   lengths,
                                                   candidate,
                                                   begin,
                                                   end,
                                                   result);
        // Multiple P records can form adjacent coordinate fragments, but two
        // records covering the same base are an ambiguous coordinate namespace.
        if (have_previous_span && span.start < previous_span.end) {
            throw std::runtime_error(
                "Overlapping P/W coordinate fragments for sequence '" +
                std::string(sequence_name) + "'");
        }
        previous_span = span;
        have_previous_span = true;
    }

    std::sort(result.node_ranks.begin(), result.node_ranks.end());
    result.node_ranks.erase(std::unique(result.node_ranks.begin(), result.node_ranks.end()),
                            result.node_ranks.end());
    if (result.node_ranks.empty()) {
        throw std::runtime_error("No indexed P/W path steps overlap the requested interval");
    }
    return result;
}

}  // namespace gfaidx::coordinates

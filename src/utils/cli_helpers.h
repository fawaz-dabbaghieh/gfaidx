#ifndef GFAIDX_CLI_HELPERS_H
#define GFAIDX_CLI_HELPERS_H

#include <cstdint>
#include <limits>
#include <string>
#include <string_view>

namespace gfaidx::utils {

bool has_suffix(std::string_view value, std::string_view suffix);

// gfaidx sidecars are normally named by appending the index suffix to the graph
// path, e.g. graph.gfa.gz.pdx and graph.gfa.gz.cdx.
std::string companion_path(std::string_view input_path, std::string_view suffix);

// Resolve an optional sidecar override. If allow_direct_sidecar is true, an
// input path that already ends in suffix is treated as the sidecar path itself.
std::string resolve_sidecar_path(std::string_view input_path,
                                 std::string_view explicit_path,
                                 std::string_view suffix,
                                 bool allow_direct_sidecar = false);

// Strict CLI parsers: consume the whole string, reject negative unsigned
// values, and raise runtime_error with the flag/field name already included.
std::uint64_t parse_u64_strict(std::string_view value,
                               std::string_view field_name,
                               bool allow_commas = false);

std::int64_t parse_i64_strict(std::string_view value,
                              std::string_view field_name);

// Parse bounded uint32 arguments such as --max_nodes and gzip levels. The
// optional comma handling is only for user-facing genomic coordinate counts.
std::uint32_t parse_u32_strict(std::string_view value,
                               std::string_view field_name,
                               std::uint32_t min_value = 0,
                               std::uint32_t max_value = std::numeric_limits<std::uint32_t>::max(),
                               bool allow_commas = false);

}  // namespace gfaidx::utils

#endif  // GFAIDX_CLI_HELPERS_H

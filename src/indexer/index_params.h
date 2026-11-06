#ifndef GFAIDX_INDEX_PARAMS_H
#define GFAIDX_INDEX_PARAMS_H

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace gfaidx::indexer {

// What index_gfa records in <out>.params: the options it ran with, what the
// graph contains, and what the index looks like, as a small JSON file kept
// beside the other outputs.
struct IndexParams {
    std::string input_gfa;
    std::uint64_t input_bytes{0};
    std::string output_gz;
    // Option name -> value already encoded as JSON (see json_string).
    std::vector<std::pair<std::string, std::string>> parameters;

    std::uint64_t segments{0};
    std::uint64_t links{0};
    std::uint64_t total_sequence_bp{0};
    std::uint64_t paths{0};
    std::uint64_t walks{0};
    // Unknown with --no_paths, which builds no .pdx.
    std::optional<std::uint64_t> path_steps;

    std::uint64_t communities{0};
    std::string node_order;
    double elapsed_seconds{0};
    // Output suffix ("gz", "idx", ...) -> size in bytes.
    std::vector<std::pair<std::string, std::uint64_t>> files;
};

// Quote and escape a string as a JSON value.
std::string json_string(std::string_view value);

// Write params as pretty-printed JSON, with the gfaidx version and a UTC
// creation time.
void write_index_params(const std::string& path, const IndexParams& params);

}  // namespace gfaidx::indexer

#endif  // GFAIDX_INDEX_PARAMS_H

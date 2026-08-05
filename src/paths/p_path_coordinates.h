#ifndef GFAIDX_P_PATH_COORDINATES_H
#define GFAIDX_P_PATH_COORDINATES_H

#include <cstdint>
#include <string>
#include <string_view>

namespace gfaidx::paths {

// Parsed coordinate namespace for a P-line name. A terminal :start-end suffix
// uses the same 0-based, half-open convention as GFA W-line coordinates.
struct PPathCoordinateName {
    std::string_view coordinate_name;
    std::uint64_t start{};
    std::uint64_t end{};
    bool has_coordinates{false};
};

// Recognize only an exact terminal :<uint64>-<uint64> suffix. Other colons and
// dashes remain ordinary path-name characters.
PPathCoordinateName parse_p_path_coordinate_name(std::string_view path_name);

// Replace an existing coordinate suffix, or append one to a path-local name.
std::string format_p_path_coordinate_name(std::string_view path_name,
                                          std::uint64_t start,
                                          std::uint64_t end);

}  // namespace gfaidx::paths

#endif  // GFAIDX_P_PATH_COORDINATES_H

#include "paths/p_path_coordinates.h"

#include <charconv>
#include <stdexcept>
#include <system_error>

namespace gfaidx::paths {
namespace {

bool is_ascii_digits(std::string_view value) {
    if (value.empty()) return false;
    for (const char c : value) {
        if (c < '0' || c > '9') return false;
    }
    return true;
}

std::uint64_t parse_coordinate(std::string_view value,
                               std::string_view path_name,
                               std::string_view field_name) {
    std::uint64_t parsed = 0;
    const auto result = std::from_chars(value.data(),
                                        value.data() + value.size(),
                                        parsed);
    if (result.ec != std::errc{} ||
        result.ptr != value.data() + value.size()) {
        throw std::runtime_error(
            "P path '" + std::string(path_name) + "' has an invalid " +
            std::string(field_name) + " coordinate");
    }
    return parsed;
}

}  // namespace

PPathCoordinateName parse_p_path_coordinate_name(std::string_view path_name) {
    PPathCoordinateName parsed;
    parsed.coordinate_name = path_name;

    const auto colon = path_name.rfind(':');
    if (colon == std::string_view::npos) return parsed;

    const auto dash = path_name.find('-', colon + 1);
    if (dash == std::string_view::npos) return parsed;

    const auto start_text =
        path_name.substr(colon + 1, dash - (colon + 1));
    const auto end_text = path_name.substr(dash + 1);
    if (!is_ascii_digits(start_text) || !is_ascii_digits(end_text)) {
        return parsed;
    }
    if (colon == 0) {
        throw std::runtime_error(
            "Coordinate-bearing P path name has an empty sequence namespace: " +
            std::string(path_name));
    }

    parsed.start = parse_coordinate(start_text, path_name, "start");
    parsed.end = parse_coordinate(end_text, path_name, "end");
    if (parsed.end <= parsed.start) {
        throw std::runtime_error(
            "Coordinate-bearing P path '" + std::string(path_name) +
            "' must have end greater than start");
    }

    parsed.coordinate_name = path_name.substr(0, colon);
    parsed.has_coordinates = true;
    return parsed;
}

std::string format_p_path_coordinate_name(std::string_view path_name,
                                          std::uint64_t start,
                                          std::uint64_t end) {
    const auto parsed = parse_p_path_coordinate_name(path_name);
    return std::string(parsed.coordinate_name) + ":" +
           std::to_string(start) + "-" + std::to_string(end);
}

}  // namespace gfaidx::paths

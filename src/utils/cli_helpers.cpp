#include "utils/cli_helpers.h"

#include <algorithm>
#include <stdexcept>
#include <string>

namespace gfaidx::utils {

bool has_suffix(std::string_view value, std::string_view suffix) {
    return value.size() >= suffix.size() &&
           value.substr(value.size() - suffix.size()) == suffix;
}

std::string companion_path(std::string_view input_path, std::string_view suffix) {
    return std::string(input_path) + std::string(suffix);
}

std::string resolve_sidecar_path(std::string_view input_path,
                                 std::string_view explicit_path,
                                 std::string_view suffix,
                                 bool allow_direct_sidecar) {
    if (!explicit_path.empty()) return std::string(explicit_path);
    if (allow_direct_sidecar && has_suffix(input_path, suffix)) return std::string(input_path);
    return companion_path(input_path, suffix);
}

std::uint64_t parse_u64_strict(std::string_view value,
                               std::string_view field_name,
                               bool allow_commas) {
    std::string normalized(value);
    if (allow_commas) {
        normalized.erase(std::remove(normalized.begin(), normalized.end(), ','),
                         normalized.end());
    }

    try {
        if (normalized.empty() || normalized.front() == '-') {
            throw std::invalid_argument("value must be non-negative");
        }
        std::size_t consumed = 0;
        const auto parsed = std::stoull(normalized, &consumed);
        if (consumed != normalized.size()) {
            throw std::invalid_argument("trailing characters");
        }
        return parsed;
    } catch (const std::exception& err) {
        throw std::runtime_error("Invalid " + std::string(field_name) +
                                 " value '" + std::string(value) + "': " +
                                 err.what());
    }
}

std::int64_t parse_i64_strict(std::string_view value,
                              std::string_view field_name) {
    try {
        std::size_t consumed = 0;
        const auto parsed = std::stoll(std::string(value), &consumed);
        if (consumed != value.size()) {
            throw std::invalid_argument("trailing characters");
        }
        return parsed;
    } catch (const std::exception& err) {
        throw std::runtime_error("Invalid " + std::string(field_name) +
                                 " value '" + std::string(value) + "': " +
                                 err.what());
    }
}

std::uint32_t parse_u32_strict(std::string_view value,
                               std::string_view field_name,
                               std::uint32_t min_value,
                               std::uint32_t max_value,
                               bool allow_commas) {
    const auto parsed = parse_u64_strict(value, field_name, allow_commas);
    if (parsed < min_value || parsed > max_value) {
        throw std::runtime_error(std::string(field_name) +
                                 " must be in the uint32 range [" +
                                 std::to_string(min_value) + ", " +
                                 std::to_string(max_value) + "]");
    }
    return static_cast<std::uint32_t>(parsed);
}

}  // namespace gfaidx::utils

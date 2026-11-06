#include "indexer/index_params.h"

#include <cstdio>
#include <ctime>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <stdexcept>

namespace gfaidx::indexer {

std::string json_string(std::string_view value) {
    std::string out = "\"";
    for (const char c : value) {
        switch (c) {
            case '"': out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\n': out += "\\n"; break;
            case '\t': out += "\\t"; break;
            case '\r': out += "\\r"; break;
            default:
                if (static_cast<unsigned char>(c) < 0x20) {
                    char escaped[8];
                    std::snprintf(escaped, sizeof(escaped), "\\u%04x", static_cast<unsigned char>(c));
                    out += escaped;
                } else {
                    out += c;
                }
        }
    }
    return out + "\"";
}

namespace {

std::string utc_now() {
    const std::time_t now = std::time(nullptr);
    std::tm utc{};
    gmtime_r(&now, &utc);
    char buffer[32];
    std::strftime(buffer, sizeof(buffer), "%Y-%m-%dT%H:%M:%SZ", &utc);
    return buffer;
}

}  // namespace

void write_index_params(const std::string& path, const IndexParams& params) {
    std::ostringstream json;
    json << "{\n"
         << "  \"gfaidx_version\": " << json_string(GFAIDX_VERSION) << ",\n"
         << "  \"created\": " << json_string(utc_now()) << ",\n"
         << "  \"input\": {\n"
         << "    \"path\": " << json_string(params.input_gfa) << ",\n"
         << "    \"bytes\": " << params.input_bytes << "\n"
         << "  },\n"
         << "  \"output\": " << json_string(params.output_gz) << ",\n";

    json << "  \"parameters\": {\n";
    for (std::size_t i = 0; i < params.parameters.size(); ++i) {
        json << "    " << json_string(params.parameters[i].first) << ": "
             << params.parameters[i].second
             << (i + 1 < params.parameters.size() ? ",\n" : "\n");
    }
    json << "  },\n";

    json << "  \"graph\": {\n"
         << "    \"segments\": " << params.segments << ",\n"
         << "    \"links\": " << params.links << ",\n"
         << "    \"total_sequence_bp\": " << params.total_sequence_bp << ",\n"
         << "    \"paths\": " << params.paths << ",\n"
         << "    \"walks\": " << params.walks << ",\n"
         << "    \"path_steps\": ";
    if (params.path_steps) {
        json << *params.path_steps;
    } else {
        json << "null";
    }
    json << "\n  },\n";

    json << "  \"index\": {\n"
         << "    \"communities\": " << params.communities << ",\n"
         << "    \"node_order\": " << json_string(params.node_order) << ",\n"
         << "    \"elapsed_seconds\": " << std::fixed << std::setprecision(1)
         << params.elapsed_seconds << ",\n"
         << "    \"file_bytes\": {\n";
    for (std::size_t i = 0; i < params.files.size(); ++i) {
        json << "      " << json_string(params.files[i].first) << ": " << params.files[i].second
             << (i + 1 < params.files.size() ? ",\n" : "\n");
    }
    json << "    }\n"
         << "  }\n"
         << "}\n";

    std::ofstream out(path, std::ios::trunc);
    if (!out) throw std::runtime_error("Failed to open index parameters file: " + path);
    out << json.str();
    out.close();
    if (!out) throw std::runtime_error("Failed while writing index parameters file: " + path);
}

}  // namespace gfaidx::indexer

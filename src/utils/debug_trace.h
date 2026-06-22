#ifndef GFAIDX_DEBUG_TRACE_H
#define GFAIDX_DEBUG_TRACE_H

#include <cstdlib>
#include <iostream>
#include <string_view>

#include "utils/Timer.h"

namespace gfaidx::debug {

// Use one environment flag so get_subgraph can enable temporary tracing across
// the chunk reader and node-hash index code paths without threading extra state
// through every helper signature.
inline bool subgraph_trace_enabled() {
    static const bool enabled = []() {
        const char* env = std::getenv("GFAIDX_DEBUG_SUBGRAPH");
        return env != nullptr && env[0] != '\0' && !(env[0] == '0' && env[1] == '\0');
    }();
    return enabled;
}

// Keep temporary debug output on stderr with timestamps so long-running
// get_subgraph runs can be correlated against the existing progress logs.
inline void log_subgraph_trace(std::string_view message) {
    if (!subgraph_trace_enabled()) {
        return;
    }
    std::cerr << get_time() << ": [subgraph-trace] " << message << std::endl;
}

}  // namespace gfaidx::debug

#endif  // GFAIDX_DEBUG_TRACE_H

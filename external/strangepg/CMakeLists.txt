cmake_minimum_required(VERSION 3.15)

# Build strangepg as a C library, linkable from C++ targets
add_library(strangepg STATIC
        fs/fs.c
        gfa/gfa.c
)

# Let consumers include headers like: #include "strpg.h" or "fs/fs.h"
target_include_directories(strangepg PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

# If strangepg needs specific C features, set them here
#set_target_properties(strangepg PROPERTIES
#        C_STANDARD 11
#        C_STANDARD_REQUIRED YES
#)

# Optional but common if the code is warning-noisy:
# target_compile_options(strangepg PRIVATE
#     $<$<COMPILE_LANGUAGE:C>:-Wall -Wextra>
# )
cmake_minimum_required(VERSION 3.20)
project(gfaidx LANGUAGES C CXX)

include(GNUInstallDirs)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Default to a release build so CLI users do not get forced debug-only performance.
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

find_package(ZLIB REQUIRED)

# Linker flags from Makefile
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS}")
link_libraries(m)

# Add external includes
include_directories(${CMAKE_SOURCE_DIR}/external/argparse/include)
include_directories(${CMAKE_SOURCE_DIR}/src)
add_subdirectory(external/louvain_method)

add_executable(gfaidx
        src/gfaidx.cpp
        src/chunk/get_chunk_command.cpp
        src/chunk/get_subgraph_command.cpp
        src/chunk/chunk_reader.cpp
        src/chunk/text_handle_cache.cpp
        src/chunk/split_gfa_to_comms.cpp
        src/coordinates/coordinate_commands.cpp
        src/coordinates/coordinate_index.cpp
        src/coordinates/path_haplotype_query.cpp
        src/coordinates/path_coordinate_query.cpp
        src/indexer/community_coarsening.cpp
        src/indexer/community_refinement.cpp
        src/indexer/direct_binary_writer.cpp
        src/indexer/index_gfa_main.cpp
        src/indexer/index_gfa_helpers.cpp
        src/indexer/node_hash_index.cpp
        src/indexer/node_length_index.cpp
        src/paths/get_path_command.cpp
        src/paths/index_path_checkpoints_command.cpp
        src/paths/index_paths_command.cpp
        src/paths/path_coordinate_checkpoints.cpp
        src/paths/path_index.cpp
        src/paths/walk_coords.cpp
        src/utils/Memory.cpp
        src/utils/cli_helpers.cpp
        src/fs/Reader.cpp
        src/fs/fs_helpers.cpp
        src/fs/gfa_line_parsers.cpp
)
# Keep warnings enabled for this target without forcing a specific optimization/debug profile.
target_compile_options(gfaidx PRIVATE -Wall)
target_link_libraries(gfaidx louvain_method ZLIB::ZLIB)

# Install the CLI into the standard binary directory so package managers can use
# `cmake --install` with their own CMAKE_INSTALL_PREFIX.
install(TARGETS gfaidx RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})

include(CTest)
if(BUILD_TESTING)
    # Exercise coordinate lookup, repeated-anchor disambiguation, insertions,
    # and reversed haplotype order through the public CLI.
    add_test(
        NAME all_haplotype_repeated_anchors
        COMMAND bash
                ${CMAKE_SOURCE_DIR}/tests/test_all_haplotype_repeated_anchors.sh
                $<TARGET_FILE:gfaidx>
                ${CMAKE_SOURCE_DIR}/tests/data/repeated_anchor_paths.gfa
                ${CMAKE_SOURCE_DIR}/tests/data/repeated_anchor_coordinate_paths.tsv
    )
endif()

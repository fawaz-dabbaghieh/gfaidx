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
find_package(Threads REQUIRED)

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
        src/paths/p_path_coordinates.cpp
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
target_link_libraries(gfaidx louvain_method ZLIB::ZLIB Threads::Threads)

# Install the CLI into the standard binary directory so package managers can use
# `cmake --install` with their own CMAKE_INSTALL_PREFIX.
install(TARGETS gfaidx RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})

include(CTest)
if(BUILD_TESTING)
    # Exercise exact reference coordinates and conservative haplotype spans
    # containing repeated anchors, insertions, and reversed sequence.
    add_test(
        NAME all_haplotype_repeated_anchors
        COMMAND bash
                ${CMAKE_SOURCE_DIR}/tests/test_all_haplotype_repeated_anchors.sh
                $<TARGET_FILE:gfaidx>
                ${CMAKE_SOURCE_DIR}/tests/data/repeated_anchor_paths.gfa
                ${CMAKE_SOURCE_DIR}/tests/data/repeated_anchor_coordinate_paths.tsv
    )

    # Verify that BFS subgraph extraction can emit coordinate-bearing P and W
    # records through the same .lnx/.pcx path machinery used by get_region.
    add_test(
        NAME get_subgraph_with_coords
        COMMAND bash
                ${CMAKE_SOURCE_DIR}/tests/test_get_subgraph_with_coords.sh
                $<TARGET_FILE:gfaidx>
                ${CMAKE_SOURCE_DIR}/tests/data/subgraph_coordinate_paths.gfa
    )

    # Verify actionable rGFA guidance and coordinate-track listing both with
    # the compatibility flag and with a standalone .cdx but no .pdx.
    add_test(
        NAME rgfa_coordinate_listing
        COMMAND bash
                ${CMAKE_SOURCE_DIR}/tests/test_rgfa_coordinate_listing.sh
                $<TARGET_FILE:gfaidx>
                ${CMAKE_SOURCE_DIR}/tests/data/rgfa_coordinates.gfa
    )

    # Check the standalone GFA 1.1 W-to-GFA 1.0 P conversion used for ODGI
    # interoperability without requiring ODGI itself in the test environment.
    add_test(
        NAME w_to_p
        COMMAND bash
                ${CMAKE_SOURCE_DIR}/tests/test_w_to_p.sh
                ${CMAKE_SOURCE_DIR}/scripts/w_to_p.awk
                ${CMAKE_SOURCE_DIR}/tests/data/w_to_p_input.gfa
                ${CMAKE_SOURCE_DIR}/tests/data/w_to_p_expected.gfa
    )

    # Keep the faster Python converter aligned with the portable AWK output and
    # validate its stdin/stdout and malformed-input behavior.
    add_test(
        NAME w_to_p_python
        COMMAND bash
                ${CMAKE_SOURCE_DIR}/tests/test_w_to_p_python.sh
                ${CMAKE_SOURCE_DIR}/scripts/w_to_p.py
                ${CMAKE_SOURCE_DIR}/tests/data/w_to_p_input.gfa
                ${CMAKE_SOURCE_DIR}/tests/data/w_to_p_expected.gfa
                ${CMAKE_SOURCE_DIR}/tests/data/w_to_p_expected_mapping.tsv
    )

    # Validate automatic benchmark-locus resolution without requiring VG or
    # ODGI binaries in the unit-test environment.
    add_test(
        NAME benchmark_locus_resolution
        COMMAND bash
                ${CMAKE_SOURCE_DIR}/tests/test_benchmark_locus_resolution.sh
                ${CMAKE_SOURCE_DIR}/benchmark/scripts/w_to_p.py
                ${CMAKE_SOURCE_DIR}/benchmark/scripts/resolve_locus.py
                ${CMAKE_SOURCE_DIR}/benchmark/scripts/parse_seed_node.py
                ${CMAKE_SOURCE_DIR}/benchmark/scripts/collect_loci.py
                ${CMAKE_SOURCE_DIR}/tests/data/w_to_p_input.gfa
                ${CMAKE_SOURCE_DIR}/tests/data/benchmark_vg_path_names.txt
                ${CMAKE_SOURCE_DIR}/tests/data/benchmark_odgi_path_names.txt
    )

    # Validate the separate benchmark resolver for unsuffixed PanSN P paths,
    # including GBZ reference metadata and inclusive VG chunk coordinates.
    add_test(
        NAME benchmark_p_locus_resolution
        COMMAND bash
                ${CMAKE_SOURCE_DIR}/tests/test_benchmark_p_locus_resolution.sh
                ${CMAKE_SOURCE_DIR}/benchmark/scripts/resolve_p_locus.py
                ${CMAKE_SOURCE_DIR}/benchmark/scripts/select_p_paths.py
                ${CMAKE_SOURCE_DIR}/tests/data/benchmark_p_path_names.txt
                ${CMAKE_SOURCE_DIR}/tests/data/benchmark_p_gbz_metadata.tsv
    )

    # Verify terminal P-name coordinate intervals in both CDX and PDX/LNX
    # lookup paths, including fragmented tracks and coordinate-bearing output.
    add_test(
        NAME p_path_coordinates
        COMMAND bash
                ${CMAKE_SOURCE_DIR}/tests/test_p_path_coordinates.sh
                $<TARGET_FILE:gfaidx>
                ${CMAKE_SOURCE_DIR}/tests/data/p_coordinate_paths.gfa
                ${CMAKE_SOURCE_DIR}/tests/data/p_coordinate_path_names.tsv
                ${CMAKE_SOURCE_DIR}/tests/data/p_coordinate_bad_span.gfa
                ${CMAKE_SOURCE_DIR}/tests/data/p_coordinate_bad_span_names.tsv
                ${CMAKE_SOURCE_DIR}/tests/data/p_coordinate_overlapping_paths.gfa
                ${CMAKE_SOURCE_DIR}/tests/data/p_coordinate_overlapping_path_names.tsv
    )
endif()

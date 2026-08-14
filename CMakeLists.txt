cmake_minimum_required(VERSION 3.20)
project(gfaidx VERSION 2.0.0 LANGUAGES C CXX)

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

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

add_subdirectory(external/louvain_method)

add_library(gfaidx_library
        src/api/graph.cpp
        src/api/indexed_graph.cpp
        src/chunk/chunk_reader.cpp
        src/chunk/text_handle_cache.cpp
        src/coordinates/coordinate_index.cpp
        src/coordinates/path_haplotype_query.cpp
        src/coordinates/path_coordinate_query.cpp
        src/indexer/node_hash_index.cpp
        src/indexer/node_length_index.cpp
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
add_library(gfaidx::gfaidx ALIAS gfaidx_library)
set_target_properties(gfaidx_library PROPERTIES
    OUTPUT_NAME gfaidx
    VERSION ${PROJECT_VERSION}
    SOVERSION 1
    POSITION_INDEPENDENT_CODE ON
    EXPORT_NAME gfaidx)
target_compile_options(gfaidx_library PRIVATE -Wall)
target_compile_definitions(gfaidx_library PUBLIC GFAIDX_VERSION="${PROJECT_VERSION}")
target_include_directories(gfaidx_library
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
    PRIVATE
        ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(gfaidx_library PUBLIC ZLIB::ZLIB Threads::Threads m)

add_executable(gfaidx_cli
        src/gfaidx.cpp
        src/chunk/get_chunk_command.cpp
        src/chunk/get_subgraph_command.cpp
        src/chunk/split_gfa_to_comms.cpp
        src/coordinates/coordinate_commands.cpp
        src/indexer/community_coarsening.cpp
        src/indexer/community_refinement.cpp
        src/indexer/direct_binary_writer.cpp
        src/indexer/index_gfa_main.cpp
        src/indexer/index_gfa_helpers.cpp
        src/paths/get_path_command.cpp
        src/paths/index_path_checkpoints_command.cpp
        src/paths/index_paths_command.cpp
)
set_target_properties(gfaidx_cli PROPERTIES OUTPUT_NAME gfaidx)
target_compile_options(gfaidx_cli PRIVATE -Wall)
target_compile_definitions(gfaidx_cli PRIVATE GFAIDX_VERSION="${PROJECT_VERSION}")
target_include_directories(gfaidx_cli PRIVATE
    ${CMAKE_SOURCE_DIR}/external/argparse/include
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(gfaidx_cli PRIVATE gfaidx_library louvain_method)

# Python builds are opt-in for ordinary CMake consumers.  The pyproject build
# enables this option and supplies pybind11 as an isolated build dependency.
option(GFAIDX_BUILD_PYTHON "Build the pygfaidx extension module" OFF)
if(GFAIDX_BUILD_PYTHON)
    find_package(Python COMPONENTS Interpreter Development.Module REQUIRED)
    find_package(pybind11 CONFIG REQUIRED)
    pybind11_add_module(_pygfaidx MODULE python/bindings.cpp)
    target_link_libraries(_pygfaidx PRIVATE gfaidx_library)
    target_include_directories(_pygfaidx PRIVATE ${CMAKE_SOURCE_DIR}/include)
    install(TARGETS _pygfaidx
        LIBRARY DESTINATION pygfaidx
        RUNTIME DESTINATION pygfaidx)
endif()

# Install the CLI into the standard binary directory so package managers can use
# `cmake --install` with their own CMAKE_INSTALL_PREFIX.
install(TARGETS gfaidx_library gfaidx_cli
    EXPORT gfaidxTargets
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(DIRECTORY include/gfaidx DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(EXPORT gfaidxTargets
    FILE gfaidxTargets.cmake
    NAMESPACE gfaidx::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/gfaidx)
configure_package_config_file(
    ${CMAKE_SOURCE_DIR}/cmake/gfaidxConfig.cmake.in
    ${CMAKE_CURRENT_BINARY_DIR}/gfaidxConfig.cmake
    INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/gfaidx)
write_basic_package_version_file(
    ${CMAKE_CURRENT_BINARY_DIR}/gfaidxConfigVersion.cmake
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMajorVersion)
install(FILES
    ${CMAKE_CURRENT_BINARY_DIR}/gfaidxConfig.cmake
    ${CMAKE_CURRENT_BINARY_DIR}/gfaidxConfigVersion.cmake
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/gfaidx)

include(CTest)
if(BUILD_TESTING)
    add_executable(gfaidx_graph_api_test tests/test_graph_api.cpp)
    target_link_libraries(gfaidx_graph_api_test PRIVATE gfaidx_library)
    add_test(NAME graph_api COMMAND gfaidx_graph_api_test)

    add_executable(gfaidx_indexed_graph_api_test tests/test_indexed_graph_api.cpp)
    target_link_libraries(gfaidx_indexed_graph_api_test PRIVATE gfaidx_library)
    add_test(
        NAME indexed_graph_api
        COMMAND bash
                ${CMAKE_SOURCE_DIR}/tests/test_library_api.sh
                $<TARGET_FILE:gfaidx_cli>
                $<TARGET_FILE:gfaidx_indexed_graph_api_test>
                ${CMAKE_SOURCE_DIR}/tests/data/subgraph_coordinate_paths.gfa)

    add_test(
        NAME install_consumer
        COMMAND ${CMAKE_COMMAND}
                -DGFAIDX_BUILD_DIR=${CMAKE_BINARY_DIR}
                -DGFAIDX_SOURCE_DIR=${CMAKE_SOURCE_DIR}
                -P ${CMAKE_SOURCE_DIR}/tests/test_install_consumer.cmake)

    # Exercise exact reference coordinates and conservative haplotype spans
    # containing repeated anchors, insertions, and reversed sequence.
    add_test(
        NAME all_haplotype_repeated_anchors
        COMMAND bash
                ${CMAKE_SOURCE_DIR}/tests/test_all_haplotype_repeated_anchors.sh
                $<TARGET_FILE:gfaidx_cli>
                ${CMAKE_SOURCE_DIR}/tests/data/repeated_anchor_paths.gfa
                ${CMAKE_SOURCE_DIR}/tests/data/repeated_anchor_coordinate_paths.tsv
    )

    # Verify that BFS subgraph extraction can emit coordinate-bearing P and W
    # records through the same .lnx/.pcx path machinery used by get_region.
    add_test(
        NAME get_subgraph_with_coords
        COMMAND bash
                ${CMAKE_SOURCE_DIR}/tests/test_get_subgraph_with_coords.sh
                $<TARGET_FILE:gfaidx_cli>
                ${CMAKE_SOURCE_DIR}/tests/data/subgraph_coordinate_paths.gfa
    )

    # Verify actionable rGFA guidance and coordinate-track listing both with
    # the compatibility flag and with a standalone .cdx but no .pdx.
    add_test(
        NAME rgfa_coordinate_listing
        COMMAND bash
                ${CMAKE_SOURCE_DIR}/tests/test_rgfa_coordinate_listing.sh
                $<TARGET_FILE:gfaidx_cli>
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

    # Verify terminal P-name coordinate intervals in both CDX and PDX/LNX
    # lookup paths, including fragmented tracks and coordinate-bearing output.
    add_test(
        NAME p_path_coordinates
        COMMAND bash
                ${CMAKE_SOURCE_DIR}/tests/test_p_path_coordinates.sh
                $<TARGET_FILE:gfaidx_cli>
                ${CMAKE_SOURCE_DIR}/tests/data/p_coordinate_paths.gfa
                ${CMAKE_SOURCE_DIR}/tests/data/p_coordinate_path_names.tsv
                ${CMAKE_SOURCE_DIR}/tests/data/p_coordinate_bad_span.gfa
                ${CMAKE_SOURCE_DIR}/tests/data/p_coordinate_bad_span_names.tsv
                ${CMAKE_SOURCE_DIR}/tests/data/p_coordinate_overlapping_paths.gfa
                ${CMAKE_SOURCE_DIR}/tests/data/p_coordinate_overlapping_path_names.tsv
    )
endif()

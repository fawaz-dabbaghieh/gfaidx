cmake_minimum_required(VERSION 4.0)
project(gfaidx LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 17)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0 -ggdb -Wall")

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
        src/chunk/chunk_reader.cpp
        src/chunk/text_handle_cache.cpp
        src/chunk/split_gfa_to_comms.cpp
        src/indexer/direct_binary_writer.cpp
        src/indexer/index_gfa_main.cpp
        src/indexer/index_gfa_helpers.cpp
        src/indexer/node_hash_index.cpp
        src/utils/Memory.cpp
        src/fs/Reader.cpp
        src/fs/fs_helpers.cpp
        src/fs/gfa_line_parsers.cpp
)
target_link_libraries(gfaidx louvain_method ZLIB::ZLIB)


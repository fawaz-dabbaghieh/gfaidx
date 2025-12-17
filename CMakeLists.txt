cmake_minimum_required(VERSION 4.0)
project(gfaidx LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 17)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -ansi -ggdb -Wall")

# Linker flags from Makefile
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -ansi")
link_libraries(m)

# Add external includes
include_directories(${CMAKE_SOURCE_DIR}/external/argparse/include)
add_subdirectory(external/louvain_method)
#add_subdirectory(external/strangepg)


# Community executable (graph_binary.o community.o main_community.o)
#add_executable(community
#        src/main_community.cpp
#        src/graph_binary.cpp
#        src/community.cpp
#        src/gfa_to_edge_list.cpp)

# Convert executable (graph.o main_convert.o)
#add_executable(convert
#        src/main_convert.cpp
#        src/graph.cpp)

# Hierarchy executable (main_hierarchy.o)
#add_executable(hierarchy
#        src/main_hierarchy.cpp)


add_executable(test_binary_hash
        src/test_binary_hash.cpp)


add_executable(index_gfa
        src/index_gfa.cpp
        src/fs/Reader.cpp
        src/fs/fs_helpers.cpp
#        external/louvain_method/community.cpp
#        external/louvain_method/graph.cpp
#        external/louvain_method/graph_binary.cpp
)

add_executable(test_reader
        src/fs/test_reader.cpp
        src/fs/Reader.cpp
)

target_link_libraries(index_gfa louvain_method)
#target_link_libraries(index_gfa louvain_method strangepg)
#target_link_libraries(index_gfa PRIVATE strangepg)

add_executable(test_file_reading
        scripts/test_file_reading.cpp)
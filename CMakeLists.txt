cmake_minimum_required(VERSION 4.0)
project(louvain_method)

set(CMAKE_CXX_STANDARD 17)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -ansi -ggdb -Wall")

# Linker flags from Makefile
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -ansi")
link_libraries(m)

# Community executable (graph_binary.o community.o main_community.o)
add_executable(community
        src/main_community.cpp
        src/graph_binary.cpp
        src/community.cpp)

# Convert executable (graph.o main_convert.o)
add_executable(convert
        src/main_convert.cpp
        src/graph.cpp)

# Hierarchy executable (main_hierarchy.o)
add_executable(hierarchy
        src/main_hierarchy.cpp)


add_executable(test_binary_hash
        src/test_binary_hash.cpp)

add_executable(gfa_to_edge_list
        scripts/gfa_to_edge_list.cpp
        src/graph.cpp
        src/graph_binary.cpp)

add_executable(test_file_reading
        scripts/test_file_reading.cpp)

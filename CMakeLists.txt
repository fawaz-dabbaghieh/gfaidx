cmake_minimum_required(VERSION 4.0)
project(louvain_method)

set(CMAKE_CXX_STANDARD 14)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -ansi -ggdb -O3 -Wall")

# Linker flags from Makefile
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -ansi")
link_libraries(m)

# Community executable (graph_binary.o community.o main_community.o)
add_executable(community
        src/main_community.cpp
        src/graph_binary.cpp
        src/community.cpp
        src/test_binary_hash.cpp)

# Convert executable (graph.o main_convert.o)
add_executable(convert
        src/main_convert.cpp
        src/graph.cpp)

# Hierarchy executable (main_hierarchy.o)
add_executable(hierarchy
        src/main_hierarchy.cpp)

#
### Keep the original executable if needed
#add_executable(louvain_method
#        main.cpp
#        src/main_community.cpp
#        src/graph_binary.cpp
#        src/community.cpp
#        src/main_convert.cpp
#        src/graph.cpp
#        src/main_hierarchy.cpp)

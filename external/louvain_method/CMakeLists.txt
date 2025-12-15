# Louvain method library
add_library(louvain_method
        community.cpp
        graph.cpp
        graph_binary.cpp)

target_include_directories(louvain_method PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
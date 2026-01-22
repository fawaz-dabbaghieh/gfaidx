cmake_minimum_required(VERSION 4.0)
project(gfaidx LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 17)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -ggdb -Wall")

find_package(ZLIB REQUIRED)

# Linker flags from Makefile
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS}")
link_libraries(m)

# Add external includes
include_directories(${CMAKE_SOURCE_DIR}/external/argparse/include)
include_directories(${CMAKE_SOURCE_DIR}/src)
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


#add_executable(test_binary_hash
#        src/test_binary_hash.cpp)


add_executable(gfaidx
        src/gfaidx.cpp
        src/chunk/get_chunk_command.cpp
        src/chunk/chunk_reader.cpp
        src/chunk/text_handle_cache.cpp
        src/chunk/split_gfa_to_comms.cpp
        src/indexer/index_gfa_main.cpp
        src/indexer/index_gfa_helpers.cpp
        src/indexer/node_hash_index.cpp
        src/utils/Memory.cpp
        src/fs/Reader.cpp
        src/fs/fs_helpers.cpp
        src/fs/gfa_line_parsers.cpp
)
target_link_libraries(gfaidx louvain_method ZLIB::ZLIB)

#add_executable(test_reader
#        src/fs/test_reader.cpp
#        src/fs/Reader.cpp
#)

#add_executable(rle_test
#        src/compressors/rle_test.cpp
#        src/compressors/compressors.cpp
#        src/fs/Reader.cpp
#)

#add_executable(vle_test
#        src/compressors/vle_test.cpp
#        src/fs/Reader.cpp
#        src/fs/gfa_line_parsers.cpp
#)


#add_executable(chunked_gzip
#        src/compressors/chunked_gzip_old.cpp
#        src/fs/Reader.cpp)

#add_executable(chunked_gzip_tmpfiles
#        src/compressors/chunked_gzip_new.cpp
#        src/fs/Reader.cpp)

#add_executable(read_random_community
#               src/read_random_community.cpp
#               )
#target_link_libraries(read_random_community louvain_method ZLIB::ZLIB)



#target_link_libraries(chunked_gzip ZLIB::ZLIB)
#target_link_libraries(chunked_gzip_tmpfiles ZLIB::ZLIB)
#target_link_libraries(index_gfa louvain_method strangepg)
#target_link_libraries(index_gfa PRIVATE strangepg)

#add_executable(test_file_reading
#        scripts/test_file_reading.cpp)

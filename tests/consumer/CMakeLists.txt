cmake_minimum_required(VERSION 3.20)
project(gfaidx_consumer LANGUAGES CXX)

find_package(gfaidx CONFIG REQUIRED)
add_executable(gfaidx_consumer main.cpp)
target_link_libraries(gfaidx_consumer PRIVATE gfaidx::gfaidx)

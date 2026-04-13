cmake_minimum_required(VERSION 4.2)
project(zenoh)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_SCAN_FOR_MODULES ON)

add_library(zenoh SHARED src/zenoh.cpp
        src/zenoh.cppm)

target_sources(zenoh PRIVATE
        FILE_SET CXX_MODULES FILES
        src/zenoh.cppm
)
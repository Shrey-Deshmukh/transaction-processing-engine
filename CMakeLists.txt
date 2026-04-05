cmake_minimum_required(VERSION 3.14)
project(txn_processor CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Optimization flags
set(CMAKE_CXX_FLAGS_RELEASE "-O2")
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

# Find RocksDB
find_package(RocksDB QUIET)

if(RocksDB_FOUND)
    message(STATUS "Found RocksDB via CMake package")
    set(ROCKSDB_LIBS RocksDB::rocksdb)
else()
    # Try pkg-config
    find_package(PkgConfig QUIET)
    if(PkgConfig_FOUND)
        pkg_check_modules(ROCKSDB QUIET rocksdb)
    endif()

    if(ROCKSDB_FOUND)
        message(STATUS "Found RocksDB via pkg-config")
        set(ROCKSDB_LIBS ${ROCKSDB_LIBRARIES})
        include_directories(${ROCKSDB_INCLUDE_DIRS})
    else()
        # Manual search
        find_library(ROCKSDB_LIB NAMES rocksdb
            HINTS /usr/local/lib /usr/lib /opt/homebrew/lib)
        find_path(ROCKSDB_INCLUDE_DIR NAMES rocksdb/db.h
            HINTS /usr/local/include /usr/include /opt/homebrew/include)

        if(ROCKSDB_LIB AND ROCKSDB_INCLUDE_DIR)
            message(STATUS "Found RocksDB: ${ROCKSDB_LIB}")
            set(ROCKSDB_LIBS ${ROCKSDB_LIB})
            include_directories(${ROCKSDB_INCLUDE_DIR})
        else()
            message(FATAL_ERROR
                "RocksDB not found. Install with:\n"
                "  Ubuntu/Debian: apt-get install librocksdb-dev\n"
                "  macOS:         brew install rocksdb\n"
                "  or build from source: https://github.com/facebook/rocksdb\n"
                "Then re-run cmake.")
        endif()
    endif()
endif()

# Main executable
add_executable(txn_processor main.cpp)
target_link_libraries(txn_processor
    PRIVATE
    ${ROCKSDB_LIBS}
    pthread
    dl
    z
    bz2
    lz4
    zstd
    snappy
)

# Suppress warnings from RocksDB headers
target_compile_options(txn_processor PRIVATE -Wall -Wextra -Wpedantic)

message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")
message(STATUS "C++ standard: ${CMAKE_CXX_STANDARD}")

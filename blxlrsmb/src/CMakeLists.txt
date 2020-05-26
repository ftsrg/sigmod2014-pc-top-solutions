cmake_minimum_required(VERSION 3.10)
project(blxlrsmb)

set(SOURCES
    bread.cpp
    cache.cpp
    data.cpp
    globals.cpp
    HybridEstimator.cpp
    main.cpp
    query1.cpp
    query2.cpp
    query3.cpp
    query4.cpp
    read.cpp
    SumEstimator.cpp
    tasty_bread.cpp
    lib/bitset.cpp
    lib/debugutils.cpp
    lib/hash_lib.cpp
    lib/ThreadPool.cpp
    lib/Timer.cpp
    lib/utils.cpp
    lib/memcpy.c)

add_executable(main ${SOURCES})

# Include settings
target_include_directories(main PRIVATE include)
target_include_directories(main SYSTEM PRIVATE third-party)

# Compile settings
target_compile_features(main PRIVATE cxx_std_11)

target_compile_options(
  main PRIVATE $<$<NOT:$<CONFIG:RELEASE>>:-Wno-unused-result
               -Wno-unused-local-typedefs -Wall -Wextra -Wconversion >)

target_compile_definitions(main PRIVATE GOOGLE_HASH)
target_compile_definitions(main PRIVATE $<$<CONFIG:RELEASE>:NUM_THREADS=4>)
target_compile_definitions(main PRIVATE $<$<NOT:$<CONFIG:RELEASE>>:DEBUG>)

# Linking
set(THREADS_PREFER_PTHREAD_FLAG ON)
set(THREADS_PREFER_PTHREAD_FLAG TRUE)
find_package(Threads)

find_package(OpenMP)

target_link_libraries(main Threads::Threads OpenMP::OpenMP_CXX)
target_link_libraries(
  main ${CMAKE_SOURCE_DIR}/third-party/libtcmalloc.a
  ${CMAKE_SOURCE_DIR}/third-party/libunwind.a
  ${CMAKE_SOURCE_DIR}/third-party/liblzma.a)
target_link_options(main PRIVATE -Wl,--wrap=memcpy -static-libstdc++ -no-pie)

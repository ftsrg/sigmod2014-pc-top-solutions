cmake_minimum_required(VERSION 3.10)
project(blxlrsmb)

set(SOURCES
    src/bread.cpp
    src/cache.cpp
    src/data.cpp
    src/globals.cpp
    src/HybridEstimator.cpp
    src/main.cpp
    src/measurement.cpp
    src/query1.cpp
    src/query2.cpp
    src/query3.cpp
    src/query4.cpp
    src/read.cpp
    src/SumEstimator.cpp
    src/tasty_bread.cpp
    src/lib/bitset.cpp
    src/lib/debugutils.cpp
    src/lib/hash_lib.cpp
    src/lib/ThreadPool.cpp
    src/lib/Timer.cpp
    src/lib/utils.cpp
    src/lib/memcpy.c)

add_executable(main ${SOURCES})

# Include settings
target_include_directories(main PRIVATE include)
target_include_directories(main SYSTEM PRIVATE src/third-party)

# Compile settings
target_compile_features(main PRIVATE cxx_std_17)

option(
  PRINT_RESULTS
  "In enabled, then the results of the queries are going to be printed to cout in Release mode. In Debug mode it is always printed."
  OFF
)

target_compile_options(
  main PRIVATE $<$<NOT:$<CONFIG:RELEASE>>:-Wno-unused-result
               -Wno-unused-local-typedefs -Wall -Wextra -Wconversion >)

target_compile_definitions(main PRIVATE GOOGLE_HASH)
target_compile_definitions(main PRIVATE $<$<CONFIG:RELEASE>:MEASURE>)
target_compile_definitions(main PRIVATE $<$<NOT:$<CONFIG:RELEASE>>:DEBUG>)

if (PRINT_RESULTS)
  target_compile_definitions(main PRIVATE PRINT_RESULTS)
endif()

# Linking
set(THREADS_PREFER_PTHREAD_FLAG ON)
set(THREADS_PREFER_PTHREAD_FLAG TRUE)
find_package(Threads)

find_package(OpenMP)

target_link_libraries(main Threads::Threads OpenMP::OpenMP_CXX)
target_link_libraries(
  main ${CMAKE_SOURCE_DIR}/src/third-party/libtcmalloc.a
  ${CMAKE_SOURCE_DIR}/src/third-party/libunwind.a
  ${CMAKE_SOURCE_DIR}/src/third-party/liblzma.a)
target_link_options(main PRIVATE -Wl,--wrap=memcpy -static-libstdc++ -no-pie)

cmake_minimum_required(VERSION 3.10)
project(AWFY)

set(COMMON_SOURCES
    util/chrono.cpp
    util/counters.cpp
    util/io.cpp
    util/measurement.cpp
    util/memoryhooks.cpp
    alloc.cpp
    indexes.cpp
    query1.cpp
    query2.cpp
    query3.cpp
    query4.cpp
    scheduler.cpp
    schedulegraph.cpp
    include/MurmurHash2.cpp
    include/MurmurHash3.cpp)

# ########################## Runner
add_executable(runGraphQueries main.cpp ${COMMON_SOURCES})

# Include settings
target_include_directories(runGraphQueries PRIVATE include)

# Compile settings
target_compile_features(runGraphQueries PRIVATE cxx_std_11)
target_compile_options(
  runGraphQueries
  PRIVATE -march=native
          -msse4.1
          -c
          $<$<CONFIG:RELEASE>:-O3>
          -W
          -Wall
          -Wextra
          -pedantic
          -ffast-math
          -funsafe-math-optimizations
          -fassociative-math
          -ffinite-math-only
          -fno-signed-zeros
          -funroll-all-loops
          -fvariable-expansion-in-unroller)
target_compile_definitions(runGraphQueries PRIVATE -DEXPBACKOFF)
target_compile_definitions(runGraphQueries PRIVATE $<$<CONFIG:RELEASE>:MEASURE>)
target_compile_definitions(runGraphQueries PRIVATE $<$<NOT:$<CONFIG:RELEASE>>:DEBUG DBGPRINT>)

# Linking
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads)
target_link_libraries(runGraphQueries Threads::Threads)
target_link_options(
  runGraphQueries
  PRIVATE
  -Wl,-O1
  -Wl,-wrap,malloc
  -Wl,-wrap,mmap
  -Wl,-wrap,posix_memalign
  -Wl,--whole-archive
  -Wl,--no-whole-archive
  -static
  -static-libgcc)

# ########################## Tester
add_executable(runTester tester.cpp ${COMMON_SOURCES})
# Include settings
target_include_directories(runTester PRIVATE include)

# Compile settings
target_compile_features(runTester PRIVATE cxx_std_11)
target_compile_options(
  runTester
  PRIVATE -march=core2
          -msse4.1
          -c
          -O3
          -W
          -Wall
          -Wextra
          -g
          -pedantic
          -ffast-math
          -funsafe-math-optimizations
          -fassociative-math
          -ffinite-math-only
          -fno-signed-zeros
          -funroll-all-loops
          -fvariable-expansion-in-unroller
          -fno-omit-frame-pointer)
target_compile_definitions(runTester PRIVATE -DDEBUG -DEXPBACKOFF)

# Linking
target_link_libraries(runTester Threads::Threads)
target_link_options(
  runTester
  PRIVATE
  -Wl,-O1
  -Wl,-wrap,malloc
  -Wl,-wrap,mmap
  -Wl,-wrap,posix_memalign
  -pthread)

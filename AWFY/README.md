# SIGMOD 2014 Programming Contest - AWFY Solution

## Build

### Debug

```bash
mkdir cmake-build-debug && cd cmake-build-debug && cmake .. && cmake --build . --parallel
```

### Release

**Use this for benchmarking:**

```bash
mkdir cmake-build-release && cd cmake-build-release && cmake .. -DCMAKE_BUILD_TYPE=Release && cmake --build . --parallel
```

If you want to print out the results then use `cmake .. -DCMAKE_BUILD_TYPE=Release -DPRINT_RESULTS=1` to generate CMake files.

## How to measure
Measure only in **Release mode** and using **CLI parameters**, otherwise the results are not accurate. The output is in `<query>,<loading time in μs>,<query running time in μs>` format.

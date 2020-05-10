# SIGMOD 2014 Programming Contest - AWFY Solution

## Build

### Debug
```
mkdir build
cd build
cmake ..
cmake --build . --parallel
```

### Release
```
mkdir build
cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
cmake --build . --parallel
```
If you want to print out the results then use `cmake .. -DCMAKE_BUILD_TYPE=Release -DPRINT_RESULTS=1` to generate CMake files.

## How to measure
Measure only in **Release mode** and using **CLI parameters**, otherwise the results are not accurate. The output is in `<query>,<loading time>,<query running time>` format.
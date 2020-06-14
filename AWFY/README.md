# SIGMOD 2014 Programming Contest - AWFY Solution

## Build

### Debug

```bash
mkdir cmake-build-debug && cd cmake-build-debug && cmake .. && cmake --build . --parallel
```

### Release

**Use this for benchmarking:**

```bash
rm -rf cmake-build-release && mkdir cmake-build-release && cd cmake-build-release && cmake .. -DCMAKE_BUILD_TYPE=Release -DPRINT_RESULTS=1 && cmake --build . --parallel
```

Remove `-DPRINT_RESULTS=1` if result column is not necessary.

## How to measure
Measure only in **Release mode** and using **CLI parameters or query file with the optional query id parameter**, otherwise the results are not accurate. The output is in `<query e.g. q4>,<loading time in μs>,<query running time in μs>` format in case of CLI parameters, and in`<qX queries from file <path>>,<loading time in μs>,<query running time in μs>` format in case of query file with the optional query id parameter.

E.g:
 * `./runGraphQueries /data/p10k/ PARAM 4 3 George_W._Bush`
 * ` ./runGraphQueries /data/p10k/ FILE /data/p10k/q4.txt 4`

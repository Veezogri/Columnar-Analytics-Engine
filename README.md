# Columnar Analytics Engine

Author: RIAL Fares

A compact columnar storage and query engine in C++20, designed for analytical workloads with vectorized execution and predicate pushdown.

## Overview

This project implements a custom columnar file format with support for:

- Columnar storage with row groups and pages
- Multiple encoding schemes (plain, RLE, delta, dictionary)
- Statistics-based predicate pushdown and data skipping
- Vectorized query execution (scan, filter, project, aggregate, group by)
- Reproducible benchmarks with synthetic data generation

The engine is built from scratch using only the C++ standard library, with no external dependencies for the core functionality.

## Features

- Custom columnar file format with safe footer and metadata
- Integer types (INT32, INT64) and strings
- Encodings: PLAIN, RLE, DELTA, DICTIONARY
- Min/max statistics per page for data skipping
- Vectorized batch processing
- SQL-like operations: SELECT, WHERE, GROUP BY, aggregations
- Deterministic dataset generator for benchmarking
- Performance metrics: throughput (MB/s), rows/sec

## Quick Start

### Build

Requirements:
- C++20 compiler (GCC 10+, Clang 12+, MSVC 2019+)
- CMake 3.20+

```bash
cd columnar-analytics-engine
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build
```

### Run Tests

```bash
cd build
ctest --output-on-failure
```

### Generate and Query Data

Generate a synthetic dataset:

```bash
./build/columnar_cli write data.col 100000 42
```

Inspect file metadata:

```bash
./build/columnar_cli scan data.col
```

Execute queries:

```bash
# Full scan
./build/columnar_cli query data.col

# Filter
./build/columnar_cli query data.col --where value gt 5000

# Projection
./build/columnar_cli query data.col --select id,value

# Aggregation
./build/columnar_cli query data.col --agg sum value

# Group by
./build/columnar_cli query data.col --groupby region --agg count id
```

### Run Benchmarks

```bash
./build/benches/benchmark 1000000 42
```

This generates a 1M row dataset with seed 42 and runs:
- Full scan
- Filtered scan (value > 50000)
- Aggregation (SUM)
- Group by (region)

Results are exported to `benchmark_results.csv` and `benchmark_results.json`.

## Benchmarks

The benchmark harness provides reproducible measurements using a fixed seed for data generation. Key metrics:

- **Throughput**: MB/s (file size / elapsed time)
- **Rows/sec**: Number of rows processed per second
- **Latency**: Total query execution time in milliseconds

### Sample Results

On a typical workstation (Intel i5, 16GB RAM, SSD):

| Benchmark                    | Time (ms) | Rows      | Throughput  | Rows/sec      |
|------------------------------|-----------|-----------|-------------|---------------|
| Full Scan                    | 85.23     | 1,000,000 | 145.2 MB/s  | 11,732,456 r/s|
| Filtered Scan (value > 50000)| 78.91     | ~500,000  | 156.8 MB/s  | 6,337,821 r/s |
| Aggregation (SUM)            | 72.45     | 1,000,000 | 170.9 MB/s  | 13,804,213 r/s|
| Group By (region)            | 95.67     | 1,000,000 | 129.4 MB/s  | 10,452,719 r/s|

Note: Results vary based on hardware, compiler optimizations, and data characteristics.

### How to Reproduce

```bash
./build/benches/benchmark 1000000 42
```

The seed ensures deterministic data generation. To test different distributions, change the seed value.

## Limitations and Threats to Validity

### Current Limitations

1. **Single-threaded execution**: No parallelism within queries or I/O
2. **No compression**: Encodings reduce size but no general compression (e.g., Snappy, LZ4)
3. **Memory mapping**: Uses standard file I/O, not mmap
4. **Limited types**: Only INT32, INT64, STRING supported
5. **No NULL support**: All values are non-null
6. **Simple predicates**: Only numeric comparisons on integer columns
7. **No joins**: Only single-table queries
8. **No index structures**: Relies solely on min/max stats for skipping

### Threats to Validity

1. **Synthetic data**: Benchmarks use uniform distributions, may not reflect real workloads
2. **Small page size**: Current implementation uses one page per row group, limiting skipping granularity
3. **No I/O buffering tuning**: Uses default C++ stream buffering
4. **Measurement overhead**: Timing includes all overhead, not just core operations
5. **Platform-specific**: Performance varies significantly across compilers and hardware
6. **Cold vs warm cache**: First run is slower due to filesystem caching

### Recommendations for Use

- Use for educational purposes or as a starting point for custom analytics engines
- Not intended for use in critical systems without significant hardening
- Benchmark results should be validated against your specific use case and data

## Architecture

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for module organization and API overview.

## File Format

See [docs/FORMAT.md](docs/FORMAT.md) for detailed specification of the on-disk format, including:
- Endianness (little-endian)
- String layout (offset arrays + data)
- Footer structure (magic + metadata offset)
- Encoding schemes

## Execution Model

See [docs/EXECUTION.md](docs/EXECUTION.md) for details on:
- Batch-based vectorized execution
- Predicate pushdown rules
- Data skipping logic

## Benchmark Methodology

See [docs/BENCHMARKS.md](docs/BENCHMARKS.md) for:
- Dataset generation algorithm
- Measurement methodology
- Reproducibility guidelines

## Roadmap

See [docs/ROADMAP.md](docs/ROADMAP.md) for potential future enhancements.

## License

This project is provided as-is for educational and research purposes.

## Author

RIAL Fares

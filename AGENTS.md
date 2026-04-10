# AGENTS.md - YCSB-C Developer Guide

YCSB-C is a C++ implementation of the Yahoo! Cloud Serving Benchmark with DPDK support for high-performance KV store testing.

## Build Commands

```bash
# Build
mkdir -p build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j$(nproc)

# Clean build
rm -rf build && mkdir build && cd build && cmake .. && make -j$(nproc)

# Clean
make clean

# Debug build
cmake .. -DCMAKE_BUILD_TYPE=Debug
```

## Run Commands

```bash
# Run workload (requires sudo for DPDK)
sudo ./run_ycsbc.sh -T 2 -P workloads/workloada.spec

# Direct execution
sudo ./build/ycsbc -T 4 -P workloads/workloadb.spec

# Options:
#   -T <n>     : Number of threads
#   -P <file>  : Workload spec file
#   -db <name> : Database name (basic, cache_migration_dpdk)
#   -l <bool>  : Load phase
```

## Testing

No formal unit test framework (no gtest/catch2). Testing is done by running benchmark workloads:

```bash
# Verify with basic workload
./run_ycsbc.sh -T 1 -P workloads/workloada.spec
tail ycsbc.output

# Debugging (see run_ycsbc.sh for examples)
sudo gdb --args ./build/ycsbc -T 1 -P workloads/workloada.spec
sudo valgrind --leak-check=full ./build/ycsbc -T 1 -P workloads/workloada.spec
```

## Code Style

### Language
- **C++17** standard
- GCC 13+ with `-Wall -Wextra -Werror`

### Naming
| Type             | Convention          | Example                             |
| ---------------- | ------------------- | ----------------------------------- |
| Classes/Structs  | PascalCase          | `CacheMigrationDpdk`, `RequestInfo` |
| Namespaces       | lowercase           | `ycsbc`, `utils`                    |
| Member variables | trailing underscore | `name_`, `db_`                      |
| Local variables  | snake_case          | `key`, `result`                     |
| Constants        | k + PascalCase      | `kOK`, `kErrorNoData`               |
| Macros           | UPPER_CASE          | `KEY_LENGTH`, `TX_NUM_MBUFS`        |
| Functions        | PascalCase          | `DoInsert()`, `GetName()`           |

### Formatting
- **Indentation**: 2 spaces (no tabs)
- **Line length**: ~100 characters
- **Braces**: Same-line opening brace

### Include Order
```cpp
#include "corresponding_header.h"   // 1. Corresponding header first
#include <unistd.h>                  // 2. C system headers
#include <vector>                    // 3. C++ STL
#include <rte_mbuf.h>                // 4. Third-party libraries
#include "core/db.h"                 // 5. Project headers
```

### Include Guards
```cpp
#ifndef YCSB_C_FILENAME_H_
#define YCSB_C_FILENAME_H_
// ... content ...
#endif  // YCSB_C_FILENAME_H_
```
Or: `#pragma once`

### Error Handling
```cpp
// Return codes for DB operations
static const int kOK = 0;
static const int kErrorNoData = 1;

// Exceptions for fatal errors
throw utils::Exception("Unknown operation");

// DPDK logging
RTE_LOG(ERR, PACKET, "Error message\n");
```

### Constants & Memory
```cpp
// Prefer constexpr over #define
constexpr size_t KEY_LENGTH = 16;

// Use smart pointers and containers
std::unique_ptr<Resource> ptr;
std::vector<uint8_t> buffer;

// Atomic operations
std::atomic<uint64_t> counter;
counter.fetch_add(1, std::memory_order_relaxed);
```

### File Header
```cpp
//
//  filename.h
//  YCSB-C
//
//  Created by Author on MM/DD/YY.
//  Copyright (c) YYYY Author <email>.
//
```

## Project Structure

```
YCSB-C/
├── core/       # Workload generators, client framework
├── db/         # Database implementations (basic_db, cache_migration_dpdk)
├── lib/        # Shared libraries (protocol, hash, request_map)
├── workloads/  # Workload specification files (*.spec)
├── conf/       # Configuration files
├── build/      # Build output directory
├── CMakeLists.txt
└── ycsbc.cc    # Main entry point
```

## Dependencies

- **Compiler**: GCC 13+ (or `$HOME/gcc`)
- **Boost**: `$HOME/lib/boost` or system package
- **TBB**: `$HOME/lib/tbb` or libtbb-dev
- **DPDK**: `$HOME/lib/dpdk` (required for cache_migration_dpdk)
- **CMake**: 4.0+

## Common Patterns

**RAII for cleanup**:
```cpp
struct DBCleaner {
  ycsbc::DB *db;
  ~DBCleaner() { db->Close(); }
} cleaner{db};
```

**DPDK network types**:
```cpp
rte_be32_t ip_addr;
uint16_t port = rte_cpu_to_be_16(UDP_PORT_KV);
```

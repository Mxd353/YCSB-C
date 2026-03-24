# AGENTS.md - YCSB-C Developer Guide

This file provides guidelines for AI coding agents working on the YCSB-C project - a C++ implementation of the Yahoo! Cloud Serving Benchmark with DPDK support.

## Build System

### Build Commands
```bash
# Create build directory and configure
cd /home/cc/Desktop/CacheMigration/YCSB-C
mkdir -p build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release

# Build the project
make -j$(nproc)
# Or use ninja if configured:
# ninja

# Clean build
make clean
```

### Run Commands
```bash
# Run with workload (requires sudo for DPDK)
cd /home/cc/Desktop/CacheMigration/YCSB-C
sudo ./run_ycsbc.sh -T 2 -P workloads/workloada.spec

# Direct execution
sudo ./build/ycsbc -T 4 -P workloads/workloadb.spec

# Available options:
# -T <threadcount>  : Number of threads
# -P <workloadfile>: Path to workload spec file
# -db <dbname>      : Database name
# -l                : Load phase (bool)
```

### Dependencies
- **Compiler**: GCC 13+ (or $HOME/gcc)
- **Boost**: $HOME/lib/boost or system package
- **TBB**: $HOME/lib/tbb or libtbb-dev
- **DPDK**: $HOME/lib/dpdk (required for cache_migration_dpdk)
- **CMake**: 4.0+

## Code Style Guidelines

### Language Standard
- **C++17** (set in CMakeLists.txt)
- No compiler extensions (`CMAKE_CXX_EXTENSIONS OFF`)

### Naming Conventions

**Types & Classes**:
- Classes: `CamelCase` (e.g., `CacheMigrationDpdk`, `CoreWorkload`)
- Structs: `CamelCase` (e.g., `RequestInfo`, `KVRequest`)
- Namespaces: lowercase (e.g., `ycsbc`, `utils`, `c_m_proto`)

**Variables**:
- Member variables: trailing underscore (e.g., `name_`, `db_`, `workload_`)
- Local variables: `snake_case` (e.g., `key`, `table`, `result`)
- Constants: `k` prefix + PascalCase (e.g., `kOK`, `kErrorNoData`, `kFNVPrime64`)
- Macros/defines: `ALL_CAPS` with underscores (e.g., `KEY_LENGTH`, `UDP_PORT_KV`)
- Template parameters: `CamelCase` with descriptive names

**Functions**:
- Methods: `CamelCase` (e.g., `DoInsert()`, `BuildRequestPacket()`)
- Getters/Setters: `CamelCase` (e.g., `GetName()`, `SetName()`)
- Inline utilities: `snake_case` or `CamelCase` (be consistent)

### Include Guards
Use header guards with project prefix:
```cpp
#ifndef YCSB_C_FILENAME_H_
#define YCSB_C_FILENAME_H_
// ... content ...
#endif  // YCSB_C_FILENAME_H_
```

Or for newer headers:
```cpp
#pragma once
```

### Include Order
1. Corresponding header file (for .cc files)
2. C system headers (e.g., `<unistd.h>`)
3. C++ standard library headers (e.g., `<vector>`, `<string>`)
4. Third-party libraries (e.g., `<rte_ethdev.h>`, `<boost/...>`)
5. Project headers using `"..."` (e.g., `"core/db.h"`)

Example:
```cpp
#include "cache_migration_dpdk.h"  // Corresponding header first

#include <algorithm>                // C++ STL
#include <vector>

#include <rte_mbuf.h>               // Third-party

#include "core/db.h"                // Project headers
#include "lib/c_m_proto.h"
```

### Code Formatting
- **Indentation**: 2 spaces (no tabs)
- **Line length**: ~80-100 characters
- **Braces**: Same-line opening brace for functions and classes
- **Comments**: Use `//` for single-line, `/* */` for file headers

### Error Handling

**Use appropriate error mechanism for context**:

1. **Return codes** for DB operations:
```cpp
static const int kOK = 0;
static const int kErrorNoData = 1;
static const int kErrorConflict = 2;
```

2. **Exceptions** for fatal/config errors:
```cpp
throw utils::Exception("Operation request is not recognized!");
throw std::invalid_argument("total_ops must be greater than 0");
```

3. **DPDK logging** for network operations:
```cpp
RTE_LOG(ERR, PACKET, "No source IP available\n");
RTE_LOG(WARNING, CORE, "Warning message\n");
```

4. **Standard error** for general diagnostics:
```cpp
std::cerr << "Error: Failed to initialize\n";
```

### Memory Management
- Use `std::unique_ptr` for owned resources
- Use `std::vector` for dynamic arrays
- Use `std::atomic` for shared counters/flags
- DPDK resources: Use `rte_pktmbuf_alloc()` / `rte_pktmbuf_free()`

### Thread Safety
- Use `thread_local` for per-thread state
- Use `std::memory_order_relaxed` for counters where appropriate
- Atomic operations for shared state:
```cpp
ops_cnt[op].fetch_add(1, std::memory_order_relaxed);
```

### Constants
Prefer `constexpr` over `#define` where possible:
```cpp
constexpr size_t KEY_LENGTH = 16;
constexpr uint16_t RETRIES = 5;
```

Use `const` for values that don't need compile-time evaluation:
```cpp
const uint16_t TOTAL_LEN = ...;
```

### DPDK Specific Guidelines
- Use `rte_be32_t` for network byte order IP addresses
- Use `rte_cpu_to_be_16()` / `rte_be_to_cpu_16()` for conversions
- Check pointer returns from `rte_pktmbuf_alloc()`
- Use `RTE_ETHER_ADDR_COPY` macros for address operations

### File Headers
Include standard file header:
```cpp
//
//  filename.h
//  YCSB-C
//
//  Created by Author Name on MM/DD/YY.
//  Copyright (c) YYYY Author Name <email>.
//
```

## Project Structure

```
YCSB-C/
├── core/           # Core framework (workload generators, client)
├── db/             # Database implementations (basic_db, cache_migration_dpdk)
├── lib/            # Shared libraries (protocol, consistent_hash, request_map)
├── workloads/      # Workload specification files (*.spec)
├── conf/           # Configuration files
├── build/          # Build output directory
├── CMakeLists.txt  # CMake configuration
└── ycsbc.cc        # Main entry point
```

## Testing

**Note**: This project does not use a formal test framework. Testing is done via:
1. Running workloads against the benchmark
2. Manual verification of output
3. DPDK packet capture and analysis

To verify changes:
```bash
# Build and run basic workload
./run_ycsbc.sh -T 1 -P workloads/workloada.spec

# Check output
tail ycsbc.output
```

## Common Patterns

**RAII for DPDK cleanup**:
```cpp
struct DBCleaner {
  ycsbc::DB *db;
  ~DBCleaner() { db->Close(); }
} cleaner{db};
```

**Atomic counters**:
```cpp
std::atomic<uint64_t> ops_cnt[ycsbc::Operation::READMODIFYWRITE + 1];
ops_cnt[op].fetch_add(1, std::memory_order_relaxed);
```

**Protocol encoding**:
```cpp
kv_request->dev_info = static_cast<uint8_t>(ENCODE_DEV_INFO(dev_id, DEV_CLIENT));
```

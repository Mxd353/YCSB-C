cmake_minimum_required(VERSION 4.0)

# =============================================================================
# 项目配置
# =============================================================================
project(ycsbc
    VERSION 1.0
    DESCRIPTION "YCSB-C: C++ implementation of YCSB with DPDK support"
    LANGUAGES CXX
)

enable_language(C)

cmake_policy(SET CMP0144 NEW)  # find_package uses upper-case <PACKAGENAME>_ROOT variables

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# =============================================================================
# 用户可配置选项
# =============================================================================
option(BUILD_SHARED_LIBS "Build shared libraries" OFF)
option(ENABLE_OPTIMIZATION "Enable O3 optimization" OFF)
option(ENABLE_DEBUG "Enable debug symbols" ON)
option(ENABLE_WARNINGS_AS_ERRORS "Treat warnings as errors" ON)
option(USE_CUSTOM_GCC "Use custom GCC from HOME_DIR/gcc" OFF)

# 依赖库路径配置 (可通过 -D 参数覆盖)
set(TBB_ROOT "${CMAKE_PREFIX_PATH}/tbb" CACHE PATH "TBB installation prefix")
set(DPDK_ROOT "$ENV{HOME}/lib/dpdk" CACHE PATH "DPDK installation prefix")
set(GCC_CUSTOM_ROOT "$ENV{HOME}/gcc" CACHE PATH "Custom GCC installation prefix")

# =============================================================================
# 编译器配置
# =============================================================================
if(USE_CUSTOM_GCC)
    if(EXISTS "${GCC_CUSTOM_ROOT}/bin/gcc" AND EXISTS "${GCC_CUSTOM_ROOT}/bin/g++")
        set(CMAKE_C_COMPILER "${GCC_CUSTOM_ROOT}/bin/gcc")
        set(CMAKE_CXX_COMPILER "${GCC_CUSTOM_ROOT}/bin/g++")
        message(STATUS "Using custom GCC from: ${GCC_CUSTOM_ROOT}")
    else()
        message(FATAL_ERROR "Custom GCC not found at ${GCC_CUSTOM_ROOT}")
    endif()
endif()

message(STATUS "C Compiler: ${CMAKE_C_COMPILER}")
message(STATUS "CXX Compiler: ${CMAKE_CXX_COMPILER}")
message(STATUS "GCC C Compiler version: ${CMAKE_C_COMPILER_VERSION}")
message(STATUS "GCC C++ Compiler version: ${CMAKE_CXX_COMPILER_VERSION}")

# =============================================================================
# 编译选项配置
# =============================================================================
add_definitions(-DRTE_USE_C11_MEM_MODEL)

set(COMMON_COMPILE_OPTIONS
    -Wall
    -Wextra
)

if(ENABLE_WARNINGS_AS_ERRORS)
    list(APPEND COMMON_COMPILE_OPTIONS -Werror)
endif()

if(ENABLE_OPTIMIZATION)
    list(APPEND COMMON_COMPILE_OPTIONS
        -O3
        -march=native
        -funroll-loops
        -fomit-frame-pointer
    )
    message(STATUS "Optimization: O3 enabled")
else()
    list(APPEND COMMON_COMPILE_OPTIONS -O2)
    message(STATUS "Optimization: O2 (default)")
endif()

if(ENABLE_DEBUG)
    list(APPEND COMMON_COMPILE_OPTIONS -g)
    message(STATUS "Debug symbols: enabled")
endif()

# =============================================================================
# 查找依赖库
# =============================================================================

# Threads
find_package(Threads REQUIRED)
message(STATUS "Threads: ${CMAKE_THREAD_LIBS_INIT}")

# TBB
if(EXISTS "${TBB_ROOT}")
    list(APPEND CMAKE_PREFIX_PATH ${TBB_ROOT})
    message(STATUS "Using custom TBB from: ${TBB_ROOT}")
endif()
find_package(TBB REQUIRED)
message(STATUS "TBB version: ${TBB_VERSION}")

# DPDK (通过 pkg-config)
find_package(PkgConfig REQUIRED)
if(EXISTS "${DPDK_ROOT}/lib/x86_64-linux-gnu/pkgconfig")
    set(ENV{PKG_CONFIG_PATH} "${DPDK_ROOT}/lib/x86_64-linux-gnu/pkgconfig:$ENV{PKG_CONFIG_PATH}")
    message(STATUS "Using custom DPDK from: ${DPDK_ROOT}")
endif()
pkg_check_modules(DPDK REQUIRED libdpdk>=21.0)
message(STATUS "DPDK version: ${DPDK_VERSION}")
message(STATUS "DPDK include: ${DPDK_INCLUDE_DIRS}")

# =============================================================================
# 源文件配置
# =============================================================================
file(GLOB_RECURSE SOURCE_FILES
    "${CMAKE_SOURCE_DIR}/core/*.cc"
    "${CMAKE_SOURCE_DIR}/db/*.cc"
    "${CMAKE_SOURCE_DIR}/lib/*.cc"
    "${CMAKE_SOURCE_DIR}/ycsbc.cc"
)
message(STATUS "Source files: ${SOURCE_FILES}")

set(TARGET_NAME ycsbc)

add_executable(${TARGET_NAME} ${SOURCE_FILES})

# =============================================================================
# 编译定义
# =============================================================================
target_compile_definitions(${TARGET_NAME}
    PRIVATE
        $<$<CONFIG:Debug>:RTE_LIBRTE_ETHDEV_DEBUG>
)

# =============================================================================
# 头文件目录
# =============================================================================
target_include_directories(${TARGET_NAME}
    PUBLIC
        ${CMAKE_SOURCE_DIR}
        ${CMAKE_SOURCE_DIR}/core
        ${CMAKE_SOURCE_DIR}/db
        ${CMAKE_SOURCE_DIR}/lib
        ${DPDK_INCLUDE_DIRS}
)

# =============================================================================
# 链接库
# =============================================================================
target_link_directories(${TARGET_NAME}
    PUBLIC
        ${DPDK_LIBRARY_DIRS}
)

target_link_libraries(${TARGET_NAME}
    PUBLIC
        TBB::tbb
        Threads::Threads
        ${DPDK_LIBRARIES}
)

# =============================================================================
# 编译选项
# =============================================================================
target_compile_options(${TARGET_NAME}
    PUBLIC
        ${COMMON_COMPILE_OPTIONS}
        ${DPDK_CFLAGS_OTHER}
)

# =============================================================================
# 配置文件复制
# =============================================================================
file(GLOB_RECURSE WORKLOADS "${CMAKE_SOURCE_DIR}/workloads/*")
foreach(workload_file ${WORKLOADS})
    get_filename_component(workload_name ${workload_file} NAME)
    configure_file(${workload_file} ${CMAKE_CURRENT_BINARY_DIR}/workloads/${workload_name} COPYONLY)
endforeach()

file(GLOB_RECURSE CONF_FILES "${CMAKE_SOURCE_DIR}/conf/*")
foreach(conf_file ${CONF_FILES})
    get_filename_component(conf_name ${conf_file} NAME)
    configure_file(${conf_file} ${CMAKE_CURRENT_BINARY_DIR}/conf/${conf_name} COPYONLY)
endforeach()

# =============================================================================
# 安装配置
# =============================================================================
include(GNUInstallDirs)

install(TARGETS ${TARGET_NAME}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)

install(DIRECTORY ${CMAKE_SOURCE_DIR}/workloads/
    DESTINATION ${CMAKE_INSTALL_DATADIR}/${PROJECT_NAME}/workloads
)

install(DIRECTORY ${CMAKE_SOURCE_DIR}/conf/
    DESTINATION ${CMAKE_INSTALL_DATADIR}/${PROJECT_NAME}/conf
)

# =============================================================================
# 摘要输出
# =============================================================================
message(STATUS "")
message(STATUS "=== ${PROJECT_NAME} Configuration Summary ===")
message(STATUS "  Version:          ${PROJECT_VERSION}")
message(STATUS "  Build type:       ${CMAKE_BUILD_TYPE}")
message(STATUS "  C++ Standard:     ${CMAKE_CXX_STANDARD}")
message(STATUS "  Install prefix:   ${CMAKE_INSTALL_PREFIX}")
message(STATUS "  TBB:              ${TBB_VERSION}")
message(STATUS "  DPDK:             ${DPDK_VERSION}")
message(STATUS "===========================================")
message(STATUS "")

cmake_minimum_required(VERSION 4.0)
project(ycsbc
    VERSION 1.0
    LANGUAGES CXX
)

set(HOME_DIR $ENV{HOME})
message(STATUS "HOME_DIR = ${HOME_DIR}")

if(EXISTS "${HOME_DIR}/gcc/bin/gcc" AND EXISTS "${HOME_DIR}/gcc/bin/g++")
    set(CMAKE_C_COMPILER "${HOME_DIR}/gcc/bin/gcc")
    set(CMAKE_CXX_COMPILER "${HOME_DIR}/gcc/bin/g++")
endif()

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

message(STATUS "Using C++ standard: ${CMAKE_CXX_STANDARD}")

add_compile_options(
    -g
    -Wall
    -Wextra
    -Werror
)

# 包含目录设置
include_directories(
    ${CMAKE_SOURCE_DIR}/
    ${CMAKE_SOURCE_DIR}/core
    ${CMAKE_SOURCE_DIR}/db
    ${CMAKE_SOURCE_DIR}/lib
)

cmake_policy(SET CMP0167 NEW)

if(EXISTS "${HOME_DIR}/lib/boost")
    set(BOOST_ROOT "${HOME_DIR}/lib/boost")
    set(Boost_NO_SYSTEM_PATHS ON)
endif()
find_package(Boost REQUIRED COMPONENTS system)
if(Boost_FOUND)
    message(STATUS "Boost found: ${Boost_VERSION}")
    message(STATUS "Boost include dir: ${Boost_INCLUDE_DIRS}")
    message(STATUS "Boost libraries: ${Boost_LIBRARIES}")
else()
    message(FATAL_ERROR "Boost not found!")
endif()

if(EXISTS "${HOME_DIR}/lib/tbb")
    set(TBB_ROOT "${HOME_DIR}/lib/tbb")
    set(TBB_NO_SYSTEM_PATHS ON)
endif()
find_package(TBB REQUIRED)
if(TBB_FOUND)
    message(STATUS "TBB found: ${TBB_VERSION}")
    message(STATUS "TBB include dir: ${TBB_INCLUDE_DIRS}")
    message(STATUS "TBB libraries: ${TBB_LIBRARIES}")
else()
    message(FATAL_ERROR "TBB not found!")
endif()

find_package(Threads REQUIRED)
if(Threads_FOUND)
    message(STATUS "Threads library: ${CMAKE_THREAD_LIBS_INIT}")
endif()

find_package(PkgConfig REQUIRED)

if(EXISTS "${HOME_DIR}/lib/dpdk")
    set(DPDK_PATH "${HOME_DIR}/lib/dpdk")
    set(DPDK_LIB_DIR "${DPDK_PATH}/lib/x86_64-linux-gnu")
    set(ENV{PKG_CONFIG_PATH} "${DPDK_LIB_DIR}/pkgconfig:$ENV{PKG_CONFIG_PATH}")
endif()
find_package(PkgConfig REQUIRED)
pkg_check_modules(DPDK REQUIRED libdpdk)
include_directories(${DPDK_INCLUDE_DIRS})
link_directories(${DPDK_LIBRARY_DIRS})
if(DPDK_FOUND)
    message(STATUS "DPDK_INCLUDE_DIRS: ${DPDK_PATH}/include")
    message(STATUS "DPDK_LIBRARY_DIRS: ${DPDK_LIBRARY_DIRS}")
    message(STATUS "DPDK_LIBRARIES: ${DPDK_LIBRARIES}")
else()
    message(FATAL_ERROR "DPDK not found. Make sure pkg-config can find libdpdk.pc.")
endif()


file(GLOB_RECURSE SOURCE_FILES
    "${CMAKE_SOURCE_DIR}/core/*.cc"
    "${CMAKE_SOURCE_DIR}/db/*.cc"
    "${CMAKE_SOURCE_DIR}/lib/*.cc"
    "${CMAKE_SOURCE_DIR}/ycsbc.cc"
)

set(TARGET_NAME ycsbc)

add_executable(${TARGET_NAME} ${SOURCE_FILES})

target_include_directories(${TARGET_NAME}
    PUBLIC
        ${BOOST_ROOT}/include
        ${DPDK_PATH}/include
)

target_link_directories(${TARGET_NAME}
    PUBLIC
        ${DPDK_LIB_DIR}
)

target_link_libraries(${TARGET_NAME}
    PUBLIC
        Boost::system
        TBB::tbb
        Threads::Threads
        ${DPDK_LIBRARIES}
)

target_compile_options(${TARGET_NAME}
    PUBLIC
        ${DPDK_CFLAGS_OTHER}
)

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

install(
    TARGETS ${TARGET_NAME}
    RUNTIME DESTINATION bin
)

set_property(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR} PROPERTY CLEAN_NO_CUSTOM 1)

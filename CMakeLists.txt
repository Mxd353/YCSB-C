cmake_minimum_required(VERSION 3.10)
project(ycsbc
    VERSION 1.0
    LANGUAGES CXX
)

# 设置编译器选项
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# 全局编译选项
add_compile_options(
    -g
    -Wall
    -Wextra
    -Wpedantic
    -Werror
    -pthread
)

# 包含目录设置
include_directories(
    ${CMAKE_SOURCE_DIR}/
    ${CMAKE_SOURCE_DIR}/core
    ${CMAKE_SOURCE_DIR}/db
    ${CMAKE_SOURCE_DIR}/utils
)

cmake_policy(SET CMP0167 NEW)
# 查找依赖项
find_package(Boost REQUIRED COMPONENTS system)
find_package(TBB REQUIRED)
find_package(Threads REQUIRED)
find_package(PkgConfig REQUIRED)

pkg_check_modules(DPDK REQUIRED libdpdk)
message(STATUS "DPDK_INCLUDE_DIRS: ${DPDK_INCLUDE_DIRS}")
message(STATUS "DPDK_LIBRARIES: ${DPDK_LIBRARIES}")

file(GLOB_RECURSE SOURCE_FILES
    "${CMAKE_SOURCE_DIR}/core/*.cc"
    "${CMAKE_SOURCE_DIR}/db/*.cc"
    "${CMAKE_SOURCE_DIR}/utils/*.cc"
    "${CMAKE_SOURCE_DIR}/ycsbc.cc"
)

set(TARGET_NAME ycsbc)

# 生成可执行文件
add_executable(${TARGET_NAME} ${SOURCE_FILES})

# 链接配置
target_link_libraries(${TARGET_NAME}
    PRIVATE
        Boost::system
        TBB::tbb
        Threads::Threads
        ${DPDK_LIBRARIES}
)

target_include_directories(${TARGET_NAME}
    PRIVATE
        ${DPDK_INCLUDE_DIRS}
)

target_compile_options(${TARGET_NAME} PUBLIC ${DPDK_CFLAGS_OTHER})

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

# 安装规则
install(
    TARGETS ${TARGET_NAME}
    RUNTIME DESTINATION bin
)

# 清理辅助配置
set_property(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR} PROPERTY CLEAN_NO_CUSTOM 1)

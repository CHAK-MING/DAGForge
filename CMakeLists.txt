cmake_minimum_required(VERSION 3.25)

project(DAGForge VERSION 0.1.0 LANGUAGES C CXX)

# Compiler Requirements Check
if(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS "19.0")
        message(FATAL_ERROR "DAGForge requires at least Clang 19.0 for C++23 features. Current version: ${CMAKE_CXX_COMPILER_VERSION}")
    endif()
elseif(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS "14.0")
        message(FATAL_ERROR "DAGForge requires at least GCC 14.0 for C++23 features. Current version: ${CMAKE_CXX_COMPILER_VERSION}")
    endif()
else()
    message(FATAL_ERROR "DAGForge requires Clang 19+ or GCC 14+ for C++23 support. Current compiler: ${CMAKE_CXX_COMPILER_ID}")
endif()

# Explicitly enable languages to ensure toolchains are fully initialized
enable_language(C)
enable_language(CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Enable ccache if available
find_program(CCACHE_PROGRAM ccache)
if(CCACHE_PROGRAM)
    set(CMAKE_C_COMPILER_LAUNCHER ${CCACHE_PROGRAM})
    set(CMAKE_CXX_COMPILER_LAUNCHER ${CCACHE_PROGRAM})
    if(NOT DEFINED ENV{CCACHE_TEMPDIR} OR "$ENV{CCACHE_TEMPDIR}" STREQUAL "")
        file(MAKE_DIRECTORY "${CMAKE_BINARY_DIR}/.ccache-tmp")
        set(ENV{CCACHE_TEMPDIR} "${CMAKE_BINARY_DIR}/.ccache-tmp")
    endif()
    message(STATUS "Using ccache: ${CCACHE_PROGRAM}")
endif()

# Parallel compilation
include(ProcessorCount)
ProcessorCount(CPU_COUNT)
if(CPU_COUNT GREATER 0)
    set(CMAKE_BUILD_PARALLEL_LEVEL ${CPU_COUNT})
endif()

option(DAGFORGE_ENABLE_TESTS "Build tests" OFF)
option(DAGFORGE_ENABLE_COVERAGE "Enable code coverage" OFF)
option(DAGFORGE_ENABLE_CLANG_TIDY "Enable clang-tidy analysis" OFF)
option(DAGFORGE_DISABLE_LIBURING "Disable io_uring integration" OFF)
option(DAGFORGE_USE_MIMALLOC "Use mimalloc as global allocator" ON)

if(DAGFORGE_ENABLE_CLANG_TIDY)
    find_program(DAGFORGE_CLANG_TIDY_PROGRAM NAMES clang-tidy-20 clang-tidy)
    if(DAGFORGE_CLANG_TIDY_PROGRAM)
        message(STATUS "Clang-tidy enabled for DAGForge targets: ${DAGFORGE_CLANG_TIDY_PROGRAM}")
    else()
        message(WARNING "clang-tidy not found, analysis disabled")
    endif()
endif()

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

include(ProjectOptions)
include(Dependencies)

add_library(dagforge_lib
    src/dagforge/core/shard.cpp
    src/dagforge/core/runtime.cpp
    src/dagforge/io/context.cpp
    src/dagforge/scheduler/cron.cpp
    src/dagforge/scheduler/engine.cpp
    src/dagforge/dag/dag.cpp
    src/dagforge/dag/dag_run.cpp
    src/dagforge/dag/dag_manager.cpp
    src/dagforge/storage/mysql_database.cpp
    src/dagforge/executor/shell_executor.cpp
    src/dagforge/executor/docker_executor.cpp
    src/dagforge/executor/sensor_executor.cpp
    src/dagforge/executor/composite_executor.cpp
    src/dagforge/app/config_builder.cpp
    src/dagforge/app/application.cpp
    src/dagforge/app/api/api_server.cpp
    src/dagforge/app/services/execution_service.cpp
    src/dagforge/app/services/scheduler_service.cpp
    src/dagforge/app/services/persistence_service.cpp
    src/dagforge/client/http/http_types.cpp
    src/dagforge/client/http/http_parser.cpp
    src/dagforge/app/http/router.cpp
    src/dagforge/app/http/http_server.cpp
    src/dagforge/client/http/http_client.cpp
    src/dagforge/app/http/websocket.cpp
    src/dagforge/config/dag_definition.cpp
    src/dagforge/config/dag_file_loader.cpp
    src/dagforge/config/config_watcher.cpp
    src/dagforge/config/config.cpp
    src/dagforge/cli/serve.cpp
    src/dagforge/cli/run.cpp
    src/dagforge/cli/list.cpp
    src/dagforge/cli/status.cpp
    src/dagforge/cli/validate.cpp
    src/dagforge/cli/state.cpp
    src/dagforge/cli/clear.cpp
    src/dagforge/cli/db.cpp
    src/dagforge/cli/logs.cpp
    src/dagforge/cli/test.cpp
    src/dagforge/cli/management_client.cpp
    src/dagforge/util/daemon.cpp
    src/dagforge/util/id.cpp
    src/dagforge/xcom/xcom_util.cpp
    src/dagforge/xcom/xcom_extractor.cpp
    src/dagforge/xcom/template_resolver.cpp
)

# Precompiled headers for faster compilation
message(STATUS "Enabling precompiled headers for dagforge_lib...")
target_precompile_headers(dagforge_lib PRIVATE
    <cstdint>
    <cstddef>
    <string>
    <string_view>
    <vector>
    <array>
    <span>
    <unordered_map>
    <unordered_set>
    <map>
    <set>
    <memory>
    <optional>
    <variant>
    <expected>
    <functional>
    <utility>
    <atomic>
    <mutex>
    <condition_variable>
    <thread>
    <chrono>
    <ranges>
    <coroutine>
    <system_error>
    <format>
)
add_library(DAGForge::lib ALIAS dagforge_lib)

target_include_directories(dagforge_lib
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

target_compile_definitions(dagforge_lib
    PUBLIC
        $<$<BOOL:${LIBURING_FOUND}>:BOOST_ASIO_HAS_IO_URING=1>
        $<$<NOT:$<BOOL:${LIBURING_FOUND}>>:BOOST_ASIO_HAS_IO_URING=0>
        $<$<BOOL:${DAGFORGE_USE_MIMALLOC}>:DAGFORGE_USE_MIMALLOC=1>
        $<$<NOT:$<BOOL:${DAGFORGE_USE_MIMALLOC}>>:DAGFORGE_USE_MIMALLOC=0>
)

target_link_libraries(dagforge_lib
    PUBLIC
        glaze::glaze
        unordered_dense::unordered_dense
        Boost::system
        Boost::url
        Boost::charconv
        Boost::process
        OpenSSL::SSL
        OpenSSL::Crypto
        $<$<BOOL:${DAGFORGE_USE_MIMALLOC}>:mimalloc>
        $<$<BOOL:${LIBURING_FOUND}>:${LIBURING_LINK_LIBRARIES}>
    PRIVATE
        CLI11::CLI11
)

dagforge_configure_target(dagforge_lib)

# Main executable
add_executable(dagforge
    src/main.cpp
)

target_link_libraries(dagforge PRIVATE DAGForge::lib CLI11::CLI11)
dagforge_configure_target(dagforge)

# Tests
if(DAGFORGE_ENABLE_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()

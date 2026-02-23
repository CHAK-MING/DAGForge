include(FetchContent)

if(CMAKE_VERSION VERSION_GREATER_EQUAL "3.25")
    set(FETCHCONTENT_SYSTEM TRUE)
endif()

# Show progress for long dependency operations (download/configure/build).
set(FETCHCONTENT_QUIET OFF)

# --- Core libraries ---------------------------------------------------------

FetchContent_Declare(
    glaze
    GIT_REPOSITORY https://github.com/stephenberry/glaze.git
    GIT_TAG v7.0.2
    GIT_SHALLOW TRUE
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE
    SYSTEM
)
message(STATUS "Preparing dependency: glaze")
FetchContent_MakeAvailable(glaze)

find_package(CLI11 QUIET)
if(NOT CLI11_FOUND)
    FetchContent_Declare(
        CLI11
        GIT_REPOSITORY https://github.com/CLIUtils/CLI11.git
        GIT_TAG v2.4.2
        GIT_SHALLOW TRUE
        DOWNLOAD_EXTRACT_TIMESTAMP TRUE
        SYSTEM
    )
    set(CLI11_BUILD_TESTS OFF CACHE BOOL "" FORCE)
    set(CLI11_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
    set(CLI11_BUILD_DOCS OFF CACHE BOOL "" FORCE)
    message(STATUS "Preparing dependency: CLI11 (FetchContent fallback)")
    FetchContent_MakeAvailable(CLI11)
else()
    message(STATUS "Found system CLI11: ${CLI11_VERSION}")
endif()

FetchContent_Declare(
    unordered_dense
    GIT_REPOSITORY https://github.com/martinus/unordered_dense.git
    GIT_TAG v4.5.0
    GIT_SHALLOW TRUE
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE
    SYSTEM
)
message(STATUS "Preparing dependency: ankerl::unordered_dense")
FetchContent_MakeAvailable(unordered_dense)

# CMake 3.30+ removed legacy FindBoost module behavior by policy CMP0167.
if(POLICY CMP0167)
    cmake_policy(SET CMP0167 NEW)
endif()

# Prefer a coherent distro Boost stack (1.88+) when available, but do not
# hard-pin paths so other environments/versions continue to work.
find_package(Boost 1.88 CONFIG QUIET COMPONENTS system url charconv
    PATHS /usr/lib/x86_64-linux-gnu/cmake /lib/x86_64-linux-gnu/cmake
)
if(NOT Boost_FOUND)
    find_package(Boost CONFIG REQUIRED COMPONENTS system url charconv)
endif()
if(Boost_FOUND)
    message(STATUS "Found Boost: ${Boost_VERSION}")
endif()

find_package(boost_process CONFIG QUIET
    HINTS /usr/lib/x86_64-linux-gnu/cmake /lib/x86_64-linux-gnu/cmake
)
if(NOT boost_process_FOUND)
    find_package(boost_process CONFIG REQUIRED)
endif()

find_package(OpenSSL REQUIRED)
# SQLite3 removed: replaced by Boost.MySQL async persistence layer.

if(DAGFORGE_USE_MIMALLOC)
    find_package(mimalloc CONFIG QUIET)
    if(NOT mimalloc_FOUND)
        FetchContent_Declare(
            mimalloc
            GIT_REPOSITORY https://github.com/microsoft/mimalloc.git
            GIT_TAG v2.1.7
            GIT_SHALLOW TRUE
            DOWNLOAD_EXTRACT_TIMESTAMP TRUE
            SYSTEM
        )
        set(MI_BUILD_TESTS OFF CACHE BOOL "" FORCE)
        set(MI_BUILD_OBJECT OFF CACHE BOOL "" FORCE)
        set(MI_BUILD_STATIC OFF CACHE BOOL "" FORCE)
        set(MI_BUILD_SHARED ON CACHE BOOL "" FORCE)
        set(MI_OVERRIDE ON CACHE BOOL "" FORCE)
        message(STATUS "Preparing dependency: mimalloc")
        FetchContent_MakeAvailable(mimalloc)
    else()
        message(STATUS "Found system mimalloc")
    endif()
endif()

set(LIBURING_FOUND FALSE)
set(LIBURING_LINK_LIBRARIES "")
if(NOT DAGFORGE_DISABLE_LIBURING)
    find_package(PkgConfig QUIET)
    if(PkgConfig_FOUND)
        pkg_check_modules(LIBURING liburing)
        if(LIBURING_FOUND)
            message(STATUS "io_uring support enabled (liburing found)")
        else()
            message(STATUS "io_uring support disabled automatically (liburing not found)")
        endif()
    else()
        message(STATUS "io_uring support disabled automatically (PkgConfig not found)")
    endif()
else()
    message(STATUS "io_uring support disabled by DAGFORGE_DISABLE_LIBURING=ON")
endif()

# --- Test-only dependencies -------------------------------------------------
if(DAGFORGE_ENABLE_TESTS)
    find_package(GTest QUIET)
    if(NOT GTest_FOUND)
        FetchContent_Declare(
            googletest
            GIT_REPOSITORY https://github.com/google/googletest.git
            GIT_TAG v1.14.0
            GIT_SHALLOW TRUE
            DOWNLOAD_EXTRACT_TIMESTAMP TRUE
            SYSTEM
        )
        set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
        message(STATUS "Preparing dependency: googletest")
        FetchContent_MakeAvailable(googletest)
    else()
        message(STATUS "Found system GTest: ${GTest_VERSION}")
    endif()

    FetchContent_Declare(
        benchmark
        GIT_REPOSITORY https://github.com/google/benchmark.git
        GIT_TAG v1.9.1
        GIT_SHALLOW TRUE
        DOWNLOAD_EXTRACT_TIMESTAMP TRUE
        SYSTEM
    )
    set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "" FORCE)
    set(BENCHMARK_ENABLE_GTEST_TESTS OFF CACHE BOOL "" FORCE)
    set(BENCHMARK_ENABLE_INSTALL OFF CACHE BOOL "" FORCE)
    message(STATUS "Preparing dependency: benchmark")
    FetchContent_MakeAvailable(benchmark)
endif()

function(dagforge_configure_target target_name)
    target_compile_features(${target_name} PRIVATE cxx_std_23)

    if(DAGFORGE_ENABLE_CLANG_TIDY AND DAGFORGE_CLANG_TIDY_PROGRAM)
        set_target_properties(${target_name} PROPERTIES
            CXX_CLANG_TIDY "${DAGFORGE_CLANG_TIDY_PROGRAM}"
        )
    endif()

    if(DAGFORGE_ENABLE_COVERAGE)
        if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
            target_compile_options(${target_name} PRIVATE --coverage -fno-inline -fno-inline-small-functions -fno-default-inline)
            target_link_options(${target_name} PRIVATE --coverage)
        endif()
    endif()

    if(CMAKE_BUILD_TYPE STREQUAL "Release")
        include(CheckIPOSupported OPTIONAL RESULT_VARIABLE _check_ipo_module)
        if(COMMAND check_ipo_supported)
            check_ipo_supported(RESULT result OUTPUT output)
            if(result)
                set_target_properties(${target_name} PROPERTIES INTERPROCEDURAL_OPTIMIZATION TRUE)
            endif()
        endif()
    endif()

    set_target_properties(${target_name} PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin"
        ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib"
        LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib"
    )
endfunction()

include(CheckCXXCompilerFlag)

if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    # Baseline hardening flags
    add_compile_options(
        -fstack-protector-strong
        -Wformat-security
    )

    check_cxx_compiler_flag("-fstack-clash-protection" COMPILER_SUPPORTS_STACK_CLASH)
    if(COMPILER_SUPPORTS_STACK_CLASH)
        add_compile_options(-fstack-clash-protection)
    endif()

    add_compile_definitions(_FORTIFY_SOURCE=3)
    add_compile_options(-fPIE -fPIC)
    add_link_options(-pie)

    add_link_options(-Wl,-z,relro -Wl,-z,now)

    # CET / Control-flow protection on x86_64 when toolchain supports it
    if(CMAKE_SYSTEM_PROCESSOR MATCHES "x86_64|amd64|AMD64")
        check_cxx_compiler_flag("-fcf-protection=full" COMPILER_SUPPORTS_CFI)
        if(COMPILER_SUPPORTS_CFI)
            add_compile_options(-fcf-protection=full)
        endif()
    endif()

    # Warning policy
    add_compile_options(
        -Wall -Wextra -Wpedantic
        -Wshadow -Wnon-virtual-dtor -Wold-style-cast
        -Wcast-align -Wunused -Woverloaded-virtual
        -Wnull-dereference -Wdouble-promotion -Wformat=2
    )

    if(CMAKE_CXX_COMPILER_ID MATCHES "Clang" AND CMAKE_CXX_COMPILER_VERSION VERSION_GREATER_EQUAL "20.0")
        add_compile_options(-Wno-error=format-nonliteral)
    endif()
endif()

if(MSVC)
    add_compile_options(/W4 /permissive-)
endif()

if(CMAKE_BUILD_TYPE STREQUAL "Release")
    check_cxx_compiler_flag("-march=native" COMPILER_SUPPORTS_MARCH_NATIVE)
    if(COMPILER_SUPPORTS_MARCH_NATIVE)
        add_compile_options(-march=native)
    endif()

    include(CheckIPOSupported)
    check_ipo_supported(RESULT result OUTPUT output)
    if(result)
        message(STATUS "LTO/IPO supported")
    else()
        message(WARNING "LTO/IPO is not supported: ${output}")
    endif()
endif()

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

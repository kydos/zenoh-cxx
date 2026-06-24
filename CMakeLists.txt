cmake_minimum_required(VERSION 3.28)
project(zenoh CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
# GNU extensions are required for the statement-expression form of ZTRY (PLAN.md D1).
set(CMAKE_CXX_EXTENSIONS ON)
set(CMAKE_CXX_SCAN_FOR_MODULES ON)
# Emit compile_commands.json so clang-tidy (scripts/tidy.sh) can replay the exact
# per-translation-unit module flags.
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Release/RelWithDebInfo keep CMake's default optimization flags, which include
# `-O3 -DNDEBUG` (Release) / `-O2 -g -DNDEBUG` (RelWithDebInfo). Do NOT override
# CMAKE_CXX_FLAGS_<CONFIG> wholesale — that silently drops `-DNDEBUG` (re-enabling
# asserts and disabling NDEBUG fast paths). Extra optimization is layered on below.

# Link-time optimization recovers cross-module inlining for the message-layer
# encode/decode bodies that live in .cpp implementation units (RESTRUCTURE.md A2).
# Enabled for optimized configs; left off for Debug/sanitizer/fuzz builds.
include(CheckIPOSupported)
check_ipo_supported(RESULT ZENOH_IPO_OK OUTPUT ZENOH_IPO_MSG)
if(ZENOH_IPO_OK)
    set(CMAKE_INTERPROCEDURAL_OPTIMIZATION_RELEASE ON)
    set(CMAKE_INTERPROCEDURAL_OPTIMIZATION_RELWITHDEBINFO ON)
else()
    message(STATUS "IPO/LTO not supported: ${ZENOH_IPO_MSG}")
endif()

# Host-CPU tuning for the fastest code on the build machine. Produces a non-portable
# binary, so it is opt-in (the release presets enable it); a bare Release stays
# portable. Applied only to optimized configs. On AArch64 `-mcpu=native` is the right
# flag (it also selects the core's scheduling model), `-march=native` elsewhere.
option(ZENOH_NATIVE "Tune for the host CPU (non-portable, fastest local code)" OFF)
if(ZENOH_NATIVE AND CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
    include(CheckCXXCompilerFlag)
    if(CMAKE_SYSTEM_PROCESSOR MATCHES "arm64|aarch64")
        check_cxx_compiler_flag("-mcpu=native" ZENOH_HAS_MCPU_NATIVE)
        if(ZENOH_HAS_MCPU_NATIVE)
            add_compile_options($<$<NOT:$<CONFIG:Debug>>:-mcpu=native>)
        endif()
    else()
        check_cxx_compiler_flag("-march=native" ZENOH_HAS_MARCH_NATIVE)
        if(ZENOH_HAS_MARCH_NATIVE)
            add_compile_options($<$<NOT:$<CONFIG:Debug>>:-march=native>)
        endif()
    endif()
    # Drop unreferenced code/data from optimized executables (macOS ld64).
    if(APPLE)
        add_link_options($<$<NOT:$<CONFIG:Debug>>:-Wl,-dead_strip>)
    endif()
endif()

option(ZENOH_SANITIZE "Build with ASan + UBSan" OFF)
if(ZENOH_SANITIZE)
    add_compile_options(-fsanitize=address,undefined -fno-omit-frame-pointer)
    add_link_options(-fsanitize=address,undefined)
endif()

# LLVM source-based code coverage (clang only). Instruments every target; running
# the test binary then emits .profraw which scripts/coverage.sh merges into an lcov
# report. Use a dedicated, un-optimized, sanitizer-free build (the `*-coverage`
# presets) for accurate line mapping.
option(ZENOH_COVERAGE "Instrument for LLVM source-based coverage (clang)" OFF)
if(ZENOH_COVERAGE)
    add_compile_options(-fprofile-instr-generate -fcoverage-mapping)
    add_link_options(-fprofile-instr-generate -fcoverage-mapping)
endif()

# The codec/proto layer as a standalone static library, unit-testable in isolation
# from any I/O. Module interface units (.cppm) carry the public, documented API;
# message-layer implementation units (.cpp) carry the encode/decode bodies, while
# the hot primitive/codec layer stays inline in its interface units.
add_library(zenoh-proto STATIC)
target_sources(zenoh-proto
    PUBLIC
        FILE_SET CXX_MODULES FILES
            src/util/util.cppm
            src/buffer/buffer.cppm
            src/codec/varint.cppm
            src/codec/codec.cppm
            src/codec/ext.cppm
            src/proto/fields/fields.cppm
            src/proto/exts/exts.cppm
            src/proto/network/network.cppm
            src/proto/transport/transport.cppm
            src/proto/declare/declare.cppm
            src/proto/interest/interest.cppm
            src/proto/proto.cppm
    PRIVATE
            src/proto/fields/fields.cpp
            src/proto/exts/exts.cpp
            src/proto/network/network.cpp
            src/proto/transport/transport.cpp
            src/proto/declare/declare.cpp
            src/proto/interest/interest.cpp
)
target_include_directories(zenoh-proto PUBLIC include)
target_compile_features(zenoh-proto PUBLIC cxx_std_23)

# The user-facing runtime: a vertically-integrated client (TCP transport + session)
# built on the pure `zenoh-proto` codec (PLAN.md D8). This layer owns OS I/O, so it
# is kept separate from the I/O-free codec library.
add_library(zenoh STATIC)
target_sources(zenoh
    PUBLIC
        FILE_SET CXX_MODULES FILES
            src/runtime/tcp.cppm
            src/runtime/strand.cppm
            src/runtime/session.cppm
            src/zenoh.cppm
    PRIVATE
            src/runtime/tcp.cpp
            src/runtime/session.cpp
)
target_link_libraries(zenoh PUBLIC zenoh-proto)
target_include_directories(zenoh PUBLIC include)
target_compile_features(zenoh PUBLIC cxx_std_23)

option(ZENOH_EXAMPLES "Build the runtime example programs" ON)
if(ZENOH_EXAMPLES)
    add_subdirectory(examples)
endif()

enable_testing()
add_subdirectory(tests)

# libFuzzer target over the decoders (clang only). Brings its own fuzzer+sanitizer
# instrumentation; build with the `linux-fuzz` preset and run via scripts/fuzz.sh.
option(ZENOH_FUZZ "Build the libFuzzer decode target (clang only)" OFF)
if(ZENOH_FUZZ)
    add_executable(zenoh-fuzz fuzz/fuzz_decode.cpp)
    target_link_libraries(zenoh-fuzz PRIVATE zenoh-proto)
    target_compile_features(zenoh-fuzz PRIVATE cxx_std_23)
    target_compile_options(zenoh-fuzz PRIVATE -g -fsanitize=fuzzer,address,undefined)
    target_link_options(zenoh-fuzz PRIVATE -fsanitize=fuzzer,address,undefined)
endif()

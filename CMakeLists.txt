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

# ThreadSanitizer, mutually exclusive with ASan+UBSan (ASan and TSan cannot be linked
# into the same binary). This is the dynamic half of the broker's concurrency
# verification story (see the broker plan's "Concurrency and thread safety" section):
# paired with the strand-discipline `assert(strand_.running_in_this_thread())` checks
# in broker/src/{tables,broker}.cpp, a clean TSan run over tests/test_broker.cpp's
# multithreaded cases is the actual check that the two-tier strand design has no data
# races, not just an assumption from the design.
option(ZENOH_TSAN "Build with ThreadSanitizer (mutually exclusive with ZENOH_SANITIZE)" OFF)
if(ZENOH_TSAN)
    if(ZENOH_SANITIZE)
        message(FATAL_ERROR "ZENOH_TSAN and ZENOH_SANITIZE cannot both be ON (ASan/TSan cannot be linked together)")
    endif()
    add_compile_options(-fsanitize=thread -fno-omit-frame-pointer)
    add_link_options(-fsanitize=thread)
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
            src/ke/ke.cppm
            src/proto/fields/fields.cppm
            src/proto/exts/exts.cppm
            src/proto/network/network.cppm
            src/proto/transport/transport.cppm
            src/proto/declare/declare.cppm
            src/proto/interest/interest.cppm
            src/proto/proto.cppm
    PRIVATE
            src/ke/ke.cpp
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

# Vendored standalone (non-Boost) Asio, broker-only (see third_party/asio/NOTICE.md
# for the pinned version and vendoring rationale — header-only, checked into the
# repo rather than fetched at configure time to keep the build hermetic/offline,
# matching this project's existing "no network fetch" convention). Neither
# zenoh-proto nor zenoh (the client) depend on this; only zenoh-broker/zenohb do.
add_library(asio INTERFACE)
target_include_directories(asio INTERFACE third_party/asio/asio/include)
target_compile_definitions(asio INTERFACE ASIO_STANDALONE ASIO_NO_DEPRECATED)
target_link_libraries(asio INTERFACE $<$<PLATFORM_ID:Linux>:pthread>)

# The broker: a multithreaded ASIO reactor over the pure zenoh-proto codec (see
# the broker plan). Links zenoh-proto (codec + zenoh.ke), never zenoh (the
# client runtime) or its blocking zenoh.runtime.tcp. Declared here at root,
# like zenoh-proto/zenoh, rather than via add_subdirectory(broker) — no module
# FILE_SET target in this repo is declared from a subdirectory, and there is no
# reason to be the first (see the broker plan's M3 section).
add_library(zenoh-broker STATIC)
target_sources(zenoh-broker
    PUBLIC
        FILE_SET CXX_MODULES FILES
            broker/src/resource.cppm
            broker/src/membership.cppm
            broker/src/tables.cppm
            broker/src/broker.cppm
    PRIVATE
            broker/src/resource.cpp
            broker/src/membership.cpp
            broker/src/tables.cpp
            broker/src/broker.cpp
)
target_link_libraries(zenoh-broker PUBLIC zenoh-proto PRIVATE asio)
target_include_directories(zenoh-broker PUBLIC include)
target_compile_features(zenoh-broker PUBLIC cxx_std_23)

add_executable(zenohb broker/src/main.cpp)
target_link_libraries(zenohb PRIVATE zenoh-broker)
target_compile_features(zenohb PRIVATE cxx_std_23)

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

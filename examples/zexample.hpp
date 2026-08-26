#pragma once
//
// Helpers shared by the example programs: the `ZError` -> name mapping every example
// needs on its error paths, the conversions between the runtime API's byte spans and
// ordinary strings, and the command-line plumbing that keeps every example's CLI
// interchangeable with its zenoh-rust counterpart.
//
// On the CLI: each example parses its own options (they read top-to-bottom, like the
// rest of this codebase), and defers to `parse_common` for the connection options the
// reference examples share via their `CommonArgs`. Only `-e/--connect` maps onto a
// capability this runtime has -- it is a client-only implementation with no config
// file, no scouting, no listening and no shared memory -- but the rest are still
// recognized, so a command line written for the reference binaries can be run against
// these verbatim and gets a note on stderr rather than an "unknown option" failure.
//
// Include this *after* `import zenoh;` -- it names types exported by that module.
//
#include <cstddef>
#include <cstdio>
#include <span>
#include <string>
#include <string_view>

namespace zexample {

/// Human-readable name for a `ZError`, for the examples' diagnostics.
inline auto error_name(zenoh::ZError e) -> const char* {
    switch (e) {
    case zenoh::ZError::would_block:
        return "would_block";
    case zenoh::ZError::connection_closed:
        return "connection_closed";
    case zenoh::ZError::io_error:
        return "io_error";
    case zenoh::ZError::protocol_error:
        return "protocol_error";
    case zenoh::ZError::encode_error:
        return "encode_error";
    case zenoh::ZError::bad_endpoint:
        return "bad_endpoint";
    case zenoh::ZError::already_subscribed:
        return "already_subscribed";
    case zenoh::ZError::already_queryable:
        return "already_queryable";
    case zenoh::ZError::query_timeout:
        return "query_timeout";
    }
    return "unknown";
}

/// Borrow a string's bytes as the payload span `put`/`reply` take.
inline auto as_bytes(std::string_view s) -> std::span<const std::byte> {
    return {reinterpret_cast<const std::byte*>(s.data()), s.size()};
}

/// Borrow a payload span as text. Payloads are arbitrary bytes -- the examples only
/// ever print ones they know to be strings, exactly as the reference examples do.
inline auto as_str(std::span<const std::byte> b) -> std::string_view {
    return {reinterpret_cast<const char*>(b.data()), b.size()};
}

/// The connection options every example shares (the reference's `CommonArgs`).
struct CommonArgs {
    std::string endpoint = "tcp/127.0.0.1:7447";
};

/// What an argv element turned out to be when offered to a parser.
enum class Arg {
    unrecognized, ///< not this parser's option; try the next one
    consumed,     ///< handled (its value, if any, was consumed too)
    fatal,        ///< malformed or unsupported; the example should exit non-zero
};

/// Report an option that needs a value but was given none. Always returns `fatal`.
inline auto missing_value(std::string_view opt) -> Arg {
    std::fprintf(stderr, "error: %.*s requires a value\n", static_cast<int>(opt.size()),
                 opt.data());
    return Arg::fatal;
}

/// Fetch the value that follows the option at `argv[i]`, advancing `i` past it.
/// Returns nullptr, having reported the problem, when the option was given no value.
inline auto option_value(int argc, char** argv, int& i) -> const char* {
    if (i + 1 >= argc) {
        (void)missing_value(argv[i]);
        return nullptr;
    }
    return argv[++i];
}

/// Report an option accepted only for command-line compatibility with the reference
/// examples: it parses, and then does nothing.
inline auto no_effect(std::string_view opt, const char* why) -> void {
    std::fprintf(stderr, "note: %.*s has no effect: %s\n", static_cast<int>(opt.size()), opt.data(),
                 why);
}

/// Report an unrecognized option. Always returns 1, for `return unknown_option(...)`.
inline auto unknown_option(const char* argv0, std::string_view opt) -> int {
    std::fprintf(stderr, "error: unknown option '%.*s'\nTry '%s --help'.\n",
                 static_cast<int>(opt.size()), opt.data(), argv0);
    return 1;
}

/// True when `-h`/`--help` appears anywhere in argv, so an example can print its usage
/// before validating anything else.
inline auto wants_help(int argc, char** argv) -> bool {
    for (int i = 1; i < argc; ++i) {
        std::string_view const arg = argv[i];
        if (arg == "-h" || arg == "--help") return true;
    }
    return false;
}

/// Try to consume argv[i] as one of the shared connection options, advancing `i` past
/// its value when it takes one.
inline auto parse_common(int argc, char** argv, int& i, CommonArgs& common) -> Arg {
    std::string_view const arg = argv[i];

    if (arg == "-e" || arg == "--connect") {
        const char* value = option_value(argc, argv, i);
        if (value == nullptr) return Arg::fatal;
        common.endpoint = value;
        return Arg::consumed;
    }
    if (arg == "-m" || arg == "--mode") {
        const char* value = option_value(argc, argv, i);
        if (value == nullptr) return Arg::fatal;
        std::string_view const mode = value;
        if (mode != "client") {
            std::fprintf(stderr,
                         "error: session mode '%.*s' is not supported: this is a client-only "
                         "implementation, which talks to a router and never peers or routes\n",
                         static_cast<int>(mode.size()), mode.data());
            return Arg::fatal;
        }
        return Arg::consumed;
    }
    if (arg == "-l" || arg == "--listen") {
        if (option_value(argc, argv, i) == nullptr) return Arg::fatal;
        no_effect(arg, "a client session only connects, it never listens");
        return Arg::consumed;
    }
    if (arg == "-c" || arg == "--config") {
        if (option_value(argc, argv, i) == nullptr) return Arg::fatal;
        no_effect(arg, "there is no configuration file; use -e/--connect");
        return Arg::consumed;
    }
    if (arg == "--cfg") {
        if (option_value(argc, argv, i) == nullptr) return Arg::fatal;
        no_effect(arg, "there is no configuration store to patch");
        return Arg::consumed;
    }
    if (arg == "--no-multicast-scouting") {
        no_effect(arg, "scouting is not implemented; the endpoint is always explicit");
        return Arg::consumed;
    }
    if (arg == "--enable-shm") {
        no_effect(arg, "shared memory is not implemented");
        return Arg::consumed;
    }
    return Arg::unrecognized;
}

/// Print the shared options block that closes every example's `--help`.
inline auto print_common_help() -> void {
    std::printf("  -e, --connect <ENDPOINT>\n"
                "          Endpoint to connect to [default: tcp/127.0.0.1:7447]\n"
                "  -m, --mode <MODE>\n"
                "          The Zenoh session mode; only 'client' is supported\n"
                "  -h, --help\n"
                "          Print help\n"
                "\n"
                "Accepted for compatibility with the zenoh-rust examples, but with no effect\n"
                "here: -c/--config <FILE>, --cfg <KEY:VALUE>, -l/--listen <ENDPOINT>,\n"
                "--no-multicast-scouting, --enable-shm.\n");
}

} // namespace zexample

// z_put: connect to a Zenoh router and publish a value to a key expression.
//
// Usage: z_put [OPTIONS] [endpoint] [key] [value]   (--help for the full list)
//   -k, --key <KEY>          key expression [default: demo/example/zenoh-cpp-put]
//   -p, --payload <PAYLOAD>  payload string [default: "Put from C++!"]
//   --try                    use try_put (non-blocking) instead of put
//   --batch                  coalesce the puts into API-level batches (one Frame each)
//   --count <N>              publish N times on the one session (default 1)
//
// The three positional arguments (endpoint, key, value) and --try/--batch/--count are
// extensions of this port with no reference counterpart; -k/-p mirror the reference's
// own options and take precedence over the positional key/value.
//
// Verify with the reference subscriber:
//   zenohd -l tcp/127.0.0.1:7447 &
//   z_sub -e tcp/127.0.0.1:7447 -k 'demo/example/**'

#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <span>
#include <string>
#include <string_view>

import zenoh;

#include "zexample.hpp"

namespace {

auto usage(const char* argv0) -> void {
    std::printf("Usage: %s [OPTIONS] [endpoint] [key] [value]\n\n"
                "Options:\n"
                "  -k, --key <KEY>\n"
                "          The key expression to write to"
                " [default: demo/example/zenoh-cpp-put]\n"
                "  -p, --payload <PAYLOAD>\n"
                "          The payload to write [default: \"Put from C++!\"]\n"
                "\n"
                "Extensions of this port, with no counterpart in the reference example:\n"
                "  [endpoint] [key] [value]\n"
                "          Positional forms of --connect, --key and --payload\n"
                "      --try\n"
                "          Use the non-blocking try_put instead of put\n"
                "      --batch\n"
                "          Coalesce the puts into API-level batches (one Frame per batch)\n"
                "      --count <N>\n"
                "          Publish N times on the one session [default: 1]\n",
                argv0);
    zexample::print_common_help();
}

} // namespace

auto main(int argc, char** argv) -> int {
    if (zexample::wants_help(argc, argv)) {
        usage(argv[0]);
        return 0;
    }
    zexample::CommonArgs common;
    std::string key = "demo/example/zenoh-cpp-put";
    std::string value = "Put from C++!";
    bool use_try = false;
    bool use_batch = false;
    int count = 1;

    int positional = 0;
    for (int i = 1; i < argc; ++i) {
        std::string_view arg = argv[i];
        // -e is handled here rather than by parse_common so that giving it explicitly
        // still consumes the positional endpoint slot, as it always has.
        if (arg == "-e" || arg == "--connect") {
            const char* value_arg = zexample::option_value(argc, argv, i);
            if (value_arg == nullptr) return 1;
            common.endpoint = value_arg;
            if (positional == 0) positional = 1;
        } else if (arg == "-k" || arg == "--key") {
            const char* value_arg = zexample::option_value(argc, argv, i);
            if (value_arg == nullptr) return 1;
            key = value_arg;
        } else if (arg == "-p" || arg == "--payload") {
            const char* value_arg = zexample::option_value(argc, argv, i);
            if (value_arg == nullptr) return 1;
            value = value_arg;
        } else if (arg == "--try") {
            use_try = true;
        } else if (arg == "--batch") {
            use_batch = true;
        } else if (arg == "--count") {
            const char* value_arg = zexample::option_value(argc, argv, i);
            if (value_arg == nullptr) return 1;
            count = std::atoi(value_arg);
        } else if (!arg.empty() && arg.front() != '-' && positional == 0) {
            common.endpoint = arg;
            ++positional;
        } else if (!arg.empty() && arg.front() != '-' && positional == 1) {
            key = arg;
            ++positional;
        } else if (!arg.empty() && arg.front() != '-' && positional == 2) {
            value = arg;
            ++positional;
        } else {
            switch (zexample::parse_common(argc, argv, i, common)) {
            case zexample::Arg::consumed:
                break;
            case zexample::Arg::fatal:
                return 1;
            case zexample::Arg::unrecognized:
                return zexample::unknown_option(argv[0], arg);
            }
        }
    }
    const std::string& endpoint = common.endpoint;

    auto session = zenoh::Session::open(endpoint);
    if (!session) {
        std::fprintf(stderr, "open(%s) failed: %s\n", endpoint.c_str(),
                     zexample::error_name(session.error()));
        return 1;
    }
    std::printf("Connected to %s\n", endpoint.c_str());

    if (use_batch) {
        auto batch = session->batch();
        for (int i = 0; i < count; ++i) {
            std::string const payload = count > 1 ? value + " #" + std::to_string(i) : value;
            if (auto r = batch.put(key, zexample::as_bytes(payload)); !r) {
                std::fprintf(stderr, "batch.put('%s') failed: %s\n", key.c_str(),
                             zexample::error_name(r.error()));
                return 1;
            }
        }
        if (auto r = batch.flush(); !r) {
            std::fprintf(stderr, "batch.flush() failed: %s\n", zexample::error_name(r.error()));
            return 1;
        }
    } else {
        for (int i = 0; i < count; ++i) {
            std::string const payload = count > 1 ? value + " #" + std::to_string(i) : value;
            auto result = use_try ? session->try_put(key, zexample::as_bytes(payload))
                                  : session->put(key, zexample::as_bytes(payload));
            if (!result) {
                std::fprintf(stderr, "%s('%s') failed: %s\n", use_try ? "try_put" : "put",
                             key.c_str(), zexample::error_name(result.error()));
                return result.error() == zenoh::ZError::would_block ? 2 : 1;
            }
            // std::printf("%s('%s') = '%s'\n", use_try ? "try_put" : "put", key.c_str(),
            // payload.c_str());
        }
    }

    session->close();
    return 0;
}

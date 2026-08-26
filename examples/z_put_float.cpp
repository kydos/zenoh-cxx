// z_put_float: connect to a Zenoh router and publish a floating-point value to a key
// expression. The C++23 equivalent of zenoh-rust's z_put_float.rs.
//
// Usage: z_put_float [OPTIONS]  (run with --help for the full option list)
//   -k, --key <KEY>          key expression [default: demo/example/zenoh-cpp-put]
//   -p, --payload <PAYLOAD>  the double to write [default: pi]
//
// The reference serializes the f64 with zenoh-ext's `z_serialize`, which writes the
// raw little-endian bytes of the double (`f64::to_le_bytes()`) — 8 bytes, no tag or
// length prefix. We reproduce that byte layout exactly so a zenoh-rust subscriber can
// `z_deserialize::<f64>` the payload.
//
// Verify with the reference subscriber:
//   zenohd -l tcp/127.0.0.1:7447 &
//   z_sub -e tcp/127.0.0.1:7447 -k 'demo/example/**'

#include <array>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <numbers>
#include <span>
#include <string>
#include <string_view>

import zenoh;

#include "zexample.hpp"

namespace {

auto usage(const char* argv0) -> void {
    std::printf("Usage: %s [OPTIONS]\n\n"
                "Options:\n"
                "  -k, --key <KEY>\n"
                "          The key expression to write to [default: demo/example/zenoh-cpp-put]\n"
                "  -p, --payload <PAYLOAD>\n"
                "          The double to write [default: 3.141592653589793]\n",
                argv0);
    zexample::print_common_help();
}

// Serialize a double the way zenoh-ext's `z_serialize` does: 8 little-endian bytes.
auto serialize_f64(double value) -> std::array<std::byte, sizeof(double)> {
    static_assert(sizeof(double) == 8, "f64 wire format assumes an 8-byte double");
    std::array<std::byte, sizeof(double)> out{};
    std::memcpy(out.data(), &value, sizeof(double)); // host is little-endian (PLAN.md D7)
    return out;
}

} // namespace

auto main(int argc, char** argv) -> int {
    if (zexample::wants_help(argc, argv)) {
        usage(argv[0]);
        return 0;
    }
    zexample::CommonArgs common;
    std::string key = "demo/example/zenoh-cpp-put";
    double payload = std::numbers::pi;

    for (int i = 1; i < argc; ++i) {
        std::string_view arg = argv[i];
        if (arg == "-k" || arg == "--key") {
            const char* value = zexample::option_value(argc, argv, i);
            if (value == nullptr) return 1;
            key = value;
        } else if (arg == "-p" || arg == "--payload") {
            const char* value = zexample::option_value(argc, argv, i);
            if (value == nullptr) return 1;
            payload = std::strtod(value, nullptr);
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

    std::printf("Opening session...\n");
    auto session = zenoh::Session::open(endpoint);
    if (!session) {
        std::fprintf(stderr, "open(%s) failed: %s\n", endpoint.c_str(),
                     zexample::error_name(session.error()));
        return 1;
    }

    std::printf("Putting Float ('%s': '%g')...\n", key.c_str(), payload);
    auto const bytes = serialize_f64(payload);
    if (auto r = session->put(key, std::span<const std::byte>{bytes}); !r) {
        std::fprintf(stderr, "put('%s') failed: %s\n", key.c_str(),
                     zexample::error_name(r.error()));
        return 1;
    }

    session->close();
    return 0;
}

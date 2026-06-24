// z_put_float: connect to a Zenoh router and publish a floating-point value to a key
// expression. The C++23 equivalent of zenoh-rust's z_put_float.rs.
//
// Usage: z_put_float [options]
//   -e, --connect E  router endpoint (default tcp/127.0.0.1:7447)
//   -k, --key K      key expression  (default demo/example/zenoh-cpp-put)
//   -p, --payload P  the double to write (default pi)
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

namespace {

auto error_name(zenoh::ZError e) -> const char* {
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
    }
    return "unknown";
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
    std::string endpoint = "tcp/127.0.0.1:7447";
    std::string key = "demo/example/zenoh-cpp-put";
    double payload = std::numbers::pi;

    for (int i = 1; i < argc; ++i) {
        std::string_view arg = argv[i];
        if ((arg == "-e" || arg == "--connect") && i + 1 < argc) {
            endpoint = argv[++i];
        } else if ((arg == "-k" || arg == "--key") && i + 1 < argc) {
            key = argv[++i];
        } else if ((arg == "-p" || arg == "--payload") && i + 1 < argc) {
            payload = std::strtod(argv[++i], nullptr);
        }
    }

    std::printf("Opening session...\n");
    auto session = zenoh::Session::open(endpoint);
    if (!session) {
        std::fprintf(stderr, "open(%s) failed: %s\n", endpoint.c_str(),
                     error_name(session.error()));
        return 1;
    }

    std::printf("Putting Float ('%s': '%g')...\n", key.c_str(), payload);
    auto const bytes = serialize_f64(payload);
    if (auto r = session->put(key, std::span<const std::byte>{bytes}); !r) {
        std::fprintf(stderr, "put('%s') failed: %s\n", key.c_str(), error_name(r.error()));
        return 1;
    }

    session->close();
    return 0;
}

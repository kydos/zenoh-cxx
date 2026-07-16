// z_pub: connect to a Zenoh router and publish a value to a key expression once a
// second, forever. The C++23 equivalent of zenoh-rust's z_pub.rs.
//
// Usage: z_pub [options]
//   -e, --connect E  router endpoint (default tcp/127.0.0.1:7447)
//   -k, --key K      key expression  (default demo/example/zenoh-cpp-pub)
//   -p, --payload P  payload string  (default "Pub from C++!")
//
// Each iteration publishes "[<idx>] <payload>" — matching the reference. The
// reference also sets a TEXT_PLAIN encoding and an optional attachment and can add a
// matching listener; the runtime `put` carries neither encoding/attachment metadata
// nor subscription matching yet, so those knobs are not modeled here.
//
// Verify with the reference subscriber:
//   zenohd -l tcp/127.0.0.1:7447 &
//   z_sub -e tcp/127.0.0.1:7447 -k 'demo/example/**'

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <span>
#include <string>
#include <string_view>
#include <thread>

import zenoh;

namespace {

auto as_bytes(std::string_view s) -> std::span<const std::byte> {
    return {reinterpret_cast<const std::byte*>(s.data()), s.size()};
}

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
    case zenoh::ZError::already_queryable:
        return "already_queryable";
    case zenoh::ZError::query_timeout:
        return "query_timeout";
    }
    return "unknown";
}

} // namespace

auto main(int argc, char** argv) -> int {
    // Line-buffer stdout so the per-second put lines appear promptly when redirected.
    std::setvbuf(stdout, nullptr, _IOLBF, 0);

    std::string endpoint = "tcp/127.0.0.1:7447";
    std::string key = "demo/example/zenoh-cpp-pub";
    std::string payload = "Pub from C++!";

    for (int i = 1; i < argc; ++i) {
        std::string_view arg = argv[i];
        if ((arg == "-e" || arg == "--connect") && i + 1 < argc) {
            endpoint = argv[++i];
        } else if ((arg == "-k" || arg == "--key") && i + 1 < argc) {
            key = argv[++i];
        } else if ((arg == "-p" || arg == "--payload") && i + 1 < argc) {
            payload = argv[++i];
        }
    }

    std::printf("Opening session...\n");
    auto session = zenoh::Session::open(endpoint);
    if (!session) {
        std::fprintf(stderr, "open(%s) failed: %s\n", endpoint.c_str(),
                     error_name(session.error()));
        return 1;
    }

    std::printf("Declaring Publisher on '%s'...\n", key.c_str());
    std::printf("Press CTRL-C to quit...\n");
    for (std::uint32_t idx = 0;; ++idx) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        std::string const buf = "[" + std::to_string(idx) + "] " + payload;
        std::printf("Putting Data ('%s': '%s')...\n", key.c_str(), buf.c_str());
        if (auto r = session->put(key, as_bytes(buf)); !r) {
            std::fprintf(stderr, "put('%s') failed: %s\n", key.c_str(), error_name(r.error()));
            return 1;
        }
    }

    return 0;
}

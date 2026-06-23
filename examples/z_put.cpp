// z_put: connect to a Zenoh router and publish a value to a key expression.
//
// Usage: z_put [endpoint] [key] [value] [--try]
//   endpoint  router locator (default tcp/127.0.0.1:7447)
//   key       key expression  (default demo/example/zenoh-cpp-put)
//   value     payload string  (default "Hello from zenoh-cpp!")
//   --try     use try_put (non-blocking) instead of put
//   --batch   coalesce the puts into API-level batches (one Frame per batch)
//   --count N publish N times on the one session (default 1)
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
    }
    return "unknown";
}

} // namespace

auto main(int argc, char** argv) -> int {
    std::string endpoint = "tcp/127.0.0.1:7447";
    std::string key = "test/thr";
    std::string value = "01234567";
    bool use_try = false;
    bool use_batch = false;
    int count = 1;

    int positional = 0;
    for (int i = 1; i < argc; ++i) {
        std::string_view arg = argv[i];
        if (arg == "--try") {
            use_try = true;
        } else if (arg == "--batch") {
            use_batch = true;
        } else if (arg == "--count" && i + 1 < argc) {
            count = std::atoi(argv[++i]);
        } else if (positional == 0) {
            endpoint = arg;
            ++positional;
        } else if (positional == 1) {
            key = arg;
            ++positional;
        } else if (positional == 2) {
            value = arg;
            ++positional;
        }
    }

    auto session = zenoh::Session::open(endpoint);
    if (!session) {
        std::fprintf(stderr, "open(%s) failed: %s\n", endpoint.c_str(), error_name(session.error()));
        return 1;
    }
    std::printf("Connected to %s\n", endpoint.c_str());

    if (use_batch) {
        auto batch = session->batch();
        for (int i = 0; i < count; ++i) {
            std::string const payload = count > 1 ? value + " #" + std::to_string(i) : value;
            if (auto r = batch.put(key, as_bytes(payload)); !r) {
                std::fprintf(stderr, "batch.put('%s') failed: %s\n", key.c_str(),
                             error_name(r.error()));
                return 1;
            }
        }
        if (auto r = batch.flush(); !r) {
            std::fprintf(stderr, "batch.flush() failed: %s\n", error_name(r.error()));
            return 1;
        }
    } else {
        for (int i = 0; i < count; ++i) {
            std::string const payload = count > 1 ? value + " #" + std::to_string(i) : value;
            auto result = use_try ? session->try_put(key, as_bytes(payload))
                                  : session->put(key, as_bytes(payload));
            if (!result) {
                std::fprintf(stderr, "%s('%s') failed: %s\n", use_try ? "try_put" : "put", key.c_str(),
                             error_name(result.error()));
                return result.error() == zenoh::ZError::would_block ? 2 : 1;
            }
            // std::printf("%s('%s') = '%s'\n", use_try ? "try_put" : "put", key.c_str(), payload.c_str());
        }
    }

    session->close();
    return 0;
}

// z_pub_thr: publish to "test/thr" as fast as possible and (optionally) report the
// achieved throughput. The C++23 equivalent of zenoh-rust's z_pub_thr.rs.
//
// Usage: z_pub_thr [options] <payload_size>
//   <payload_size>   payload length in bytes (required, positional)
//   -e, --connect E  router endpoint (default tcp/127.0.0.1:7447)
//   -t, --print      print throughput statistics
//   -n, --number N   messages per measurement window (default 100000)
//       --batch B    coalesce B puts per flush via the batching API (default 1)
//
// Our `put` blocks until the message is handed to the transport, matching the
// reference's CongestionControl::Block. Priority/express knobs are not modeled yet.
//
// Measure with the reference subscriber:
//   zenohd -l tcp/127.0.0.1:7447 &
//   z_sub_thr -e tcp/127.0.0.1:7447 <payload_size>   # from ../zenoh-rust

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <span>
#include <string>
#include <string_view>
#include <vector>

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

} // namespace

auto main(int argc, char** argv) -> int {
    // Line-buffer stdout so throughput lines flush promptly even when redirected.
    std::setvbuf(stdout, nullptr, _IOLBF, 0);

    std::string endpoint = "tcp/127.0.0.1:7447";
    bool print = false;
    std::size_t number = 100000;
    std::size_t batch_size = 1;
    long payload_size = -1;

    for (int i = 1; i < argc; ++i) {
        std::string_view arg = argv[i];
        if ((arg == "-e" || arg == "--connect") && i + 1 < argc) {
            endpoint = argv[++i];
        } else if (arg == "-t" || arg == "--print") {
            print = true;
        } else if ((arg == "-n" || arg == "--number") && i + 1 < argc) {
            number = std::strtoul(argv[++i], nullptr, 10);
        } else if (arg == "--batch" && i + 1 < argc) {
            batch_size = std::strtoul(argv[++i], nullptr, 10);
        } else if (!arg.empty() && arg.front() != '-') {
            payload_size = std::atol(argv[i]);
        }
    }

    if (payload_size < 0) {
        std::fprintf(stderr, "usage: %s [-e endpoint] [-t] [-n N] [--batch B] <payload_size>\n",
                     argv[0]);
        return 1;
    }
    if (number == 0) number = 1;
    if (batch_size == 0) batch_size = 1;

    // Payload of `payload_size` bytes: data[i] = i % 10 (matches the reference).
    std::vector<std::byte> data(static_cast<std::size_t>(payload_size));
    for (std::size_t i = 0; i < data.size(); ++i) data[i] = static_cast<std::byte>(i % 10);
    std::span<const std::byte> const payload{data};

    auto session = zenoh::Session::open(endpoint);
    if (!session) {
        std::fprintf(stderr, "open(%s) failed: %s\n", endpoint.c_str(),
                     error_name(session.error()));
        return 1;
    }

    static constexpr std::string_view key = "test/thr";
    std::printf("Publishing %ld byte payloads to '%.*s' (batch=%zu). Press CTRL-C to quit...\n",
                payload_size, static_cast<int>(key.size()), key.data(), batch_size);

    std::size_t count = 0;
    auto start = std::chrono::steady_clock::now();

    auto tick = [&]() -> bool {
        if (!print) return true;
        if (count < number) {
            ++count;
            return true;
        }
        auto const now = std::chrono::steady_clock::now();
        double const secs = std::chrono::duration<double>(now - start).count();
        std::printf("%f msg/s\n", static_cast<double>(count) / secs);
        count = 0;
        start = std::chrono::steady_clock::now();
        return true;
    };

    if (batch_size == 1) {
        for (;;) {
            if (auto r = session->put(key, payload); !r) {
                std::fprintf(stderr, "put failed: %s\n", error_name(r.error()));
                return 1;
            }
            tick();
        }
    } else {
        for (;;) {
            auto batch = session->batch();
            for (std::size_t i = 0; i < batch_size; ++i) {
                if (auto r = batch.put(key, payload); !r) {
                    std::fprintf(stderr, "batch.put failed: %s\n", error_name(r.error()));
                    return 1;
                }
                tick();
            }
            if (auto r = batch.flush(); !r) {
                std::fprintf(stderr, "batch.flush failed: %s\n", error_name(r.error()));
                return 1;
            }
        }
    }

    return 0;
}

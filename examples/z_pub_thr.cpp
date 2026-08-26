// z_pub_thr: publish to "test/thr" as fast as possible and (optionally) report the
// achieved throughput. The C++23 equivalent of zenoh-rust's z_pub_thr.rs.
//
// Usage: z_pub_thr [OPTIONS] <PAYLOAD_SIZE>   (run with --help for the full list)
//   <PAYLOAD_SIZE>         payload length in bytes (required, positional)
//   -t, --print            print throughput statistics
//   -n, --number <NUMBER>  messages per measurement window [default: 100000]
//       --batch <BATCH>    coalesce BATCH puts per flush via the batching API
//                          (default 1) — an extension, not a reference option
//
// Our `put` blocks until the message is handed to the transport, matching the
// reference's CongestionControl::Block. The reference's --express and -p/--priority
// select QoS the runtime does not expose, so both are accepted and reported as having
// no effect.
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

#include "zexample.hpp"

namespace {

auto usage(const char* argv0) -> void {
    std::printf("Usage: %s [OPTIONS] <PAYLOAD_SIZE>\n\n"
                "Arguments:\n"
                "  <PAYLOAD_SIZE>\n"
                "          Size of the payload to publish, in bytes\n"
                "\n"
                "Options:\n"
                "  -t, --print\n"
                "          Print the statistics\n"
                "  -n, --number <NUMBER>\n"
                "          Number of messages in each throughput measurement"
                " [default: 100000]\n"
                "      --batch <BATCH>\n"
                "          Coalesce BATCH puts per flush (an extension of this port,\n"
                "          with no reference counterpart) [default: 1]\n"
                "      --express\n"
                "          (no effect: express QoS is not implemented)\n"
                "  -p, --priority <PRIORITY>\n"
                "          (no effect: priority QoS is not implemented)\n",
                argv0);
    zexample::print_common_help();
}

} // namespace

auto main(int argc, char** argv) -> int {
    if (zexample::wants_help(argc, argv)) {
        usage(argv[0]);
        return 0;
    }
    // Line-buffer stdout so throughput lines flush promptly even when redirected.
    std::setvbuf(stdout, nullptr, _IOLBF, 0);

    zexample::CommonArgs common;
    bool print = false;
    std::size_t number = 100000;
    std::size_t batch_size = 1;
    long payload_size = -1;

    for (int i = 1; i < argc; ++i) {
        std::string_view arg = argv[i];
        if (arg == "-t" || arg == "--print") {
            print = true;
        } else if (arg == "-n" || arg == "--number") {
            const char* value = zexample::option_value(argc, argv, i);
            if (value == nullptr) return 1;
            number = std::strtoul(value, nullptr, 10);
        } else if (arg == "--batch") {
            const char* value = zexample::option_value(argc, argv, i);
            if (value == nullptr) return 1;
            batch_size = std::strtoul(value, nullptr, 10);
        } else if (arg == "--express") {
            zexample::no_effect(arg, "express QoS is not implemented");
        } else if (arg == "-p" || arg == "--priority") {
            if (zexample::option_value(argc, argv, i) == nullptr) return 1;
            zexample::no_effect(arg, "priority QoS is not implemented");
        } else if (!arg.empty() && arg.front() != '-') {
            payload_size = std::atol(argv[i]);
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
                     zexample::error_name(session.error()));
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
                std::fprintf(stderr, "put failed: %s\n", zexample::error_name(r.error()));
                return 1;
            }
            tick();
        }
    } else {
        for (;;) {
            auto batch = session->batch();
            for (std::size_t i = 0; i < batch_size; ++i) {
                if (auto r = batch.put(key, payload); !r) {
                    std::fprintf(stderr, "batch.put failed: %s\n", zexample::error_name(r.error()));
                    return 1;
                }
                tick();
            }
            if (auto r = batch.flush(); !r) {
                std::fprintf(stderr, "batch.flush failed: %s\n", zexample::error_name(r.error()));
                return 1;
            }
        }
    }

    return 0;
}

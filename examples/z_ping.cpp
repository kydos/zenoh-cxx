// z_ping: round-trip latency probe. Publishes a fixed-size payload on test/ping and
// waits for the echo on test/pong, timing each round trip. The C++23 equivalent of
// zenoh-rust's z_ping.rs; run it against z_pong (this repo's or the reference's).
//
// Usage: z_ping [OPTIONS] <PAYLOAD_SIZE>   (run with --help for the full list)
//   <PAYLOAD_SIZE>           payload length in bytes (required, positional)
//   -n, --samples <SAMPLES>  number of round-trips to measure [default: 100]
//   -w, --warmup <WARMUP>    warmup duration in seconds, float [default: 1]
//
// The reference publishes with CongestionControl::Block and express QoS (its
// --no-express flag); the runtime's blocking `put` has no QoS knobs, so that flag is
// not modeled. `put` blocking until the payload is written is itself the equivalent
// of the reference's blocking congestion control.
//
// Run:
//   zenohd -l tcp/127.0.0.1:7447 &     (or: ./build/clang/zenohb -l tcp/127.0.0.1:7447 &)
//   ./build/clang/examples/z_pong &    (or the reference z_pong, from ../zenoh-rust)
//   ./build/clang/examples/z_ping 64

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <optional>
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
                "  -w, --warmup <WARMUP>\n"
                "          The number of seconds to warm up (float) [default: 1]\n"
                "  -n, --samples <SAMPLES>\n"
                "          The number of round-trips to measure [default: 100]\n"
                "      --no-express\n"
                "          (no effect: express QoS is not implemented)\n",
                argv0);
    zexample::print_common_help();
}

} // namespace

auto main(int argc, char** argv) -> int {
    if (zexample::wants_help(argc, argv)) {
        usage(argv[0]);
        return 0;
    }
    std::setvbuf(stdout, nullptr, _IOLBF, 0);

    zexample::CommonArgs common;
    std::size_t samples = 100;
    double warmup_s = 1.0;
    std::optional<std::size_t> payload_size;

    for (int i = 1; i < argc; ++i) {
        std::string_view arg = argv[i];
        if (arg == "-n" || arg == "--samples") {
            const char* value = zexample::option_value(argc, argv, i);
            if (value == nullptr) return 1;
            samples = std::strtoul(value, nullptr, 10);
        } else if (arg == "-w" || arg == "--warmup") {
            const char* value = zexample::option_value(argc, argv, i);
            if (value == nullptr) return 1;
            warmup_s = std::strtod(value, nullptr);
        } else if (arg == "--no-express") {
            zexample::no_effect(arg, "express QoS is not implemented");
        } else if (!arg.starts_with('-')) {
            payload_size = std::strtoul(argv[i], nullptr, 10);
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
    if (!payload_size) {
        std::fprintf(stderr, "usage: %s [options] <payload_size>\n", argv[0]);
        return 1;
    }

    std::printf("Opening session...\n");
    auto session = zenoh::Session::open(endpoint);
    if (!session) {
        std::fprintf(stderr, "open(%s) failed: %s\n", endpoint.c_str(),
                     zexample::error_name(session.error()));
        return 1;
    }

    // Subscribe to the echo *before* publishing anything, so no round trip is missed.
    auto sub = session->declare_subscriber("test/pong");
    if (!sub) {
        std::fprintf(stderr, "declare_subscriber('test/pong') failed: %s\n",
                     zexample::error_name(sub.error()));
        return 1;
    }

    std::vector<std::byte> data(*payload_size);
    for (std::size_t i = 0; i < data.size(); ++i) {
        data[i] = static_cast<std::byte>(i % 10);
    }

    // One round trip: publish on test/ping, block until the echo lands on test/pong.
    const auto round_trip = [&]() -> bool {
        if (auto r = session->put("test/ping", data); !r) {
            std::fprintf(stderr, "put('test/ping') failed: %s\n", zexample::error_name(r.error()));
            return false;
        }
        if (auto s = sub->recv(); !s) {
            std::fprintf(stderr, "recv failed: %s\n", zexample::error_name(s.error()));
            return false;
        }
        return true;
    };

    using Clock = std::chrono::steady_clock;

    std::printf("Warming up for %gs...\n", warmup_s);
    const auto warmup = std::chrono::duration<double>(warmup_s);
    for (const auto start = Clock::now(); Clock::now() - start < warmup;) {
        if (!round_trip()) return 1;
    }

    std::vector<std::uint64_t> rtts;
    rtts.reserve(samples);
    for (std::size_t i = 0; i < samples; ++i) {
        const auto write_time = Clock::now();
        if (!round_trip()) return 1;
        const auto rtt =
            std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - write_time);
        rtts.push_back(static_cast<std::uint64_t>(rtt.count()));
    }

    for (std::size_t i = 0; i < rtts.size(); ++i) {
        std::printf("%zu bytes: seq=%zu rtt=%lluµs lat=%lluµs\n", *payload_size, i,
                    static_cast<unsigned long long>(rtts[i]),
                    static_cast<unsigned long long>(rtts[i] / 2));
    }

    session->close();
    return 0;
}

// z_pong: the echo half of the latency probe. Subscribes to test/ping and republishes
// every payload it receives on test/pong. The C++23 equivalent of zenoh-rust's
// z_pong.rs; run it against z_ping (this repo's or the reference's).
//
// Usage: z_pong [OPTIONS]       (run with --help for the full option list)
//
// The reference echoes from a subscriber callback and parks the main thread; this
// uses the equivalent blocking recv loop, which keeps the echo on the measured path
// with no handler indirection. Its --no-express flag models QoS the runtime's `put`
// does not expose, so it is not modeled here.
//
// Run:
//   zenohd -l tcp/127.0.0.1:7447 &     (or: ./build/clang/zenohb -l tcp/127.0.0.1:7447 &)
//   ./build/clang/examples/z_pong &
//   ./build/clang/examples/z_ping 64   (or the reference z_ping, from ../zenoh-rust)

#include <cstdio>
#include <cstdlib>
#include <string>
#include <string_view>

import zenoh;

#include "zexample.hpp"

namespace {

auto usage(const char* argv0) -> void {
    std::printf("Usage: %s [OPTIONS]\n\n"
                "Options:\n"
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

    for (int i = 1; i < argc; ++i) {
        std::string_view arg = argv[i];
        if (arg == "--no-express") {
            zexample::no_effect(arg, "express QoS is not implemented");
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

    auto sub = session->declare_subscriber("test/ping");
    if (!sub) {
        std::fprintf(stderr, "declare_subscriber('test/ping') failed: %s\n",
                     zexample::error_name(sub.error()));
        return 1;
    }

    std::printf("Echoing test/ping -> test/pong. Press CTRL-C to quit...\n");
    for (;;) {
        auto sample = sub->recv();
        if (!sample) {
            if (sample.error() == zenoh::ZError::connection_closed) break;
            std::fprintf(stderr, "recv failed: %s\n", zexample::error_name(sample.error()));
            return 1;
        }
        if (auto r = session->put("test/pong", sample->payload()); !r) {
            std::fprintf(stderr, "put('test/pong') failed: %s\n", zexample::error_name(r.error()));
            return 1;
        }
    }

    return 0;
}

// z_sub: connect to a Zenoh router, declare a subscriber, and print every received
// sample. The C++23 equivalent of zenoh-rust's z_sub.rs (the blocking recv loop).
//
// Usage: z_sub [OPTIONS]        (run with --help for the full option list)
//   -k, --key <KEY>  key expression to subscribe to [default: demo/example/**]
//
// Verify with the reference publisher:
//   zenohd -l tcp/127.0.0.1:7447 &
//   z_pub -e tcp/127.0.0.1:7447 -k demo/example/test           # from ../zenoh-rust
//   z_put -e tcp/127.0.0.1:7447 -k demo/example/test -p hello

#include <cstddef>
#include <cstdio>
#include <string>
#include <string_view>

import zenoh;

#include "zexample.hpp"

namespace {

auto usage(const char* argv0) -> void {
    std::printf("Usage: %s [OPTIONS]\n\n"
                "Options:\n"
                "  -k, --key <KEY>\n"
                "          The key expression to subscribe to [default: demo/example/**]\n",
                argv0);
    zexample::print_common_help();
}

auto kind_name(zenoh::SampleKind k) -> const char* {
    return k == zenoh::SampleKind::put ? "PUT" : "DELETE";
}

} // namespace

auto main(int argc, char** argv) -> int {
    if (zexample::wants_help(argc, argv)) {
        usage(argv[0]);
        return 0;
    }
    std::setvbuf(stdout, nullptr, _IOLBF, 0);

    zexample::CommonArgs common;
    std::string key = "demo/example/**";

    for (int i = 1; i < argc; ++i) {
        std::string_view arg = argv[i];
        if (arg == "-k" || arg == "--key") {
            const char* value = zexample::option_value(argc, argv, i);
            if (value == nullptr) return 1;
            key = value;
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

    std::printf("Declaring Subscriber on '%s'...\n", key.c_str());
    auto sub = session->declare_subscriber(key);
    if (!sub) {
        std::fprintf(stderr, "declare_subscriber('%s') failed: %s\n", key.c_str(),
                     zexample::error_name(sub.error()));
        return 1;
    }

    std::printf("Press CTRL-C to quit...\n");
    for (;;) {
        auto sample = sub->recv();
        if (!sample) {
            if (sample.error() == zenoh::ZError::connection_closed) break;
            std::fprintf(stderr, "recv failed: %s\n", zexample::error_name(sample.error()));
            return 1;
        }
        auto payload = sample->payload();
        std::printf(">> [Subscriber] Received %s ('%.*s': '%.*s')\n", kind_name(sample->kind()),
                    static_cast<int>(sample->key_expr().size()), sample->key_expr().data(),
                    static_cast<int>(payload.size()),
                    reinterpret_cast<const char*>(payload.data()));
    }

    return 0;
}

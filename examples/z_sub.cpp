// z_sub: connect to a Zenoh router, declare a subscriber, and print every received
// sample. The C++23 equivalent of zenoh-rust's z_sub.rs (the blocking recv loop).
//
// Usage: z_sub [options]
//   -e, --connect E  router endpoint (default tcp/127.0.0.1:7447)
//   -k, --key K      key expression to subscribe to (default demo/example/**)
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
    case zenoh::ZError::already_queryable:
        return "already_queryable";
    case zenoh::ZError::query_timeout:
        return "query_timeout";
    }
    return "unknown";
}

auto kind_name(zenoh::SampleKind k) -> const char* {
    return k == zenoh::SampleKind::put ? "PUT" : "DELETE";
}

} // namespace

auto main(int argc, char** argv) -> int {
    std::setvbuf(stdout, nullptr, _IOLBF, 0);

    std::string endpoint = "tcp/127.0.0.1:7447";
    std::string key = "demo/example/**";

    for (int i = 1; i < argc; ++i) {
        std::string_view arg = argv[i];
        if ((arg == "-e" || arg == "--connect") && i + 1 < argc) {
            endpoint = argv[++i];
        } else if ((arg == "-k" || arg == "--key") && i + 1 < argc) {
            key = argv[++i];
        }
    }

    std::printf("Opening session...\n");
    auto session = zenoh::Session::open(endpoint);
    if (!session) {
        std::fprintf(stderr, "open(%s) failed: %s\n", endpoint.c_str(),
                     error_name(session.error()));
        return 1;
    }

    std::printf("Declaring Subscriber on '%s'...\n", key.c_str());
    auto sub = session->declare_subscriber(key);
    if (!sub) {
        std::fprintf(stderr, "declare_subscriber('%s') failed: %s\n", key.c_str(),
                     error_name(sub.error()));
        return 1;
    }

    std::printf("Press CTRL-C to quit...\n");
    for (;;) {
        auto sample = sub->recv();
        if (!sample) {
            if (sample.error() == zenoh::ZError::connection_closed) break;
            std::fprintf(stderr, "recv failed: %s\n", error_name(sample.error()));
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

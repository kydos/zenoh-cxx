// z_queryable: connect to a Zenoh router, declare a queryable, and answer every
// incoming query with a fixed payload. The C++23 equivalent of zenoh-rust's
// z_queryable.rs (the blocking query-pull loop).
//
// Usage: z_queryable [options]
//   -e, --connect E  router endpoint (default tcp/127.0.0.1:7447)
//   -k, --key K      key expression to answer queries on
//                    (default demo/example/zenoh-cpp-queryable)
//   -p, --payload P  payload to reply with (default "Queryable from C++!")
//   --complete       declare the queryable as complete w.r.t. the key expression
//
// Verify against the reference getter:
//   zenohd -l tcp/127.0.0.1:7447 &                    # or this project's zenohb
//   ./build/clang/examples/z_queryable &
//   z_get -s 'demo/example/**'                        # from ../zenoh-rust

#include <cstdio>
#include <cstdlib>
#include <string>
#include <string_view>

import zenoh;

#include "zexample.hpp"

auto main(int argc, char** argv) -> int {
    std::setvbuf(stdout, nullptr, _IOLBF, 0);

    std::string endpoint = "tcp/127.0.0.1:7447";
    std::string key = "demo/example/zenoh-cpp-queryable";
    std::string payload = "Queryable from C++!";
    zenoh::QueryableOptions opts{};

    for (int i = 1; i < argc; ++i) {
        std::string_view arg = argv[i];
        if ((arg == "-e" || arg == "--connect") && i + 1 < argc) {
            endpoint = argv[++i];
        } else if ((arg == "-k" || arg == "--key") && i + 1 < argc) {
            key = argv[++i];
        } else if ((arg == "-p" || arg == "--payload") && i + 1 < argc) {
            payload = argv[++i];
        } else if (arg == "--complete") {
            opts.complete = true;
        }
    }

    std::printf("Opening session...\n");
    auto session = zenoh::Session::open(endpoint);
    if (!session) {
        std::fprintf(stderr, "open(%s) failed: %s\n", endpoint.c_str(),
                     zexample::error_name(session.error()));
        return 1;
    }

    std::printf("Declaring Queryable on '%s'...\n", key.c_str());
    auto queryable = session->declare_queryable(key, opts);
    if (!queryable) {
        std::fprintf(stderr, "declare_queryable('%s') failed: %s\n", key.c_str(),
                     zexample::error_name(queryable.error()));
        return 1;
    }

    std::printf("Press CTRL-C to quit...\n");
    for (;;) {
        auto query = queryable->recv();
        if (!query) {
            if (query.error() == zenoh::ZError::connection_closed) break;
            std::fprintf(stderr, "recv failed: %s\n", zexample::error_name(query.error()));
            return 1;
        }

        // Reassemble the selector the way the reference prints it: "<key>?<params>".
        std::string selector{query->key_expr()};
        if (!query->parameters().empty()) {
            selector += '?';
            selector += query->parameters();
        }
        if (query->payload().empty()) {
            std::printf(">> [Queryable ] Received Query '%s'\n", selector.c_str());
        } else {
            auto value = zexample::as_str(query->payload());
            std::printf(">> [Queryable ] Received Query '%s' with payload '%.*s'\n",
                        selector.c_str(), static_cast<int>(value.size()), value.data());
        }

        std::printf(">> [Queryable ] Responding ('%s': '%s')\n", key.c_str(), payload.c_str());
        if (auto r = query->reply(key, zexample::as_bytes(payload)); !r) {
            std::printf(">> [Queryable ] Error sending reply: %s\n",
                        zexample::error_name(r.error()));
        }
        // `query` goes out of scope here, which sends the query's ResponseFinal.
    }

    return 0;
}

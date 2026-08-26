// z_queryable: connect to a Zenoh router, declare a queryable, and answer every
// incoming query with a fixed payload. The C++23 equivalent of zenoh-rust's
// z_queryable.rs (the blocking query-pull loop).
//
// Usage: z_queryable [OPTIONS]  (run with --help for the full option list)
//   -k, --key <KEY>          key expression to answer queries on
//                            [default: demo/example/zenoh-cpp-queryable]
//   -p, --payload <PAYLOAD>  payload to reply with [default: "Queryable from C++!"]
//       --complete           declare the queryable complete w.r.t. the key expression
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

namespace {

auto usage(const char* argv0) -> void {
    std::printf("Usage: %s [OPTIONS]\n\n"
                "Options:\n"
                "  -k, --key <KEY>\n"
                "          The key expression matching queries to reply to\n"
                "          [default: demo/example/zenoh-cpp-queryable]\n"
                "  -p, --payload <PAYLOAD>\n"
                "          The payload to reply to queries"
                " [default: \"Queryable from C++!\"]\n"
                "      --complete\n"
                "          Declare the queryable as complete w.r.t. the key expression\n",
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
    std::string key = "demo/example/zenoh-cpp-queryable";
    std::string payload = "Queryable from C++!";
    zenoh::QueryableOptions opts{};

    for (int i = 1; i < argc; ++i) {
        std::string_view arg = argv[i];
        if (arg == "-k" || arg == "--key") {
            const char* value = zexample::option_value(argc, argv, i);
            if (value == nullptr) return 1;
            key = value;
        } else if (arg == "-p" || arg == "--payload") {
            const char* value = zexample::option_value(argc, argv, i);
            if (value == nullptr) return 1;
            payload = value;
        } else if (arg == "--complete") {
            opts.complete = true;
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

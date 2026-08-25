// z_get: connect to a Zenoh router, send one query, and print every reply until the
// query completes. The C++23 equivalent of zenoh-rust's z_get.rs (the blocking
// reply-pull loop).
//
// Usage: z_get [options]
//   -e, --connect E   router endpoint (default tcp/127.0.0.1:7447)
//   -s, --selector S  selector to query (default demo/example/**), "<key>?<params>"
//   -t, --target T    BEST_MATCHING | ALL | ALL_COMPLETE (default BEST_MATCHING)
//   -o, --timeout MS  query timeout in milliseconds (default 10000)
//
// The reference splits a Selector into key expression + parameters; the runtime's
// `get` takes the two separately, so the selector is split at the first '?' here.
// The reference also accepts `-p/--payload` to attach a payload to the query itself:
// the runtime's `get` carries a key expression and parameters only (a queryable can
// *read* a query payload via `IncomingQuery::payload()`, but nothing here can send
// one), so that option is not modeled.
//
// Verify against the reference queryable:
//   zenohd -l tcp/127.0.0.1:7447 &                       # or this project's zenohb
//   z_queryable -k demo/example/zenoh-rs-queryable &     # from ../zenoh-rust
//   ./build/clang/examples/z_get -s 'demo/example/**'

#include <cstdio>
#include <cstdlib>
#include <string>
#include <string_view>

import zenoh;

#include "zexample.hpp"

namespace {

auto parse_target(std::string_view s, zenoh::GetTarget& out) -> bool {
    if (s == "BEST_MATCHING") {
        out = zenoh::GetTarget::best_matching;
    } else if (s == "ALL") {
        out = zenoh::GetTarget::all;
    } else if (s == "ALL_COMPLETE") {
        out = zenoh::GetTarget::all_complete;
    } else {
        return false;
    }
    return true;
}

} // namespace

auto main(int argc, char** argv) -> int {
    std::setvbuf(stdout, nullptr, _IOLBF, 0);

    std::string endpoint = "tcp/127.0.0.1:7447";
    std::string selector = "demo/example/**";
    zenoh::GetOptions opts{};
    opts.timeout_ms = 10000;

    for (int i = 1; i < argc; ++i) {
        std::string_view arg = argv[i];
        if ((arg == "-e" || arg == "--connect") && i + 1 < argc) {
            endpoint = argv[++i];
        } else if ((arg == "-s" || arg == "--selector") && i + 1 < argc) {
            selector = argv[++i];
        } else if ((arg == "-t" || arg == "--target") && i + 1 < argc) {
            if (!parse_target(argv[++i], opts.target)) {
                std::fprintf(stderr, "unknown target '%s'\n", argv[i]);
                return 1;
            }
        } else if ((arg == "-o" || arg == "--timeout") && i + 1 < argc) {
            opts.timeout_ms = static_cast<std::uint32_t>(std::strtoul(argv[++i], nullptr, 10));
        }
    }

    // Split "<key expr>?<parameters>" the way the reference's Selector does.
    std::string key = selector;
    std::string parameters;
    if (auto q = selector.find('?'); q != std::string::npos) {
        key = selector.substr(0, q);
        parameters = selector.substr(q + 1);
    }

    std::printf("Opening session...\n");
    auto session = zenoh::Session::open(endpoint);
    if (!session) {
        std::fprintf(stderr, "open(%s) failed: %s\n", endpoint.c_str(),
                     zexample::error_name(session.error()));
        return 1;
    }

    std::printf("Sending Query '%s'...\n", selector.c_str());
    auto getter = session->get(key, parameters, opts);
    if (!getter) {
        std::fprintf(stderr, "get('%s') failed: %s\n", selector.c_str(),
                     zexample::error_name(getter.error()));
        return 1;
    }

    for (;;) {
        auto reply = getter->recv();
        if (!reply) {
            if (reply.error() == zenoh::ZError::query_timeout) {
                std::printf(">> Query timed out\n");
                break;
            }
            std::fprintf(stderr, "recv failed: %s\n", zexample::error_name(reply.error()));
            return 1;
        }
        if (!*reply) break; // the query completed: no more replies are coming
        if ((*reply)->is_ok()) {
            const auto& sample = (*reply)->sample();
            auto value = zexample::as_str(sample.payload());
            std::printf(">> Received ('%.*s': '%.*s')\n",
                        static_cast<int>(sample.key_expr().size()), sample.key_expr().data(),
                        static_cast<int>(value.size()), value.data());
        } else {
            auto value = zexample::as_str((*reply)->error_payload());
            std::printf(">> Received (ERROR: '%.*s')\n", static_cast<int>(value.size()),
                        value.data());
        }
    }

    session->close();
    return 0;
}

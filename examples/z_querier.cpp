// z_querier: connect to a Zenoh router and issue the same query once a second,
// printing every reply. The C++23 equivalent of zenoh-rust's z_querier.rs.
//
// Usage: z_querier [OPTIONS]    (run with --help for the full option list)
//   -s, --selector <SELECTOR>  selector to query, "<key>?<params>"
//                              [default: demo/example/**]
//   -t, --target <TARGET>      BEST_MATCHING | ALL | ALL_COMPLETE
//   -o, --timeout <TIMEOUT>    per-query timeout in milliseconds [default: 10000]
//
// Two deliberate differences from the reference, both runtime gaps rather than
// choices: there is no `declare_querier` (a querier declared once and reused, with a
// matching-listener callback), so each iteration issues a plain `get()`; and a query
// cannot carry a payload, so the iteration counter the reference sends as the query
// payload only appears in this side's printed output.
//
// Verify against the reference queryable:
//   zenohd -l tcp/127.0.0.1:7447 &                       # or this project's zenohb
//   z_queryable -k demo/example/zenoh-rs-queryable &     # from ../zenoh-rust
//   ./build/clang/examples/z_querier -s 'demo/example/**'

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <string_view>
#include <thread>

import zenoh;

#include "zexample.hpp"

namespace {

auto usage(const char* argv0) -> void {
    std::printf("Usage: %s [OPTIONS]\n\n"
                "Options:\n"
                "  -s, --selector <SELECTOR>\n"
                "          The selection of resources to query [default: demo/example/**]\n"
                "  -p, --payload <PAYLOAD>\n"
                "          Payload to put in the query (no effect: `get` sends a key\n"
                "          expression and parameters only)\n"
                "  -t, --target <TARGET>\n"
                "          The target queryables of the query [default: BEST_MATCHING]\n"
                "          [possible values: BEST_MATCHING, ALL, ALL_COMPLETE]\n"
                "  -o, --timeout <TIMEOUT>\n"
                "          The query timeout in milliseconds [default: 10000]\n"
                "      --add-matching-listener\n"
                "          (no effect: matching listeners are not implemented)\n",
                argv0);
    zexample::print_common_help();
}

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
    if (zexample::wants_help(argc, argv)) {
        usage(argv[0]);
        return 0;
    }
    std::setvbuf(stdout, nullptr, _IOLBF, 0);

    zexample::CommonArgs common;
    std::string selector = "demo/example/**";
    zenoh::GetOptions opts{};
    opts.timeout_ms = 10000;

    for (int i = 1; i < argc; ++i) {
        std::string_view arg = argv[i];
        if (arg == "-s" || arg == "--selector") {
            const char* value = zexample::option_value(argc, argv, i);
            if (value == nullptr) return 1;
            selector = value;
        } else if (arg == "-p" || arg == "--payload") {
            if (zexample::option_value(argc, argv, i) == nullptr) return 1;
            zexample::no_effect(arg, "a query carries a key expression and parameters only");
        } else if (arg == "-t" || arg == "--target") {
            const char* value = zexample::option_value(argc, argv, i);
            if (value == nullptr) return 1;
            if (!parse_target(value, opts.target)) {
                std::fprintf(stderr, "error: unknown target '%s'\n", value);
                return 1;
            }
        } else if (arg == "-o" || arg == "--timeout") {
            const char* value = zexample::option_value(argc, argv, i);
            if (value == nullptr) return 1;
            opts.timeout_ms = static_cast<std::uint32_t>(std::strtoul(value, nullptr, 10));
        } else if (arg == "--add-matching-listener") {
            zexample::no_effect(arg, "matching listeners are not implemented");
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

    std::printf("Press CTRL-C to quit...\n");
    for (std::uint32_t idx = 0;; ++idx) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        std::printf("[%4u] Querying '%s'...\n", idx, selector.c_str());

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
            if (!*reply) break; // this query completed
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
    }

    return 0;
}

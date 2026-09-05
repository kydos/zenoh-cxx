// z_computation: connect to a Zenoh router, declare a computation at one concrete
// key, and run it for every incoming eval (docs/RUNTIME.md "Evaluation").
//
// Unlike every other example here this one has no zenoh-rust counterpart -- the
// Evaluation abstraction is specific to this implementation -- so its CLI mirrors
// z_queryable's, which is the closest thing it has to a sibling, rather than a
// reference binary's. The shared connection options are the usual ones.
//
// Usage: z_computation [OPTIONS]  (run with --help for the full option list)
//   -k, --key <KEY>          concrete key to register the computation at
//                            [default: demo/example/zenoh-cpp-computation]
//   -p, --payload <PAYLOAD>  payload to reply with [default: "Computation from C++!"]
//
// Note the key must be concrete: a computation is a computation at one key, so
// `demo/example/*` is rejected. The wildcard belongs on the other side, in z_eval.
//
// Verify against this repo's evaluator:
//   zenohb -l tcp/127.0.0.1:7447 &                    # or a reference zenohd
//   ./build/clang/examples/z_computation &
//   ./build/clang/examples/z_eval -k 'demo/example/**'

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
                "          The concrete key to register the computation at\n"
                "          [default: demo/example/zenoh-cpp-computation]\n"
                "  -p, --payload <PAYLOAD>\n"
                "          The payload to reply with"
                " [default: \"Computation from C++!\"]\n",
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
    std::string key = "demo/example/zenoh-cpp-computation";
    std::string payload = "Computation from C++!";

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

    std::printf("Declaring Computation at '%s'...\n", key.c_str());
    auto computation = session->declare_computation(key);
    if (!computation) {
        std::fprintf(stderr, "declare_computation('%s') failed: %s\n", key.c_str(),
                     zexample::error_name(computation.error()));
        if (computation.error() == zenoh::ZError::invalid_key_expr) {
            std::fprintf(stderr, "note: a computation must be registered at a concrete key; "
                                 "wild key expressions are not allowed\n");
        }
        return 1;
    }

    std::printf("Press CTRL-C to quit...\n");
    for (;;) {
        auto eval = computation->recv();
        if (!eval) {
            if (eval.error() == zenoh::ZError::connection_closed) break;
            std::fprintf(stderr, "recv failed: %s\n", zexample::error_name(eval.error()));
            return 1;
        }

        // The evaluator's key expression is what was asked for; this computation's own
        // key is what is answering. They differ whenever the evaluation was wild.
        auto argument = zexample::as_str(eval->argument());
        std::printf(">> [Computation] Received Eval '%.*s' (argument: '%.*s')\n",
                    static_cast<int>(eval->key_expr().size()), eval->key_expr().data(),
                    static_cast<int>(argument.size()), argument.data());

        std::printf(">> [Computation] Responding ('%.*s': '%s')\n",
                    static_cast<int>(eval->computation_key().size()),
                    eval->computation_key().data(), payload.c_str());
        // No key is passed: the reply is keyed by this computation's own key.
        if (auto r = eval->reply(zexample::as_bytes(payload)); !r) {
            std::printf(">> [Computation] Error sending reply: %s\n",
                        zexample::error_name(r.error()));
        }
        // `eval` goes out of scope here, which releases its share of the request --
        // the ResponseFinal follows once every computation this eval reached is done.
    }

    return 0;
}

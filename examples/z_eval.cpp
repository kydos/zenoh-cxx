// z_eval: connect to a Zenoh router, evaluate one argument on every computation
// matching a key expression, and print every reply until the evaluation completes
// (docs/RUNTIME.md "Evaluation").
//
// Like z_computation this has no zenoh-rust counterpart, so its CLI mirrors z_get's,
// the closest sibling it has here. Two differences follow from the semantics rather
// than from taste:
//
//   * there is no `-t/--target`: an eval always reaches *every* matching computation
//     registration, which is the whole point of the abstraction;
//   * the payload is the computation's argument, not optional decoration, so
//     `-p/--payload` has a default rather than being absent by default.
//
// Usage: z_eval [OPTIONS]       (run with --help for the full option list)
//   -k, --key <KEY>          key expression to evaluate [default: demo/example/**]
//   -p, --payload <PAYLOAD>  argument to evaluate [default: "Argument from C++!"]
//   -o, --timeout <TIMEOUT>  reply-collection timeout in milliseconds [default: 10000]
//   -d, --declare            evaluate through a declared Evaluator
//
// `-d/--declare` is what z_querier is to z_get: the same exchange, but through a
// handle that binds its key expression to a numeric id once (so the request carries
// the id rather than the text). The replies are identical either way.
//
// Verify against this repo's computation:
//   zenohb -l tcp/127.0.0.1:7447 &                    # or a reference zenohd
//   ./build/clang/examples/z_computation &
//   ./build/clang/examples/z_eval -k 'demo/example/**'

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

import zenoh;

#include "zexample.hpp"

namespace {

auto usage(const char* argv0) -> void {
    std::printf("Usage: %s [OPTIONS]\n\n"
                "Options:\n"
                "  -k, --key <KEY>\n"
                "          The key expression matching the computations to evaluate\n"
                "          [default: demo/example/**]\n"
                "  -p, --payload <PAYLOAD>\n"
                "          The argument to evaluate the computations on\n"
                "          [default: \"Argument from C++!\"]\n"
                "  -o, --timeout <TIMEOUT>\n"
                "          The reply-collection timeout in milliseconds [default: 10000]\n"
                "  -d, --declare\n"
                "          Evaluate through a declared Evaluator (the key expression"
                " travels as its declared id)\n",
                argv0);
    zexample::print_common_help();
}

auto parse_timeout(std::string_view s, std::uint32_t& out) -> bool {
    std::uint32_t value = 0;
    for (char const c : s) {
        if (c < '0' || c > '9') return false;
        value = (value * 10) + static_cast<std::uint32_t>(c - '0');
    }
    if (s.empty()) return false;
    out = value;
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
    std::string key = "demo/example/**";
    std::string payload = "Argument from C++!";
    bool declared = false;
    zenoh::EvalOptions opts{};

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
        } else if (arg == "-o" || arg == "--timeout") {
            const char* value = zexample::option_value(argc, argv, i);
            if (value == nullptr) return 1;
            std::uint32_t timeout_ms = 0;
            if (!parse_timeout(value, timeout_ms)) {
                std::fprintf(stderr, "error: invalid timeout '%s'\n", value);
                return 1;
            }
            opts.timeout_ms = timeout_ms;
        } else if (arg == "-d" || arg == "--declare") {
            declared = true;
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

    // Declared or not, this is the same exchange; the declared form just sends the key
    // expression as an id. `evaluator` must outlive the evaluation it starts, so it is
    // declared here rather than inside the `if`.
    std::optional<zenoh::Evaluator> evaluator;
    if (declared) {
        std::printf("Declaring Evaluator on '%s'...\n", key.c_str());
        auto declaration = session->declare_evaluator(key, opts);
        if (!declaration) {
            std::fprintf(stderr, "declare_evaluator('%s') failed: %s\n", key.c_str(),
                         zexample::error_name(declaration.error()));
            return 1;
        }
        evaluator.emplace(std::move(*declaration));
    }

    std::printf("Sending Eval '%s' with argument '%s'...\n", key.c_str(), payload.c_str());
    auto evaluation = evaluator ? evaluator->eval(zexample::as_bytes(payload))
                                : session->eval(key, zexample::as_bytes(payload), opts);
    if (!evaluation) {
        std::fprintf(stderr, "eval('%s') failed: %s\n", key.c_str(),
                     zexample::error_name(evaluation.error()));
        return 1;
    }

    // Every matching computation registration replies under its own concrete key, and
    // nothing is consolidated -- so two computations at one key produce two replies.
    for (;;) {
        auto reply = evaluation->recv();
        if (!reply) {
            std::fprintf(stderr, "recv failed: %s\n", zexample::error_name(reply.error()));
            return 1;
        }
        if (!*reply) break; // every computation finished

        const zenoh::GetReply& r = **reply;
        if (r.is_ok()) {
            auto value = zexample::as_str(r.sample().payload());
            std::printf(">> Received ('%.*s': '%.*s')\n",
                        static_cast<int>(r.sample().key_expr().size()),
                        r.sample().key_expr().data(), static_cast<int>(value.size()), value.data());
        } else {
            auto value = zexample::as_str(r.error_payload());
            std::printf(">> Received (ERROR: '%.*s')\n", static_cast<int>(value.size()),
                        value.data());
        }
    }

    return 0;
}

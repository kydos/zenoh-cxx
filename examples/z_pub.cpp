// z_pub: connect to a Zenoh router and publish a value to a key expression once a
// second, forever. The C++23 equivalent of zenoh-rust's z_pub.rs.
//
// Usage: z_pub [OPTIONS]        (run with --help for the full option list)
//   -k, --key <KEY>          key expression [default: demo/example/zenoh-cpp-pub]
//   -p, --payload <PAYLOAD>  payload string [default: "Pub from C++!"]
//
// Each iteration publishes "[<idx>] <payload>" — matching the reference. The
// reference also sets a TEXT_PLAIN encoding, takes an optional attachment
// (-a/--attach) and can add a matching listener (--add-matching-listener); `put`
// carries neither encoding/attachment metadata nor subscription matching, so those
// two options are accepted and reported as having no effect.
//
// Verify with the reference subscriber:
//   zenohd -l tcp/127.0.0.1:7447 &
//   z_sub -e tcp/127.0.0.1:7447 -k 'demo/example/**'

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <span>
#include <string>
#include <string_view>
#include <thread>

import zenoh;

#include "zexample.hpp"

namespace {

auto usage(const char* argv0) -> void {
    std::printf("Usage: %s [OPTIONS]\n\n"
                "Options:\n"
                "  -k, --key <KEY>\n"
                "          The key expression to write to [default: demo/example/zenoh-cpp-pub]\n"
                "  -p, --payload <PAYLOAD>\n"
                "          The payload to write [default: \"Pub from C++!\"]\n"
                "  -a, --attach <ATTACH>\n"
                "          Attachment to add (no effect: `put` carries no attachment)\n"
                "      --add-matching-listener\n"
                "          (no effect: subscription matching is not implemented)\n",
                argv0);
    zexample::print_common_help();
}

} // namespace

auto main(int argc, char** argv) -> int {
    if (zexample::wants_help(argc, argv)) {
        usage(argv[0]);
        return 0;
    }
    // Line-buffer stdout so the per-second put lines appear promptly when redirected.
    std::setvbuf(stdout, nullptr, _IOLBF, 0);

    zexample::CommonArgs common;
    std::string key = "demo/example/zenoh-cpp-pub";
    std::string payload = "Pub from C++!";

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
        } else if (arg == "-a" || arg == "--attach") {
            if (zexample::option_value(argc, argv, i) == nullptr) return 1;
            zexample::no_effect(arg, "`put` carries no attachment");
        } else if (arg == "--add-matching-listener") {
            zexample::no_effect(arg, "subscription matching is not implemented");
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

    std::printf("Declaring Publisher on '%s'...\n", key.c_str());
    std::printf("Press CTRL-C to quit...\n");
    for (std::uint32_t idx = 0;; ++idx) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        // The reference formats the index right-aligned in 4 columns: "[   0] <payload>".
        std::array<char, 16> idx_buf{};
        std::snprintf(idx_buf.data(), idx_buf.size(), "[%4u] ", idx);
        std::string const buf = std::string(idx_buf.data()) + payload;
        std::printf("Putting Data ('%s': '%s')...\n", key.c_str(), buf.c_str());
        if (auto r = session->put(key, zexample::as_bytes(buf)); !r) {
            std::fprintf(stderr, "put('%s') failed: %s\n", key.c_str(),
                         zexample::error_name(r.error()));
            return 1;
        }
    }

    return 0;
}

import zenoh.broker;

#include <cstdio>
#include <cstdlib>
#include <string>
#include <string_view>
#include <thread>

namespace {

struct Args {
    std::string host = "0.0.0.0";
    std::uint16_t port = 7447;
    unsigned threads = 0; // 0 = std::thread::hardware_concurrency()
};

// Parse "[-l tcp/host:port] [--threads N]". Minimal, matches the examples' own
// argument-parsing style (see e.g. examples/z_put.cpp) rather than pulling in a CLI
// library for a handful of flags.
[[nodiscard]] auto parse_args(int argc, char** argv) -> Args {
    Args args;
    for (int i = 1; i < argc; ++i) {
        std::string_view const a = argv[i];
        if ((a == "-l" || a == "--listen") && i + 1 < argc) {
            std::string_view ep = argv[++i];
            if (ep.starts_with("tcp/")) ep.remove_prefix(4);
            auto const colon = ep.rfind(':');
            if (colon != std::string_view::npos) {
                args.host = std::string(ep.substr(0, colon));
                args.port =
                    static_cast<std::uint16_t>(std::stoi(std::string(ep.substr(colon + 1))));
            }
        } else if (a == "--threads" && i + 1 < argc) {
            args.threads = static_cast<unsigned>(std::stoul(argv[++i]));
        }
    }
    return args;
}

} // namespace

auto main(int argc, char** argv) -> int {
    auto const args = parse_args(argc, argv);

    auto broker = zenoh::broker::Broker::bind(args.host, args.port);
    if (!broker) {
        std::fprintf(stderr, "zenohb: failed to bind tcp/%s:%u\n", args.host.c_str(), args.port);
        return EXIT_FAILURE;
    }

    unsigned const threads = args.threads != 0 ? args.threads : std::thread::hardware_concurrency();
    std::fprintf(stderr, "zenohb: listening on tcp/%s:%u (%u thread%s)\n", args.host.c_str(),
                 (*broker)->port(), threads == 0 ? 1 : threads, threads == 1 ? "" : "s");

    (*broker)->run(threads);
    return EXIT_SUCCESS;
}

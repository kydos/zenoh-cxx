import zenoh.broker;

#include <cstdio>
#include <cstdlib>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

namespace {

struct Args {
    std::string host = "0.0.0.0";
    std::uint16_t port = 7447;
    unsigned threads = 0; // 0 = std::thread::hardware_concurrency()
    bool ok = true;       // cleared by a malformed argument
    /// Peer brokers to dial, from repeated `--peer`. See docs/CLIQUE.md.
    std::vector<std::string> peers;
    /// What to tell the rest of the clique to dial this broker on. Required when
    /// `-l` names a wildcard address.
    std::optional<std::string> advertise;
    /// Whether an inbound connection may become a clique link by announcing
    /// `whatami = router` (`--accept-router-faces`). Off by default: that claim is
    /// unauthenticated, so accepting it from anyone lets a client act as a peer
    /// broker. Needed on whichever end of a peer pair receives the link.
    bool accept_router_faces = false;
};

// Parse an unsigned decimal argument, or return nullopt. `std::stoi`/`std::stoul`
// throw on junk, and this codebase does not use exceptions -- `zenohb --threads x`
// used to terminate.
[[nodiscard]] auto parse_u32(std::string_view text) -> std::optional<std::uint32_t> {
    if (text.empty()) return std::nullopt;
    std::uint64_t value = 0;
    for (char const c : text) {
        if (c < '0' || c > '9') return std::nullopt;
        value = value * 10 + static_cast<std::uint64_t>(c - '0');
        if (value > 0xffffffffULL) return std::nullopt;
    }
    return static_cast<std::uint32_t>(value);
}

// Parse "[-l tcp/host:port] [--peer tcp/host:port]... [--advertise tcp/host:port] [--threads N]".
// Minimal, matches the examples' own argument-parsing style (see e.g. examples/z_put.cpp) rather
// than pulling in a CLI library for a handful of flags.
[[nodiscard]] auto parse_args(int argc, char** argv) -> Args {
    Args args;
    for (int i = 1; i < argc; ++i) {
        std::string_view const a = argv[i];
        if ((a == "-l" || a == "--listen") && i + 1 < argc) {
            std::string_view ep = argv[++i];
            if (ep.starts_with("tcp/")) ep.remove_prefix(4);
            auto const colon = ep.rfind(':');
            if (colon != std::string_view::npos) {
                auto const port = parse_u32(ep.substr(colon + 1));
                if (!port || *port > 0xffff) {
                    std::fprintf(stderr, "zenohb: invalid port in -l %s\n", argv[i]);
                    args.ok = false;
                    continue;
                }
                args.host = std::string(ep.substr(0, colon));
                args.port = static_cast<std::uint16_t>(*port);
            }
        } else if ((a == "-p" || a == "--peer") && i + 1 < argc) {
            args.peers.emplace_back(argv[++i]);
        } else if (a == "--advertise" && i + 1 < argc) {
            args.advertise = argv[++i];
        } else if (a == "--threads" && i + 1 < argc) {
            auto const n = parse_u32(argv[++i]);
            if (!n) {
                std::fprintf(stderr, "zenohb: invalid --threads %s\n", argv[i]);
                args.ok = false;
                continue;
            }
            args.threads = static_cast<unsigned>(*n);
        } else if (a == "--accept-router-faces") {
            args.accept_router_faces = true;
        }
    }
    return args;
}

} // namespace

auto main(int argc, char** argv) -> int {
    auto const args = parse_args(argc, argv);
    if (!args.ok) {
        std::fprintf(stderr, "usage: zenohb [-l tcp/host:port] [--peer tcp/host:port]... "
                             "[--advertise tcp/host:port] [--threads N] [--accept-router-faces]\n");
        return EXIT_FAILURE;
    }

    auto broker = zenoh::broker::Broker::bind(
        zenoh::broker::BrokerConfig{.listen_host = args.host,
                                    .listen_port = args.port,
                                    .peers = args.peers,
                                    .advertise = args.advertise,
                                    .accept_router_faces = args.accept_router_faces});
    if (!broker) {
        std::fprintf(stderr, "zenohb: failed to bind tcp/%s:%u\n", args.host.c_str(), args.port);
        return EXIT_FAILURE;
    }

    unsigned const threads = args.threads != 0 ? args.threads : std::thread::hardware_concurrency();
    std::fprintf(stderr, "zenohb: listening on tcp/%s:%u (%u thread%s)\n", args.host.c_str(),
                 (*broker)->port(), threads == 0 ? 1 : threads, threads == 1 ? "" : "s");
    for (auto const& peer : args.peers) {
        std::fprintf(stderr, "zenohb: peer %s\n", peer.c_str());
    }

    (*broker)->run(threads);
    return EXIT_SUCCESS;
}

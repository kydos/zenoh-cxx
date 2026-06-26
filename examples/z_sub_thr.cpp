// z_sub_thr: subscribe to "test/thr" and measure receive throughput. The C++23
// equivalent of zenoh-rust's z_sub_thr.rs — a callback subscriber that counts messages
// in rounds of N, prints `msg/s` per round, and exits after M rounds (printing a final
// summary).
//
// Usage: z_sub_thr [options]
//   -e, --connect E  router endpoint (default tcp/127.0.0.1:7447)
//   -s, --samples M  number of throughput measurements before exit (default 10)
//   -n, --number N   messages per measurement round (default 100000)
//
// Measure against the publisher (this project's z_pub_thr or the reference):
//   zenohd -l tcp/127.0.0.1:7447 &
//   ./build/clang/examples/z_sub_thr -e tcp/127.0.0.1:7447 &
//   ./build/clang/examples/z_pub_thr -e tcp/127.0.0.1:7447 8

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <optional>
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
    }
    return "unknown";
}

using Clock = std::chrono::steady_clock;

// Throughput counter: runs exactly `rounds` rounds, each receiving exactly
// `round_size` samples; per round it prints `samples/elapsed` msg/s, and a final
// summary over all rounds. `increment()` returns true once all rounds are done.
class Stats {
  public:
    Stats(std::size_t rounds, std::size_t round_size) : rounds_(rounds), round_size_(round_size) {}

    [[nodiscard]] auto increment() -> bool {
        if (round_count_ == 0) {
            round_start_ = Clock::now();
            if (!global_start_) global_start_ = round_start_;
        }
        if (++round_count_ == round_size_) { // this round received exactly round_size_ samples
            print_round();
            ++finished_rounds_;
            round_count_ = 0;
            return finished_rounds_ >= rounds_;
        }
        return false;
    }

    auto print_summary() const -> void {
        if (!global_start_) return;
        double const elapsed = std::chrono::duration<double>(Clock::now() - *global_start_).count();
        std::size_t const total = round_size_ * finished_rounds_ + round_count_;
        std::printf("Received %zu messages over %.2fs: %f msg/s\n", total, elapsed,
                    static_cast<double>(total) / elapsed);
    }

  private:
    auto print_round() const -> void {
        double const elapsed = std::chrono::duration<double>(Clock::now() - round_start_).count();
        std::printf("%f msg/s\n", static_cast<double>(round_size_) / elapsed);
    }

    std::size_t rounds_;     // total rounds to run before exiting
    std::size_t round_size_; // samples per round
    std::size_t round_count_ = 0;
    std::size_t finished_rounds_ = 0;
    Clock::time_point round_start_{};
    std::optional<Clock::time_point> global_start_{};
};

} // namespace

auto main(int argc, char** argv) -> int {
    std::setvbuf(stdout, nullptr, _IOLBF, 0);

    std::string endpoint = "tcp/127.0.0.1:7447";
    std::size_t samples = 10;    // m: number of measurement rounds before exit
    std::size_t number = 100000; // n: messages per round

    for (int i = 1; i < argc; ++i) {
        std::string_view arg = argv[i];
        if ((arg == "-e" || arg == "--connect") && i + 1 < argc) {
            endpoint = argv[++i];
        } else if ((arg == "-s" || arg == "--samples") && i + 1 < argc) {
            samples = std::strtoul(argv[++i], nullptr, 10);
        } else if ((arg == "-n" || arg == "--number") && i + 1 < argc) {
            number = std::strtoul(argv[++i], nullptr, 10);
        }
    }
    if (number == 0) number = 1;

    auto session = zenoh::Session::open(endpoint);
    if (!session) {
        std::fprintf(stderr, "open(%s) failed: %s\n", endpoint.c_str(),
                     error_name(session.error()));
        return 1;
    }

    Stats stats(samples, number); // `samples` rounds, each of `number` samples
    // A generous strand so a full TCP batch of tiny payloads is delivered without
    // mid-batch backpressure pauses.
    auto sub = session->declare_subscriber(
        "test/thr",
        [&stats](const zenoh::Sample&) {
            if (stats.increment()) { // all rounds done
                stats.print_summary();
                std::exit(0);
            }
        },
        zenoh::SubscriberOptions{.capacity = 8192});
    if (!sub) {
        std::fprintf(stderr, "declare_subscriber failed: %s\n", error_name(sub.error()));
        return 1;
    }

    std::printf("Press CTRL-C to quit...\n");
    // Drive the receive pump; the callback fires per sample and exits after M rounds.
    if (auto r = session->run(); !r) {
        if (r.error() != zenoh::ZError::connection_closed)
            std::fprintf(stderr, "run failed: %s\n", error_name(r.error()));
    }
    return 0;
}

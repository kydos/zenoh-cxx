// z_sub_thr: subscribe to "test/thr" and measure receive throughput. The C++23
// equivalent of zenoh-rust's z_sub_thr.rs — a callback subscriber that counts messages
// in rounds of N, prints `msg/s` per round, and exits after M rounds (printing a final
// summary).
//
// Usage: z_sub_thr [OPTIONS]    (run with --help for the full option list)
//   -s, --samples <SAMPLES>  throughput measurements before exit [default: 10]
//   -n, --number <NUMBER>    messages per measurement round [default: 100000]
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

#include "zexample.hpp"

namespace {

auto usage(const char* argv0) -> void {
    std::printf("Usage: %s [OPTIONS]\n\n"
                "Options:\n"
                "  -s, --samples <SAMPLES>\n"
                "          Number of throughput measurements before exiting [default: 10]\n"
                "  -n, --number <NUMBER>\n"
                "          Number of messages in each measurement [default: 100000]\n",
                argv0);
    zexample::print_common_help();
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
    if (zexample::wants_help(argc, argv)) {
        usage(argv[0]);
        return 0;
    }
    std::setvbuf(stdout, nullptr, _IOLBF, 0);

    zexample::CommonArgs common;
    std::size_t samples = 10;    // m: number of measurement rounds before exit
    std::size_t number = 100000; // n: messages per round

    for (int i = 1; i < argc; ++i) {
        std::string_view arg = argv[i];
        if (arg == "-s" || arg == "--samples") {
            const char* value = zexample::option_value(argc, argv, i);
            if (value == nullptr) return 1;
            samples = std::strtoul(value, nullptr, 10);
        } else if (arg == "-n" || arg == "--number") {
            const char* value = zexample::option_value(argc, argv, i);
            if (value == nullptr) return 1;
            number = std::strtoul(value, nullptr, 10);
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
    if (number == 0) number = 1;

    auto session = zenoh::Session::open(endpoint);
    if (!session) {
        std::fprintf(stderr, "open(%s) failed: %s\n", endpoint.c_str(),
                     zexample::error_name(session.error()));
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
        std::fprintf(stderr, "declare_subscriber failed: %s\n", zexample::error_name(sub.error()));
        return 1;
    }

    std::printf("Press CTRL-C to quit...\n");
    // Drive the receive pump; the callback fires per sample and exits after M rounds.
    if (auto r = session->run(); !r) {
        if (r.error() != zenoh::ZError::connection_closed)
            std::fprintf(stderr, "run failed: %s\n", zexample::error_name(r.error()));
    }
    return 0;
}

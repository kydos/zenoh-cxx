#pragma once
//
// Minimal, dependency-free test harness.
//
// Tests stay self-contained (no network fetch, no extra build deps) in the spirit of
// the project's "simple and streamlined" goal: `TEST("name") { CHECK(cond); }` registers
// a case at static-init time, and `main()` hands the whole registry to `run()`.
//
// Beyond registration the harness reports progress per test: one `ok`/`FAIL` line per
// case, grouped under the source file the case came from, carrying its elapsed time,
// with every failed CHECK indented beneath it. A small CLI (`--list`, `--run`,
// `--filter`, `--quiet`) narrows a run down to a single case or a substring match --
// `--list`/`--run` are also what `tests/ZTestDiscover.cmake` uses to register one ctest
// case per TEST, so `ctest` reports the suite test-by-test rather than as one opaque
// binary.
//
#include <unistd.h>

#include <chrono>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <string_view>
#include <vector>

namespace ztest {

/// One registered test case: the source file stem it was declared in, its display
/// name, and the function holding the TEST body.
struct Case {
    std::string_view suite;
    std::string_view name;
    void (*fn)();
};

/// Source-file stem of `path` ("tests/test_ke.cpp" -> "test_ke"), used as the suite a
/// case is grouped and labeled under. The result borrows `path`, which is always a
/// `__FILE__` literal with static storage duration.
inline auto suite_of(std::string_view path) -> std::string_view {
    if (const auto slash = path.find_last_of("/\\"); slash != std::string_view::npos) {
        path.remove_prefix(slash + 1);
    }
    if (const auto dot = path.find_last_of('.'); dot != std::string_view::npos) {
        path = path.substr(0, dot);
    }
    return path;
}

/// Every case declared with TEST, in static-init order (which groups by translation
/// unit, hence by source file).
inline auto registry() -> std::vector<Case>& {
    static std::vector<Case> r;
    return r;
}

/// Failed CHECKs across the whole run; also the process exit status (0 == all passed).
inline auto failures() -> int& {
    static int f = 0;
    return f;
}

/// CHECKs evaluated across the whole run, reported in the summary line.
inline auto checks() -> int& {
    static int c = 0;
    return c;
}

/// ANSI escapes, all empty unless colored output is enabled, so piped and CI logs stay
/// plain text.
struct Style {
    const char* dim = "";
    const char* red = "";
    const char* green = "";
    const char* bold = "";
    const char* reset = "";
};

inline auto style() -> Style& {
    static Style s;
    return s;
}

/// Turn ANSI styling on or off. Also gates the transient "currently running" line,
/// which only makes sense on a terminal that can overwrite it.
inline auto set_color(bool on) -> void {
    style() = on ? Style{"\033[2m", "\033[31m", "\033[32m", "\033[1m", "\033[0m"} : Style{};
}

/// True when stdout is an interactive terminal that has not opted out via NO_COLOR or
/// TERM=dumb.
inline auto color_supported() -> bool {
    if (::isatty(::fileno(stdout)) == 0) {
        return false;
    }
    if (const char* no_color = std::getenv("NO_COLOR"); no_color != nullptr && *no_color != '\0') {
        return false;
    }
    const char* term = std::getenv("TERM");
    return term == nullptr || std::strcmp(term, "dumb") != 0;
}

struct Reg {
    Reg(const char* file, std::string_view name, void (*fn)()) {
        registry().push_back({suite_of(file), name, fn});
    }
};

/// Source locations of the CHECKs the running case has failed. Buffered rather than
/// printed on the spot so they appear *under* that case's result line, keeping a piped
/// log readable top to bottom.
inline auto pending_failures() -> std::vector<std::string>& {
    static std::vector<std::string> p;
    return p;
}

/// Record one CHECK, tallying the failures that decide the process exit status.
inline auto check(bool cond, const char* expr, const char* file, int line) -> void {
    ++checks();
    if (!cond) {
        ++failures();
        pending_failures().push_back(std::string(file) + ':' + std::to_string(line) + ": CHECK(" +
                                     expr + ')');
    }
}

/// Fully qualified case id, "<suite>::<name>" -- the form `--list` prints, `--run`
/// accepts, and ctest registers cases under.
inline auto id_of(const Case& c) -> std::string {
    std::string s(c.suite);
    s += "::";
    s += c.name;
    return s;
}

/// Parsed command line. An empty `only`/`filters` selection runs every case.
struct Options {
    std::vector<std::string_view> filters; ///< --filter: substring match against the id.
    std::string_view only;                 ///< --run: exact id (or bare name) match.
    bool list = false;                     ///< --list: print ids and exit.
    bool quiet = false;                    ///< --quiet: only failures and the summary.
    bool help = false;                     ///< --help: print usage and exit.
    bool color = false;
};

inline auto usage(const char* argv0) -> void {
    std::printf("usage: %s [options]\n\n"
                "  --list             print every test id (\"<suite>::<name>\") and exit\n"
                "  --run <id>         run exactly one case, matched on its id or bare name\n"
                "  --filter <text>    run only cases whose id contains <text> (repeatable)\n"
                "  -q, --quiet        print only failures and the summary\n"
                "  --color, --no-color  force styled output on or off (default: auto)\n"
                "  -h, --help         show this message\n",
                argv0);
}

/// Parse argv into `opt`. Returns false on a malformed command line, having already
/// reported what was wrong.
inline auto parse_args(int argc, char** argv, Options& opt) -> bool {
    const auto needs_value = [&](int i) {
        if (i + 1 >= argc) {
            std::fprintf(stderr, "ztest: %s requires an argument\n", argv[i]);
            return false;
        }
        return true;
    };
    for (int i = 1; i < argc; ++i) {
        const std::string_view arg = argv[i];
        if (arg == "--list") {
            opt.list = true;
        } else if (arg == "--run") {
            if (!needs_value(i)) {
                return false;
            }
            opt.only = argv[++i];
        } else if (arg == "--filter") {
            if (!needs_value(i)) {
                return false;
            }
            opt.filters.emplace_back(argv[++i]);
        } else if (arg == "-q" || arg == "--quiet") {
            opt.quiet = true;
        } else if (arg == "--color") {
            opt.color = true;
        } else if (arg == "--no-color") {
            opt.color = false;
        } else if (arg == "-h" || arg == "--help") {
            opt.help = true;
        } else {
            std::fprintf(stderr, "ztest: unknown option '%.*s' (try --help)\n",
                         static_cast<int>(arg.size()), arg.data());
            return false;
        }
    }
    return true;
}

/// True when `c` is part of the selection described by `opt`.
inline auto selected(const Case& c, const Options& opt) -> bool {
    if (!opt.only.empty()) {
        return id_of(c) == opt.only || c.name == opt.only;
    }
    if (opt.filters.empty()) {
        return true;
    }
    const std::string id = id_of(c);
    for (const auto& f : opt.filters) {
        if (id.find(f) != std::string::npos) {
            return true;
        }
    }
    return false;
}

/// Write `name`, then pad to a fixed column and append `ms` right-aligned, so the
/// timings line up down the report.
inline auto print_timed(std::string_view name, double ms) -> void {
    constexpr std::size_t kTimeColumn = 74;
    std::fwrite(name.data(), 1, name.size(), stdout);
    for (std::size_t pad = name.size() < kTimeColumn ? kTimeColumn - name.size() : 1; pad > 0;
         --pad) {
        std::putchar(' ');
    }
    if (ms >= 1000.0) {
        std::printf("%s%7.2f s%s\n", style().dim, ms / 1000.0, style().reset);
    } else {
        std::printf("%s%7.2f ms%s\n", style().dim, ms, style().reset);
    }
}

/// Run the selected cases, reporting each one as it completes.
///
/// Returns 0 when every CHECK passed, 1 when any failed, and 2 for a bad command line
/// or a `--run` id that matches no case.
inline auto run(int argc = 0, char** argv = nullptr) -> int {
    Options opt;
    opt.color = color_supported();
    if (argc > 0 && !parse_args(argc, argv, opt)) {
        return 2;
    }
    if (opt.help) {
        usage(argv[0]);
        return 0;
    }
    set_color(opt.color);

    if (opt.list) {
        for (const auto& c : registry()) {
            std::printf("%s\n", id_of(c).c_str());
        }
        return 0;
    }

    std::vector<const Case*> cases;
    for (const auto& c : registry()) {
        if (selected(c, opt)) {
            cases.push_back(&c);
        }
    }
    if (cases.empty()) {
        std::fprintf(stderr, "ztest: no test matched the given selection\n");
        return 2;
    }

    std::vector<std::string> failed;
    std::string_view suite;
    const auto started = std::chrono::steady_clock::now();
    for (const Case* c : cases) {
        if (!opt.quiet && c->suite != suite) {
            suite = c->suite;
            std::printf("\n%s%.*s%s\n", style().bold, static_cast<int>(suite.size()), suite.data(),
                        style().reset);
        }
        // On a terminal, show the case before it runs so a hang or a crash names the
        // culprit; the result line below overwrites it (it is never shorter).
        if (!opt.quiet && opt.color) {
            std::printf("  %s....%s %.*s\r", style().dim, style().reset,
                        static_cast<int>(c->name.size()), c->name.data());
            std::fflush(stdout);
        }

        const int before = failures();
        const auto t0 = std::chrono::steady_clock::now();
        c->fn();
        const std::chrono::duration<double, std::milli> elapsed =
            std::chrono::steady_clock::now() - t0;

        const bool ok = failures() == before;
        if (!ok) {
            failed.push_back(id_of(*c));
        }
        if (!ok || !opt.quiet) {
            std::printf("  %s%-4s%s ", ok ? style().green : style().red, ok ? "ok" : "FAIL",
                        style().reset);
            print_timed(c->name, elapsed.count());
        }
        for (const auto& where : pending_failures()) {
            std::printf("         %s%s%s\n", style().red, where.c_str(), style().reset);
        }
        pending_failures().clear();
    }
    const std::chrono::duration<double> total = std::chrono::steady_clock::now() - started;

    const auto passed = cases.size() - failed.size();
    std::printf("\n%s%zu/%zu tests passed%s, %d checks (%d failed) in %.2f s\n",
                failed.empty() ? style().green : style().red, passed, cases.size(), style().reset,
                checks(), failures(), total.count());
    if (!failed.empty()) {
        std::printf("\nfailed:\n");
        for (const auto& id : failed) {
            std::printf("  %s%s%s\n", style().red, id.c_str(), style().reset);
        }
    }
    std::fflush(stdout);
    return failures() == 0 ? 0 : 1;
}

} // namespace ztest

#define ZTEST_CAT2(a, b) a##b
#define ZTEST_CAT(a, b) ZTEST_CAT2(a, b)

/// Declare a test case: `TEST("what it proves") { CHECK(...); }`.
#define TEST(name)                                                                                 \
    static auto ZTEST_CAT(ztest_fn_, __LINE__)()->void;                                            \
    static ::ztest::Reg ZTEST_CAT(ztest_reg_, __LINE__){__FILE__, name,                            \
                                                        &ZTEST_CAT(ztest_fn_, __LINE__)};          \
    static auto ZTEST_CAT(ztest_fn_, __LINE__)() -> void

/// Assert `cond`; on failure record it and print the source location, then continue.
#define CHECK(cond) ::ztest::check((cond), #cond, __FILE__, __LINE__)

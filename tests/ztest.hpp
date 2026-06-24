#pragma once
//
// Minimal, dependency-free test harness.
//
// Phase 1 keeps tests self-contained (no network fetch, no extra build deps) in
// the spirit of the project's "simple and streamlined" goal. It can be swapped for
// doctest later without changing test bodies: TEST(...) registers a case, CHECK(x)
// records a failure. Tests `import` the zenoh modules and `#include` this header.
//
#include <cstddef>
#include <cstdio>
#include <string_view>
#include <vector>

namespace ztest {

struct Case {
    std::string_view name;
    void (*fn)();
};

inline auto registry() -> std::vector<Case>& {
    static std::vector<Case> r;
    return r;
}

inline auto failures() -> int& {
    static int f = 0;
    return f;
}

struct Reg {
    Reg(std::string_view name, void (*fn)()) { registry().push_back({name, fn}); }
};

inline auto check(bool cond, const char* expr, const char* file, int line) -> void {
    if (!cond) {
        ++failures();
        std::fprintf(stderr, "  FAIL %s:%d: CHECK(%s)\n", file, line, expr);
    }
}

inline auto run() -> int {
    int passed = 0;
    for (const auto& c : registry()) {
        const int before = failures();
        c.fn();
        if (failures() == before) {
            ++passed;
        } else {
            std::fprintf(stderr, "  ^ in test \"%.*s\"\n", static_cast<int>(c.name.size()),
                         c.name.data());
        }
    }
    std::fprintf(stderr, "%d/%zu tests passed; %d checks failed\n", passed, registry().size(),
                 failures());
    return failures() == 0 ? 0 : 1;
}

} // namespace ztest

#define ZTEST_CAT2(a, b) a##b
#define ZTEST_CAT(a, b) ZTEST_CAT2(a, b)

#define TEST(name)                                                                                 \
    static auto ZTEST_CAT(ztest_fn_, __LINE__)()->void;                                            \
    static ::ztest::Reg ZTEST_CAT(ztest_reg_, __LINE__){name, &ZTEST_CAT(ztest_fn_, __LINE__)};    \
    static auto ZTEST_CAT(ztest_fn_, __LINE__)() -> void

#define CHECK(cond) ::ztest::check((cond), #cond, __FILE__, __LINE__)

// Evaluation (Computation / Evaluator) integration tests: a real in-process
// zenoh::broker::Broker on loopback, driven by real zenoh::Session clients -- the same
// inverted harness test_broker.cpp uses, and the only one that can prove what matters
// here, since every guarantee of the abstraction (fan-out to all matching
// registrations, isolation from ordinary Query/Reply, reply identity, no
// consolidation) is a property of what actually crosses the wire.
//
// Everything runs on the test thread: the two sessions are pumped explicitly and in
// order (send the eval, pump the responder until it has run the computations, then
// drain the evaluator's replies), so there is no sleep-and-hope and no CHECK() on a
// background thread -- see test_query_api.cpp's note on why that discipline matters.
// The one exception is the 50 ms after `declare_*`, which lets a declaration reach the
// broker's routing strand before the evaluation that must find it (the same wait
// test_broker.cpp uses for the same reason).
import zenoh.broker;
import zenoh;

#include "ztest.hpp"

#include <chrono>
#include <cstddef>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

using namespace zenoh;

namespace {

auto bytes(std::string_view s) -> std::span<const std::byte> {
    return {reinterpret_cast<const std::byte*>(s.data()), s.size()};
}

auto str(std::span<const std::byte> b) -> std::string {
    return {reinterpret_cast<const char*>(b.data()), b.size()};
}

// Same shape as test_broker.cpp's TestBroker (a broker on an ephemeral loopback port,
// stopped and joined on destruction); duplicated rather than shared because the test
// files are deliberately self-contained.
struct TestBroker {
    std::unique_ptr<broker::Broker> broker;
    std::thread runner;

    TestBroker() = default;
    TestBroker(const TestBroker&) = delete;
    auto operator=(const TestBroker&) -> TestBroker& = delete;
    TestBroker(TestBroker&&) = default;
    auto operator=(TestBroker&&) -> TestBroker& = default;

    static auto start() -> TestBroker {
        TestBroker tb;
        auto b = broker::Broker::bind("127.0.0.1", 0);
        CHECK(b.has_value());
        if (!b) return tb;
        tb.broker = std::move(*b);
        auto* raw = tb.broker.get();
        tb.runner = std::thread([raw] { raw->run(1); });
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        return tb;
    }

    [[nodiscard]] auto endpoint() const -> std::string {
        return "tcp/127.0.0.1:" + std::to_string(broker->port());
    }

    ~TestBroker() {
        if (broker) broker->stop();
        if (runner.joinable()) runner.join();
    }
};

/// Let the declarations just made reach the broker's routing strand.
auto settle() -> void { std::this_thread::sleep_for(std::chrono::milliseconds(50)); }

/// Evaluations are collected with a short deadline so a regression fails in seconds
/// rather than sitting out the 10 s default.
auto test_opts() -> EvalOptions { return EvalOptions{.timeout_ms = 5000}; }

/// Pump `s` until `done()` holds, or until `max_pumps` batches have gone by (each
/// `run_once` blocks at most one keepalive interval, so a broken test ends rather
/// than hangs). Used to drive the *responder*: the evaluator side pumps itself
/// inside `Getter::recv()`.
auto pump_until(Session& s, const std::function<bool()>& done, int max_pumps = 8) -> void {
    for (int i = 0; i < max_pumps && !done(); ++i) (void)s.run_once();
}

/// Everything one evaluation returned: the (key, payload) of each ok reply in arrival
/// order, how many error replies came back, and whether the reply stream closed
/// normally (`Getter::recv()` returning `nullopt`) rather than timing out.
struct Replies {
    std::vector<std::pair<std::string, std::string>> ok;
    int errors = 0;
    bool completed = false;

    [[nodiscard]] auto count_key(std::string_view key) const -> std::size_t {
        std::size_t n = 0;
        for (auto const& [k, _] : ok) {
            if (k == key) ++n;
        }
        return n;
    }
};

auto collect(Getter& getter) -> Replies {
    Replies out;
    for (;;) {
        auto r = getter.recv();
        if (!r) return out; // error (incl. timeout): `completed` stays false
        if (!*r) {
            out.completed = true;
            return out;
        }
        if ((*r)->is_ok()) {
            out.ok.emplace_back(std::string((*r)->sample().key_expr()),
                                str((*r)->sample().payload()));
        } else {
            ++out.errors;
        }
    }
}

/// A computation that counts its invocations and replies with `value`, for the
/// side-effect and fan-out cases.
auto counting_computation(int& counter, std::string_view value) -> EvalHandler {
    return [&counter, value](Eval e) {
        ++counter;
        (void)e.reply(bytes(value));
    };
}

} // namespace

// --- Declaration -----------------------------------------------------------------

TEST("declare_computation accepts a concrete key and rejects every wild or "
     "non-canonical one") {
    auto tb = TestBroker::start();
    if (!tb.broker) return;

    auto session = Session::open(tb.endpoint());
    CHECK(session.has_value());
    if (!session) return;

    auto ok = session->declare_computation("foo/bar");
    CHECK(ok.has_value());
    CHECK(ok.has_value() && ok->key() == "foo/bar");

    for (auto const* wild : {"foo/*", "foo/**", "*/bar", "**", "*"}) {
        auto bad = session->declare_computation(wild);
        CHECK(!bad.has_value());
        CHECK(!bad.has_value() && bad.error() == ZError::invalid_key_expr);
    }
    // Non-canonical keys are rejected on the same footing: an empty chunk or a
    // doubled `**` is not one concrete computation either.
    for (auto const* bad_key : {"foo//bar", "", "/foo", "foo/"}) {
        auto bad = session->declare_computation(bad_key);
        CHECK(!bad.has_value());
        CHECK(!bad.has_value() && bad.error() == ZError::invalid_key_expr);
    }
}

TEST("a Computation may be undeclared explicitly and by dropping its handle") {
    auto tb = TestBroker::start();
    if (!tb.broker) return;

    auto responder = Session::open(tb.endpoint());
    auto requester = Session::open(tb.endpoint());
    CHECK(responder.has_value() && requester.has_value());
    if (!responder || !requester) return;

    int runs = 0;
    {
        auto comp = responder->declare_computation("foo/a", counting_computation(runs, "ok"));
        CHECK(comp.has_value());
        if (!comp) return;
        comp->undeclare();
        CHECK(comp->key().empty()); // undeclared: no key left to report
    }
    settle();

    auto getter = requester->eval("foo/a", bytes("arg"), test_opts());
    CHECK(getter.has_value());
    if (!getter) return;
    auto replies = collect(*getter);

    // Nothing is registered any more, so the evaluation completes with no replies --
    // the same outcome as evaluating a key nobody ever registered. `completed` is the
    // load-bearing check: had the undeclared computation still been registered, the
    // broker would be waiting on a reply from a session nobody is pumping, and this
    // would time out instead. (The responder is deliberately not pumped: that is what
    // makes the check meaningful, and it keeps the case off the keepalive clock.)
    CHECK(runs == 0);
    CHECK(replies.ok.empty());
    CHECK(replies.completed);
}

TEST("undeclaring one of two Computations at the same key leaves the other running") {
    auto tb = TestBroker::start();
    if (!tb.broker) return;

    auto responder = Session::open(tb.endpoint());
    auto requester = Session::open(tb.endpoint());
    CHECK(responder.has_value() && requester.has_value());
    if (!responder || !requester) return;

    int runs_1 = 0;
    int runs_2 = 0;
    auto c1 = responder->declare_computation("foo/a", counting_computation(runs_1, "one"));
    auto c2 = responder->declare_computation("foo/a", counting_computation(runs_2, "two"));
    CHECK(c1.has_value() && c2.has_value());
    if (!c1 || !c2) return;
    settle();

    c1->undeclare();
    settle();

    auto getter = requester->eval("foo/a", bytes("arg"), test_opts());
    CHECK(getter.has_value());
    if (!getter) return;
    pump_until(*responder, [&] { return runs_2 > 0; });
    auto replies = collect(*getter);

    // The survivor must still be reachable: two Computations at one key are two
    // independent registrations, and undeclaring one says nothing about the other.
    CHECK(runs_1 == 0);
    CHECK(runs_2 == 1);
    CHECK(replies.completed);
    CHECK(replies.ok.size() == 1);
    CHECK(replies.ok.size() == 1 && replies.ok[0].second == "two");
}

// --- Fan-out ---------------------------------------------------------------------

TEST("an exact evaluation runs only the Computation registered at that key") {
    auto tb = TestBroker::start();
    if (!tb.broker) return;

    auto responder = Session::open(tb.endpoint());
    auto requester = Session::open(tb.endpoint());
    CHECK(responder.has_value() && requester.has_value());
    if (!responder || !requester) return;

    int runs_a = 0;
    int runs_b = 0;
    auto ca = responder->declare_computation("foo/a", counting_computation(runs_a, "a"));
    auto cb = responder->declare_computation("foo/b", counting_computation(runs_b, "b"));
    CHECK(ca.has_value() && cb.has_value());
    if (!ca || !cb) return;
    settle();

    auto getter = requester->eval("foo/a", bytes("arg"), test_opts());
    CHECK(getter.has_value());
    if (!getter) return;
    pump_until(*responder, [&] { return runs_a > 0; });
    auto replies = collect(*getter);

    CHECK(runs_a == 1);
    CHECK(runs_b == 0);
    CHECK(replies.completed);
    CHECK(replies.ok.size() == 1);
    CHECK(replies.ok.size() == 1 && replies.ok[0].first == "foo/a");
    CHECK(replies.ok.size() == 1 && replies.ok[0].second == "a");
}

TEST("a wildcard evaluation runs every matching Computation and no other") {
    auto tb = TestBroker::start();
    if (!tb.broker) return;

    auto responder = Session::open(tb.endpoint());
    auto requester = Session::open(tb.endpoint());
    CHECK(responder.has_value() && requester.has_value());
    if (!responder || !requester) return;

    int runs_a = 0;
    int runs_b = 0;
    int runs_c = 0;
    int runs_bar = 0;
    auto ca = responder->declare_computation("foo/a", counting_computation(runs_a, "a"));
    auto cb = responder->declare_computation("foo/b", counting_computation(runs_b, "b"));
    auto cc = responder->declare_computation("foo/c", counting_computation(runs_c, "c"));
    auto cbar = responder->declare_computation("bar/a", counting_computation(runs_bar, "bar"));
    CHECK(ca.has_value() && cb.has_value() && cc.has_value() && cbar.has_value());
    if (!ca || !cb || !cc || !cbar) return;
    settle();

    auto getter = requester->eval("foo/*", bytes("arg"), test_opts());
    CHECK(getter.has_value());
    if (!getter) return;
    pump_until(*responder, [&] { return runs_a > 0 && runs_b > 0 && runs_c > 0; });
    auto replies = collect(*getter);

    // Every matching registration ran exactly once for the one delivered request --
    // which is a statement about this fan-out, NOT an exactly-once delivery guarantee
    // across failures or retries (see session.cppm's `Session::eval`).
    CHECK(runs_a == 1);
    CHECK(runs_b == 1);
    CHECK(runs_c == 1);
    CHECK(runs_bar == 0);
    CHECK(replies.completed);
    CHECK(replies.ok.size() == 3);
    CHECK(replies.count_key("foo/a") == 1);
    CHECK(replies.count_key("foo/b") == 1);
    CHECK(replies.count_key("foo/c") == 1);
}

TEST("two Computations registered at the same key both run, with no deduplication") {
    auto tb = TestBroker::start();
    if (!tb.broker) return;

    auto responder = Session::open(tb.endpoint());
    auto requester = Session::open(tb.endpoint());
    CHECK(responder.has_value() && requester.has_value());
    if (!responder || !requester) return;

    int runs_1 = 0;
    int runs_2 = 0;
    auto c1 = responder->declare_computation("foo/a", counting_computation(runs_1, "one"));
    auto c2 = responder->declare_computation("foo/a", counting_computation(runs_2, "two"));
    CHECK(c1.has_value() && c2.has_value());
    if (!c1 || !c2) return;
    settle();

    auto getter = requester->eval("foo/a", bytes("arg"), test_opts());
    CHECK(getter.has_value());
    if (!getter) return;
    pump_until(*responder, [&] { return runs_1 > 0 && runs_2 > 0; });
    auto replies = collect(*getter);

    // The guaranteed fan-out unit is the *registration*, not the key: both run, and
    // both replies reach the evaluator (nothing is consolidated by key either).
    CHECK(runs_1 == 1);
    CHECK(runs_2 == 1);
    CHECK(replies.completed);
    CHECK(replies.ok.size() == 2);
    CHECK(replies.count_key("foo/a") == 2);
}

TEST("computations spread over several sessions all run, each replying under its own "
     "key") {
    auto tb = TestBroker::start();
    if (!tb.broker) return;

    auto first = Session::open(tb.endpoint());
    auto second = Session::open(tb.endpoint());
    auto requester = Session::open(tb.endpoint());
    CHECK(first.has_value() && second.has_value() && requester.has_value());
    if (!first || !second || !requester) return;

    int runs_a = 0;
    int runs_b = 0;
    auto ca = first->declare_computation("robot/r1/reset", counting_computation(runs_a, "r1 ok"));
    auto cb = second->declare_computation("robot/r2/reset", counting_computation(runs_b, "r2 ok"));
    CHECK(ca.has_value() && cb.has_value());
    if (!ca || !cb) return;
    settle();

    auto getter = requester->eval("robot/*/reset", bytes(""), test_opts());
    CHECK(getter.has_value());
    if (!getter) return;
    pump_until(*first, [&] { return runs_a > 0; });
    pump_until(*second, [&] { return runs_b > 0; });
    auto replies = collect(*getter);

    CHECK(runs_a == 1);
    CHECK(runs_b == 1);
    CHECK(replies.completed);
    CHECK(replies.ok.size() == 2);
    CHECK(replies.count_key("robot/r1/reset") == 1);
    CHECK(replies.count_key("robot/r2/reset") == 1);
}

// --- Isolation from ordinary Query/Reply -----------------------------------------

TEST("a get() reaches only the Queryable and an eval() only the Computation, on one "
     "key") {
    auto tb = TestBroker::start();
    if (!tb.broker) return;

    auto responder = Session::open(tb.endpoint());
    auto requester = Session::open(tb.endpoint());
    CHECK(responder.has_value() && requester.has_value());
    if (!responder || !requester) return;

    int queries = 0;
    int evals = 0;
    auto qbl = responder->declare_queryable("foo/a", [&](IncomingQuery q) {
        ++queries;
        (void)q.reply("foo/a", bytes("from queryable"));
    });
    auto comp = responder->declare_computation("foo/a", [&](Eval e) {
        ++evals;
        (void)e.reply(bytes("from computation"));
    });
    CHECK(qbl.has_value() && comp.has_value());
    if (!qbl || !comp) return;
    settle();

    // 1. An ordinary get() on the very same key must not invoke the Computation.
    auto getter = requester->get("foo/a");
    CHECK(getter.has_value());
    if (!getter) return;
    pump_until(*responder, [&] { return queries > 0; });
    auto got = collect(*getter);

    CHECK(queries == 1);
    CHECK(evals == 0);
    CHECK(got.completed);
    CHECK(got.ok.size() == 1);
    CHECK(got.ok.size() == 1 && got.ok[0].second == "from queryable");

    // 2. And an eval() must not invoke the Queryable.
    auto evaluated = requester->eval("foo/a", bytes("arg"), test_opts());
    CHECK(evaluated.has_value());
    if (!evaluated) return;
    pump_until(*responder, [&] { return evals > 0; });
    auto ran = collect(*evaluated);

    CHECK(queries == 1); // unchanged
    CHECK(evals == 1);
    CHECK(ran.completed);
    CHECK(ran.ok.size() == 1);
    CHECK(ran.ok.size() == 1 && ran.ok[0].second == "from computation");
}

TEST("not even get(\"**\") reaches a Computation: the internal namespace is verbatim") {
    auto tb = TestBroker::start();
    if (!tb.broker) return;

    auto responder = Session::open(tb.endpoint());
    auto requester = Session::open(tb.endpoint());
    CHECK(responder.has_value() && requester.has_value());
    if (!responder || !requester) return;

    int queries = 0;
    int evals = 0;
    auto qbl = responder->declare_queryable("foo/a", [&](IncomingQuery q) {
        ++queries;
        (void)q.reply("foo/a", bytes("from queryable"));
    });
    auto comp = responder->declare_computation("foo/a", [&](Eval e) {
        ++evals;
        (void)e.reply(bytes("from computation"));
    });
    CHECK(qbl.has_value() && comp.has_value());
    if (!qbl || !comp) return;
    settle();

    // `**` matches every ordinary key in the system, and must still match nothing in
    // the reserved `@eval` namespace -- that is what makes the mapping an isolation
    // mechanism rather than an obfuscation.
    auto getter = requester->get("**");
    CHECK(getter.has_value());
    if (!getter) return;
    pump_until(*responder, [&] { return queries > 0; });
    auto got = collect(*getter);

    CHECK(queries == 1);
    CHECK(evals == 0);
    CHECK(got.completed);
    CHECK(got.ok.size() == 1);
}

TEST("an eval() never reaches a Queryable declared on a matching wildcard") {
    auto tb = TestBroker::start();
    if (!tb.broker) return;

    auto responder = Session::open(tb.endpoint());
    auto requester = Session::open(tb.endpoint());
    CHECK(responder.has_value() && requester.has_value());
    if (!responder || !requester) return;

    int queries = 0;
    auto qbl = responder->declare_queryable("**", [&](IncomingQuery q) {
        ++queries;
        (void)q.reply("foo/a", bytes("from queryable"));
    });
    CHECK(qbl.has_value());
    if (!qbl) return;
    settle();

    auto getter = requester->eval("foo/a", bytes("arg"), test_opts());
    CHECK(getter.has_value());
    if (!getter) return;
    auto replies = collect(*getter);

    // The responder is deliberately never pumped, which is what makes `completed` the
    // proof: if the broker had fanned this eval out to the `**` queryable it would be
    // holding the request open waiting for that queryable's answer, and the evaluation
    // would end in a timeout rather than a clean, empty completion.
    CHECK(queries == 0);
    CHECK(replies.ok.empty());
    CHECK(replies.completed);
}

TEST("the internal namespace cannot be named through the ordinary query API") {
    auto tb = TestBroker::start();
    if (!tb.broker) return;

    auto responder = Session::open(tb.endpoint());
    auto requester = Session::open(tb.endpoint());
    CHECK(responder.has_value() && requester.has_value());
    if (!responder || !requester) return;

    int evals = 0;
    auto comp = responder->declare_computation("foo/a", counting_computation(evals, "computed"));
    CHECK(comp.has_value());
    if (!comp) return;
    settle();

    // `**` cannot reach the namespace because `@eval` is verbatim -- but a user who
    // *types* the prefix would otherwise match the computation's wire declaration
    // exactly, which would invoke a Computation from an ordinary get(). The namespace
    // is reserved, so naming it through the ordinary API is refused outright.
    auto getter = requester->get("@eval/foo/a");
    CHECK(!getter.has_value());
    CHECK(!getter.has_value() && getter.error() == ZError::invalid_key_expr);
    CHECK(evals == 0);

    // The same door, from the other side: an ordinary Queryable must not be able to
    // register itself inside the namespace an eval routes through.
    auto qbl = responder->declare_queryable("@eval/foo/a");
    CHECK(!qbl.has_value());
    CHECK(!qbl.has_value() && qbl.error() == ZError::invalid_key_expr);

    // And the Evaluation API refuses to nest the namespace inside itself.
    auto nested_comp = responder->declare_computation("@eval/foo/a");
    CHECK(!nested_comp.has_value());
    auto nested_eval = requester->eval("@eval/foo/a", bytes("arg"), test_opts());
    CHECK(!nested_eval.has_value());
    auto nested_evaluator = requester->declare_evaluator("@eval/**");
    CHECK(!nested_evaluator.has_value());
}

TEST("eval rejects a key expression that is not canonical") {
    auto tb = TestBroker::start();
    if (!tb.broker) return;

    auto session = Session::open(tb.endpoint());
    CHECK(session.has_value());
    if (!session) return;

    for (auto const* bad : {"", "foo//bar", "a/**/**/b"}) {
        auto getter = session->eval(bad, bytes("arg"), test_opts());
        CHECK(!getter.has_value());
        CHECK(!getter.has_value() && getter.error() == ZError::invalid_key_expr);
        auto evaluator = session->declare_evaluator(bad);
        CHECK(!evaluator.has_value());
        CHECK(!evaluator.has_value() && evaluator.error() == ZError::invalid_key_expr);
    }
}

// --- The Eval object -------------------------------------------------------------

TEST("an Eval carries the argument, the evaluator's key expression and its own key") {
    auto tb = TestBroker::start();
    if (!tb.broker) return;

    auto responder = Session::open(tb.endpoint());
    auto requester = Session::open(tb.endpoint());
    CHECK(responder.has_value() && requester.has_value());
    if (!responder || !requester) return;

    std::string seen_argument;
    std::string seen_key_expr;
    std::string seen_computation_key;
    int evals = 0;
    auto comp = responder->declare_computation("robot/r1/reset", [&](Eval e) {
        ++evals;
        seen_argument = str(e.argument());
        seen_key_expr = std::string(e.key_expr());
        seen_computation_key = std::string(e.computation_key());
        (void)e.reply(bytes("ok"));
    });
    CHECK(comp.has_value());
    if (!comp) return;
    CHECK(comp->key() == "robot/r1/reset");
    settle();

    auto getter = requester->eval("robot/*/reset", bytes("hard"), test_opts());
    CHECK(getter.has_value());
    if (!getter) return;
    pump_until(*responder, [&] { return evals > 0; });
    auto replies = collect(*getter);

    CHECK(evals == 1);
    CHECK(seen_argument == "hard");
    // The logical key expression the evaluator used, and this computation's own
    // concrete key -- never the internal `@eval/...` wire form, on either side.
    CHECK(seen_key_expr == "robot/*/reset");
    CHECK(seen_computation_key == "robot/r1/reset");
    CHECK(replies.completed);
    CHECK(replies.ok.size() == 1);
    CHECK(replies.ok.size() == 1 && replies.ok[0].first == "robot/r1/reset");
}

TEST("an empty argument is delivered as an empty argument") {
    auto tb = TestBroker::start();
    if (!tb.broker) return;

    auto responder = Session::open(tb.endpoint());
    auto requester = Session::open(tb.endpoint());
    CHECK(responder.has_value() && requester.has_value());
    if (!responder || !requester) return;

    bool empty_argument = false;
    int evals = 0;
    auto comp = responder->declare_computation("foo/a", [&](Eval e) {
        ++evals;
        empty_argument = e.argument().empty();
        (void)e.reply(bytes("ok"));
    });
    CHECK(comp.has_value());
    if (!comp) return;
    settle();

    auto getter = requester->eval("foo/a", {}, test_opts());
    CHECK(getter.has_value());
    if (!getter) return;
    pump_until(*responder, [&] { return evals > 0; });
    auto replies = collect(*getter);

    CHECK(evals == 1);
    CHECK(empty_argument);
    CHECK(replies.completed);
}

TEST("a Computation may send several replies and error replies, none consolidated") {
    auto tb = TestBroker::start();
    if (!tb.broker) return;

    auto responder = Session::open(tb.endpoint());
    auto requester = Session::open(tb.endpoint());
    CHECK(responder.has_value() && requester.has_value());
    if (!responder || !requester) return;

    int evals = 0;
    auto comp = responder->declare_computation("foo/a", [&](Eval e) {
        ++evals;
        (void)e.reply(bytes("first"));
        (void)e.reply(bytes("second"));
        (void)e.reply_err(bytes("nope"));
    });
    CHECK(comp.has_value());
    if (!comp) return;
    settle();

    auto getter = requester->eval("foo/a", bytes("arg"), test_opts());
    CHECK(getter.has_value());
    if (!getter) return;
    pump_until(*responder, [&] { return evals > 0; });
    auto replies = collect(*getter);

    CHECK(evals == 1);
    CHECK(replies.completed);
    // Both ok replies survive on the same key (ConsolidationMode::none), and the
    // error reply arrives alongside them.
    CHECK(replies.ok.size() == 2);
    CHECK(replies.ok.size() == 2 && replies.ok[0].second == "first");
    CHECK(replies.ok.size() == 2 && replies.ok[1].second == "second");
    CHECK(replies.errors == 1);
}

TEST("a pull-based Computation delivers evals through recv()") {
    auto tb = TestBroker::start();
    if (!tb.broker) return;

    auto responder = Session::open(tb.endpoint());
    auto requester = Session::open(tb.endpoint());
    CHECK(responder.has_value() && requester.has_value());
    if (!responder || !requester) return;

    auto comp = responder->declare_computation("foo/a");
    CHECK(comp.has_value());
    if (!comp) return;
    settle();

    auto getter = requester->eval("foo/a", bytes("arg"), test_opts());
    CHECK(getter.has_value());
    if (!getter) return;

    auto e = comp->recv();
    CHECK(e.has_value());
    if (!e) return;
    CHECK(str(e->argument()) == "arg");
    CHECK(e->computation_key() == "foo/a");
    CHECK(e->reply(bytes("pulled")).has_value());
    // Release the eval so its request is finalized -- the evaluator's reply stream
    // closes when the last Eval of that request is done, not when it replies.
    {
        auto finished = std::move(*e);
    }

    auto replies = collect(*getter);
    CHECK(replies.completed);
    CHECK(replies.ok.size() == 1);
    CHECK(replies.ok.size() == 1 && replies.ok[0].second == "pulled");
}

TEST("undeclaring a Computation with an eval still queued finalizes it") {
    auto tb = TestBroker::start();
    if (!tb.broker) return;

    auto responder = Session::open(tb.endpoint());
    auto requester = Session::open(tb.endpoint());
    CHECK(responder.has_value() && requester.has_value());
    if (!responder || !requester) return;

    auto comp = responder->declare_computation("foo/a"); // pull-based: nothing drains it
    CHECK(comp.has_value());
    if (!comp) return;
    settle();

    auto getter = requester->eval("foo/a", bytes("arg"), test_opts());
    CHECK(getter.has_value());
    if (!getter) return;
    // One pump parks the eval in the computation's strand without delivering it.
    (void)responder->run_once();
    comp->undeclare();

    // The evaluator must not be left waiting out its whole timeout for replies that
    // are never coming: undeclaring releases the queued eval's share of the request.
    auto replies = collect(*getter);
    CHECK(replies.completed);
    CHECK(replies.ok.empty());
}

// --- Declared Evaluator ----------------------------------------------------------

TEST("a declared Evaluator evaluates through its declared key-expression id") {
    auto tb = TestBroker::start();
    if (!tb.broker) return;

    auto responder = Session::open(tb.endpoint());
    auto requester = Session::open(tb.endpoint());
    CHECK(responder.has_value() && requester.has_value());
    if (!responder || !requester) return;

    int runs_a = 0;
    int runs_b = 0;
    auto ca = responder->declare_computation("math/square", counting_computation(runs_a, "16"));
    auto cb = responder->declare_computation("math/cube", counting_computation(runs_b, "64"));
    CHECK(ca.has_value() && cb.has_value());
    if (!ca || !cb) return;

    auto evaluator = requester->declare_evaluator("math/*", test_opts());
    CHECK(evaluator.has_value());
    if (!evaluator) return;
    CHECK(evaluator->key_expr() == "math/*"); // the logical key expression, not @eval/...
    CHECK(evaluator->keyexpr_id() != 0);      // ... which travels as an id, like a Publisher's
    settle();

    auto getter = evaluator->eval(bytes("4"));
    CHECK(getter.has_value());
    if (!getter) return;
    pump_until(*responder, [&] { return runs_a > 0 && runs_b > 0; });
    auto replies = collect(*getter);

    CHECK(runs_a == 1);
    CHECK(runs_b == 1);
    CHECK(replies.completed);
    CHECK(replies.ok.size() == 2);
    CHECK(replies.count_key("math/square") == 1);
    CHECK(replies.count_key("math/cube") == 1);

    // A declared evaluator is reusable: the second eval behaves exactly like the first.
    auto again = evaluator->eval(bytes("5"));
    CHECK(again.has_value());
    if (!again) return;
    pump_until(*responder, [&] { return runs_a > 1 && runs_b > 1; });
    auto second = collect(*again);
    CHECK(runs_a == 2);
    CHECK(runs_b == 2);
    CHECK(second.completed);
    CHECK(second.ok.size() == 2);
}

TEST("an Evaluator delivers replies to a callback through run_once()") {
    auto tb = TestBroker::start();
    if (!tb.broker) return;

    auto responder = Session::open(tb.endpoint());
    auto requester = Session::open(tb.endpoint());
    CHECK(responder.has_value() && requester.has_value());
    if (!responder || !requester) return;

    int runs = 0;
    auto comp = responder->declare_computation("foo/a", counting_computation(runs, "done"));
    CHECK(comp.has_value());
    if (!comp) return;

    auto evaluator = requester->declare_evaluator("foo/*", test_opts());
    CHECK(evaluator.has_value());
    if (!evaluator) return;
    settle();

    std::vector<std::string> keys;
    CHECK(evaluator
              ->eval(bytes("arg"),
                     [&](const GetReply& r) {
                         if (r.is_ok()) keys.emplace_back(r.sample().key_expr());
                     })
              .has_value());
    pump_until(*responder, [&] { return runs > 0; });
    pump_until(*requester, [&] { return !keys.empty(); });

    CHECK(runs == 1);
    CHECK(keys.size() == 1);
    CHECK(keys.size() == 1 && keys[0] == "foo/a");
}

TEST("Session::eval delivers replies to a callback through run_once()") {
    auto tb = TestBroker::start();
    if (!tb.broker) return;

    auto responder = Session::open(tb.endpoint());
    auto requester = Session::open(tb.endpoint());
    CHECK(responder.has_value() && requester.has_value());
    if (!responder || !requester) return;

    int runs = 0;
    auto comp = responder->declare_computation("foo/a", counting_computation(runs, "done"));
    CHECK(comp.has_value());
    if (!comp) return;
    settle();

    std::vector<std::string> payloads;
    CHECK(requester
              ->eval(
                  "foo/*", bytes("arg"),
                  [&](const GetReply& r) {
                      if (r.is_ok()) payloads.push_back(str(r.sample().payload()));
                  },
                  test_opts())
              .has_value());
    pump_until(*responder, [&] { return runs > 0; });
    pump_until(*requester, [&] { return !payloads.empty(); });

    CHECK(runs == 1);
    CHECK(payloads.size() == 1);
    CHECK(payloads.size() == 1 && payloads[0] == "done");
}

TEST("a reply handler may start the next evaluation from inside itself") {
    auto tb = TestBroker::start();
    if (!tb.broker) return;

    auto responder = Session::open(tb.endpoint());
    auto requester = Session::open(tb.endpoint());
    CHECK(responder.has_value() && requester.has_value());
    if (!responder || !requester) return;

    int runs = 0;
    auto comp = responder->declare_computation("foo/a", counting_computation(runs, "done"));
    CHECK(comp.has_value());
    if (!comp) return;
    settle();

    // "On each reply, evaluate the next step" is the idiom this API invites, and it
    // means re-entering the session from inside a reply handler that the session is
    // itself iterating. The second eval inserts into the in-flight map -- which is why
    // the delivery loops walk a snapshot of request ids instead of the map (ASan
    // catches the alternative).
    int replies = 0;
    int chained_replies = 0;
    Session& session = *requester;
    CHECK(session
              .eval(
                  "foo/*", bytes("first"),
                  [&](const GetReply& r) {
                      if (!r.is_ok()) return;
                      ++replies;
                      (void)session.eval(
                          "foo/*", bytes("second"),
                          [&](const GetReply& inner) {
                              if (inner.is_ok()) ++chained_replies;
                          },
                          test_opts());
                  },
                  test_opts())
              .has_value());

    pump_until(*responder, [&] { return runs > 0; });
    pump_until(*requester, [&] { return replies > 0; });
    pump_until(*responder, [&] { return runs > 1; });
    pump_until(*requester, [&] { return chained_replies > 0; });

    CHECK(runs == 2);
    CHECK(replies == 1);
    CHECK(chained_replies == 1);
}

TEST("evaluating a key expression nobody registered completes without replies") {
    auto tb = TestBroker::start();
    if (!tb.broker) return;

    auto requester = Session::open(tb.endpoint());
    CHECK(requester.has_value());
    if (!requester) return;

    auto getter = requester->eval("nobody/home", bytes("arg"), test_opts());
    CHECK(getter.has_value());
    if (!getter) return;
    auto replies = collect(*getter);

    CHECK(replies.completed);
    CHECK(replies.ok.empty());
    CHECK(replies.errors == 0);
}

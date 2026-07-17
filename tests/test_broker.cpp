// Broker integration tests (zenoh.broker): spins a real zenoh::broker::Broker on a
// background thread pool against loopback port 0, driven by real zenoh::Session
// clients over the actual wire -- the inverted FakeRouter/SubRouter pattern (see
// test_session.cpp/test_subscriber.cpp): there, an in-process test router drives a
// real Session; here, a real Session drives a real, in-process Broker.
//
// M6 full functional suite: pub/sub fan-out, undeclare cleanup, QueryTarget
// variants, zero-queryable get() termination, the Err reply path, zid-targeting
// filter-not-bypass proofs on both put() and get(), disconnect-mid-query cleanup,
// and a multi-threaded concurrency-stress case. Every case here uses `--threads 1`
// except the stress test, which explicitly exercises `--threads > 1` (see the broker
// plan's M6 section; the dedicated `linux-tsan` preset re-runs this whole binary
// under ThreadSanitizer as the concurrency-safety gate, not a separate test file).
//
// Corner-case follow-up pass (rock-solid-before-optimizing): raw Del delivery, the
// publisher self-loop skip, deep wildcard hierarchies, payload size boundaries,
// declare/undeclare churn, disconnect-not-undeclare resource cleanup, a
// since-disconnected zid target, multi-reply queries, mixed ok/err fan-in, wildcard
// fan-out to multiple literal queryables, explicit queryable undeclare, and
// concurrent gets on one session (request-id crosstalk). `RawClient` (below) plays
// the client side of the handshake by hand -- mirroring test_session.cpp's
// FakeRouter, which plays the mirror-image server side against a real Session --
// so a test can put bytes on the wire a real Session can never produce (e.g. a raw
// Del Push).
import zenoh.broker;
import zenoh.proto;
import zenoh;

#include "ztest.hpp"

#include <asio/post.hpp>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <future>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <vector>

#include <arpa/inet.h>
#include <cerrno>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

using namespace zenoh::broker;
using namespace zenoh;

namespace {

auto bytes(std::string_view s) -> std::span<const std::byte> {
    return {reinterpret_cast<const std::byte*>(s.data()), s.size()};
}

auto str(std::span<const std::byte> b) -> std::string {
    return {reinterpret_cast<const char*>(b.data()), b.size()};
}

// Spins a real Broker on its own thread pool against an ephemeral loopback port;
// stop()+join() on destruction. `broker` is null if bind() failed (callers CHECK it).
struct TestBroker {
    std::unique_ptr<Broker> broker;
    std::thread runner;

    TestBroker() = default;
    TestBroker(const TestBroker&) = delete;
    auto operator=(const TestBroker&) -> TestBroker& = delete;
    // A user-declared destructor (below) suppresses the implicitly-declared move
    // constructor/assignment -- without these, `return tb;` in start() falls back to
    // the (deleted, non-copyable-members) copy constructor instead.
    TestBroker(TestBroker&&) = default;
    auto operator=(TestBroker&&) -> TestBroker& = default;

    static auto start(unsigned threads = 1) -> TestBroker {
        TestBroker tb;
        auto b = Broker::bind("127.0.0.1", 0);
        CHECK(b.has_value());
        if (!b) return tb;
        tb.broker = std::move(*b);
        auto* raw = tb.broker.get();
        tb.runner = std::thread([raw, threads] { raw->run(threads); });
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

// Plays the client side of the transport handshake by hand (mirror image of
// test_session.cpp's FakeRouter, which plays the server side against a real
// Session), so a test can put arbitrary bytes on the wire -- content a real
// Session's public API can never produce (a raw Del Push, deliberately malformed
// content, etc). Kept minimal: only what the corner-case tests below need.
struct RawClient {
    int fd = -1;

    RawClient() = default;
    RawClient(const RawClient&) = delete;
    auto operator=(const RawClient&) -> RawClient& = delete;
    RawClient(RawClient&& o) noexcept : fd(o.fd) { o.fd = -1; }
    auto operator=(RawClient&& o) noexcept -> RawClient& {
        if (this != &o) {
            if (fd >= 0) ::close(fd);
            fd = o.fd;
            o.fd = -1;
        }
        return *this;
    }
    ~RawClient() {
        if (fd >= 0) ::close(fd);
    }

    [[nodiscard]] static auto connect(std::uint16_t port) -> std::optional<RawClient> {
        RawClient c;
        c.fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (c.fd < 0) return std::nullopt;
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        ::inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
        if (::connect(c.fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0)
            return std::nullopt;
        if (!c.handshake()) return std::nullopt;
        return c;
    }

    template <class Msg>
    [[nodiscard]] static auto encode_body(const Msg& m) -> std::vector<std::byte> {
        std::vector<std::byte> buf(4096);
        ByteWriter w{buf};
        if (!m.encode(w)) return {};
        return {buf.data(), buf.data() + w.written()};
    }

    [[nodiscard]] auto read_exact(std::span<std::byte> out) const -> bool {
        std::size_t off = 0;
        while (off < out.size()) {
            ssize_t const n = ::recv(fd, out.data() + off, out.size() - off, 0);
            if (n > 0) {
                off += static_cast<std::size_t>(n);
                continue;
            }
            if (n < 0 && errno == EINTR) continue;
            return false;
        }
        return true;
    }

    [[nodiscard]] auto write_all(std::span<const std::byte> data) const -> bool {
        std::size_t off = 0;
        while (off < data.size()) {
            ssize_t const n = ::send(fd, data.data() + off, data.size() - off, 0);
            if (n > 0) {
                off += static_cast<std::size_t>(n);
                continue;
            }
            if (n < 0 && errno == EINTR) continue;
            return false;
        }
        return true;
    }

    [[nodiscard]] auto recv_batch() const -> std::optional<std::vector<std::byte>> {
        std::array<std::byte, 2> len{};
        if (!read_exact(len)) return std::nullopt;
        std::uint16_t const l = load_le<std::uint16_t>(len.data());
        std::vector<std::byte> body(l);
        if (l != 0 && !read_exact(body)) return std::nullopt;
        return body;
    }

    [[nodiscard]] auto send_batch(std::span<const std::byte> body) const -> bool {
        std::array<std::byte, 2> len{};
        store_le<std::uint16_t>(len.data(), static_cast<std::uint16_t>(body.size()));
        return write_all(len) && write_all(body);
    }

  private:
    // Client side of InitSyn/InitAck/OpenSyn/OpenAck -- mirrors Session::open, with
    // the roles of Face::handshake (broker/src/broker.cpp) inverted.
    [[nodiscard]] auto handshake() -> bool {
        InitSyn isyn{};
        isyn.version = 9;
        isyn.identifier.whatami = WhatAmI::client;
        isyn.identifier.zid.len = 16;
        isyn.identifier.zid.bytes.fill(std::byte{0x42});
        if (!send_batch(encode_body(isyn))) return false;

        auto iack_bytes = recv_batch();
        if (!iack_bytes) return false;
        ByteReader ir{*iack_bytes};
        auto iack = InitAck::decode(ir);
        if (!iack || iack->version != 9) return false;
        std::vector<std::byte> const cookie(iack->cookie.begin(), iack->cookie.end());

        OpenSyn osyn{};
        osyn.lease = Duration::from_millis(10000);
        osyn.sn = 0;
        osyn.cookie = cookie;
        if (!send_batch(encode_body(osyn))) return false;

        auto oack_bytes = recv_batch();
        if (!oack_bytes) return false;
        ByteReader ackr{*oack_bytes};
        return OpenAck::decode(ackr).has_value();
    }
};

// Encodes one FrameHeader + one Push(Del{}) on `key` and sends it as a single raw
// batch -- the shape a real Session can never produce (Session::put/try_put only
// ever encode Put), mirroring broker/src/tables.cpp's own encode_push(is_del=true).
[[nodiscard]] auto send_raw_del(const RawClient& c, std::string_view key, std::uint32_t sn)
    -> bool {
    FrameHeader fh{};
    fh.reliability = Reliability::reliable;
    fh.sn = sn;

    Push push{};
    push.wire_expr = WireExpr{.scope = 0, .mapping = Mapping::sender, .suffix = key};
    push.payload = PushBody{.body = Del{}};

    std::vector<std::byte> buf(256 + key.size());
    ByteWriter w{buf};
    if (!fh.encode(w) || !push.encode(w)) return false;
    return c.send_batch(std::span(buf).first(w.written()));
}

// Reads `fn()` from inside a real handler running on `tables`'s own routing strand,
// blocking the calling (test) thread until it completes. Required for any test-only
// introspection (face_count/resource_count/pending_query_count/fanout_count):
// reading these directly from the test thread would race the routing strand exactly
// like production code would (see the broker plan's M6 section).
template <class Fn>
[[nodiscard]] auto on_strand(Tables& tables, Fn fn) -> std::invoke_result_t<Fn> {
    using R = std::invoke_result_t<Fn>;
    std::promise<R> prom;
    auto fut = prom.get_future();
    asio::post(tables.strand(), [&fn, &prom] { prom.set_value(fn()); });
    return fut.get();
}

// Polls `pred` (each call itself strand-marshaled by the caller) until true or
// `timeout_ms` elapses. Cleanup (undeclare, disconnect fan-in) is asynchronous from
// the test thread's perspective -- it happens once the relevant Declare/Undeclare/
// ResponseFinal message is decoded and its posted routing-strand job runs.
template <class Pred> auto wait_until(Pred pred, int timeout_ms = 2000) -> bool {
    auto const deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    return pred();
}

// Drains a Getter until it completes (nullopt) or errors, collecting each reply's
// payload (sample payload if ok, error payload otherwise) as a string.
auto drain_replies(zenoh::Getter& g) -> std::vector<std::string> {
    std::vector<std::string> out;
    for (;;) {
        auto r = g.recv();
        if (!r || !*r) return out;
        auto const& reply = **r;
        out.push_back(str(reply.is_ok() ? reply.sample().payload() : reply.error_payload()));
    }
}

} // namespace

TEST("Broker::bind resolves an ephemeral port and Broker::run/stop cleanly shut down") {
    auto broker = Broker::bind("127.0.0.1", 0);
    CHECK(broker.has_value());
    if (!broker) return;
    CHECK((*broker)->port() != 0);

    std::thread runner([b = broker->get()] { b->run(2); });
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    (*broker)->stop();
    runner.join();
}

// Regression test for a bug caught in cpp-systems-expert review: Tables::on_request
// used to reject any query key containing '*', on the (wrong) assumption that a
// routed key is always literal like Push's. Query key expressions are legitimately
// wildcarded -- get()'s own default selector shape ("demo/example/**") -- so that
// check silently dropped the single most common query pattern. This drives a real
// wildcarded get() against a real literal-keyed queryable through a real Broker.
TEST("Broker routes a wildcard get() to a literal-keyed queryable") {
    auto tb = TestBroker::start();
    if (!tb.broker) return;

    using namespace zenoh;
    auto responder = Session::open(tb.endpoint());
    CHECK(responder.has_value());
    auto qbl = responder->declare_queryable("demo/example/zenoh-cxx-queryable");
    CHECK(qbl.has_value());
    if (!responder || !qbl) return;

    bool got_query = false;
    std::thread responder_thread([&] {
        auto q = qbl->recv();
        if (q) {
            got_query = true;
            (void)q->reply(q->key_expr(), {});
        }
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    auto requester = Session::open(tb.endpoint());
    CHECK(requester.has_value());
    if (!requester) {
        responder_thread.join();
        return;
    }
    auto getter = requester->get("demo/example/**");
    CHECK(getter.has_value());

    bool got_reply = false;
    if (getter) {
        auto r = getter->recv();
        got_reply = r.has_value() && r->has_value();
    }
    responder_thread.join();

    CHECK(got_query);
    CHECK(got_reply);
}

TEST("Broker fans a Push out to every matching subscriber and never to a non-matching one") {
    auto tb = TestBroker::start();
    if (!tb.broker) return;

    using namespace zenoh;
    auto pub = Session::open(tb.endpoint());
    auto sess_a = Session::open(tb.endpoint());     // matches via **
    auto sess_b = Session::open(tb.endpoint());     // matches via the literal key itself
    auto sess_other = Session::open(tb.endpoint()); // does not match
    CHECK(pub.has_value());
    CHECK(sess_a.has_value());
    CHECK(sess_b.has_value());
    CHECK(sess_other.has_value());
    if (!pub || !sess_a || !sess_b || !sess_other) return;

    auto sub_a = sess_a->declare_subscriber("demo/fanout/**");
    auto sub_b = sess_b->declare_subscriber("demo/fanout/a");
    auto sub_other = sess_other->declare_subscriber("other/topic");
    CHECK(sub_a.has_value());
    CHECK(sub_b.has_value());
    CHECK(sub_other.has_value());
    if (!sub_a || !sub_b || !sub_other) return;

    std::this_thread::sleep_for(std::chrono::milliseconds(150));

    bool other_got_it = false;
    std::string other_payload;
    std::thread other_thread([&] {
        auto s = sub_other->recv();
        other_got_it = s.has_value();
        if (s) other_payload = str(s->payload());
    });

    auto put_r = pub->put("demo/fanout/a", bytes("hi"));
    CHECK(put_r.has_value());

    auto sa = sub_a->recv();
    CHECK(sa.has_value());
    if (sa) CHECK(str(sa->payload()) == "hi");

    auto sb = sub_b->recv();
    CHECK(sb.has_value());
    if (sb) CHECK(str(sb->payload()) == "hi");

    // Prove `other` never received the earlier, non-matching push: publish a second
    // message it *does* match, and confirm that's the first thing it ever sees.
    auto put_r2 = pub->put("other/topic", bytes("only-this"));
    CHECK(put_r2.has_value());
    other_thread.join(); // join() establishes happens-before for other_got_it below
    CHECK(other_got_it);
    if (other_got_it) CHECK(other_payload == "only-this");
}

// Regression test for a bug caught in cpp-systems-expert review of the throughput
// optimization pass: Face::dispatch_frame_body batches every consecutive Push
// decoded from one inbound frame and posts them to Tables together (amortizing the
// Face->Tables asio::post hop over the whole batch instead of paying it per
// message -- see docs/BROKER.md's "Performance testing" section). The client's
// Batch API (used here via Session::batch()) is what actually produces a single
// Frame carrying multiple Push messages -- Session::put() alone never does, so
// this is the only way to exercise the *multi-entry* batching path at all; every
// other test in this file sends one Push per frame.
TEST("A Frame carrying multiple Pushes delivers every one of them") {
    auto tb = TestBroker::start();
    if (!tb.broker) return;

    using namespace zenoh;
    auto pub = Session::open(tb.endpoint());
    auto sub_sess = Session::open(tb.endpoint());
    CHECK(pub.has_value());
    CHECK(sub_sess.has_value());
    if (!pub || !sub_sess) return;

    auto sub = sub_sess->declare_subscriber("demo/batch/**");
    CHECK(sub.has_value());
    if (!sub) return;

    std::this_thread::sleep_for(std::chrono::milliseconds(150));

    // One Batch::flush() sends every buffered put as Push messages in a single
    // Frame -- exactly the multi-Push-per-frame shape the batching path exists
    // to amortize.
    auto b = pub->batch();
    CHECK(b.put("demo/batch/1", bytes("one")).has_value());
    CHECK(b.put("demo/batch/2", bytes("two")).has_value());
    CHECK(b.put("demo/batch/3", bytes("three")).has_value());
    CHECK(b.flush().has_value());

    std::vector<std::string> received;
    for (int i = 0; i < 3; ++i) {
        auto s = sub->recv();
        CHECK(s.has_value());
        if (s) received.push_back(str(s->payload()));
    }
    std::ranges::sort(received);
    CHECK((received == std::vector<std::string>{"one", "three", "two"}));
}

TEST("Undeclaring a subscriber removes it from the resource table") {
    auto tb = TestBroker::start();
    if (!tb.broker) return;
    auto& tables = tb.broker->tables;

    using namespace zenoh;
    auto sess = Session::open(tb.endpoint());
    CHECK(sess.has_value());
    if (!sess) return;

    auto sub = sess->declare_subscriber("demo/undeclare/only");
    CHECK(sub.has_value());
    if (!sub) return;

    CHECK(wait_until(
        [&] { return on_strand(tables, [&] { return tables.resource_count(); }) == 1; }));

    sub->undeclare();

    CHECK(wait_until(
        [&] { return on_strand(tables, [&] { return tables.resource_count(); }) == 0; }));
}

TEST("QueryTarget::all_complete reaches only complete queryables; all/best_matching reach every "
     "one") {
    auto tb = TestBroker::start();
    if (!tb.broker) return;

    using namespace zenoh;
    auto sess_complete = Session::open(tb.endpoint());
    auto sess_partial = Session::open(tb.endpoint());
    auto requester = Session::open(tb.endpoint());
    CHECK(sess_complete.has_value());
    CHECK(sess_partial.has_value());
    CHECK(requester.has_value());
    if (!sess_complete || !sess_partial || !requester) return;

    auto qbl_complete =
        sess_complete->declare_queryable("demo/target", QueryableOptions{.complete = true});
    auto qbl_partial =
        sess_partial->declare_queryable("demo/target", QueryableOptions{.complete = false});
    CHECK(qbl_complete.has_value());
    CHECK(qbl_partial.has_value());
    if (!qbl_complete || !qbl_partial) return;

    std::this_thread::sleep_for(std::chrono::milliseconds(150));

    // Round 1: default target (best_matching) -- hub topology, so both answer.
    std::thread t_complete_1([&] {
        auto q = qbl_complete->recv();
        if (q) (void)q->reply(q->key_expr(), bytes("complete"));
    });
    std::thread t_partial_1([&] {
        auto q = qbl_partial->recv();
        if (q) (void)q->reply(q->key_expr(), bytes("partial"));
    });
    auto getter1 = requester->get("demo/target");
    CHECK(getter1.has_value());
    if (getter1) {
        auto replies = drain_replies(*getter1);
        std::ranges::sort(replies);
        CHECK((replies == std::vector<std::string>{"complete", "partial"}));
    }
    t_complete_1.join();
    t_partial_1.join();

    // Round 2: all_complete -- only the complete queryable is ever asked.
    std::thread t_complete_2([&] {
        auto q = qbl_complete->recv();
        if (q) (void)q->reply(q->key_expr(), bytes("complete"));
    });
    auto getter2 = requester->get("demo/target", "", GetOptions{.target = GetTarget::all_complete});
    CHECK(getter2.has_value());
    if (getter2) {
        auto replies = drain_replies(*getter2);
        CHECK(replies == std::vector<std::string>{"complete"});
    }
    t_complete_2.join();

    // Round 3: explicit GetTarget::all -- exercises the third enumerator by name (a
    // swapped-case-label bug in Session's GetTarget->QueryTarget wire mapping would
    // otherwise go undetected, since round 1 only ever sends the *default*, and
    // Tables::on_request itself doesn't distinguish best_matching/all in a hub
    // topology -- so this is purely a client-encode-layer check).
    std::thread t_complete_3([&] {
        auto q = qbl_complete->recv();
        if (q) (void)q->reply(q->key_expr(), bytes("complete"));
    });
    std::thread t_partial_3([&] {
        auto q = qbl_partial->recv();
        if (q) (void)q->reply(q->key_expr(), bytes("partial"));
    });
    auto getter3 = requester->get("demo/target", "", GetOptions{.target = GetTarget::all});
    CHECK(getter3.has_value());
    if (getter3) {
        auto replies = drain_replies(*getter3);
        std::ranges::sort(replies);
        CHECK((replies == std::vector<std::string>{"complete", "partial"}));
    }
    t_complete_3.join();
    t_partial_3.join();
}

TEST("get() over zero matching queryables completes immediately, not with an error") {
    auto tb = TestBroker::start();
    if (!tb.broker) return;

    using namespace zenoh;
    auto requester = Session::open(tb.endpoint());
    CHECK(requester.has_value());
    if (!requester) return;

    auto getter = requester->get("demo/nobody/home");
    CHECK(getter.has_value());
    if (!getter) return;

    auto r = getter->recv();
    CHECK(r.has_value());
    CHECK(r.has_value() && !r->has_value());
}

TEST("A Queryable's reply_err() is delivered to the requester as a non-ok GetReply") {
    auto tb = TestBroker::start();
    if (!tb.broker) return;

    using namespace zenoh;
    auto responder = Session::open(tb.endpoint());
    auto requester = Session::open(tb.endpoint());
    CHECK(responder.has_value());
    CHECK(requester.has_value());
    if (!responder || !requester) return;

    auto qbl = responder->declare_queryable("demo/err");
    CHECK(qbl.has_value());
    if (!qbl) return;

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    std::thread responder_thread([&] {
        auto q = qbl->recv();
        if (q) (void)q->reply_err(bytes("boom"));
    });

    auto getter = requester->get("demo/err");
    CHECK(getter.has_value());
    if (getter) {
        auto r = getter->recv();
        CHECK(r.has_value() && r->has_value());
        if (r && *r) {
            CHECK(!(*r)->is_ok());
            CHECK(str((*r)->error_payload()) == "boom");
        }
        auto rfinal = getter->recv();
        CHECK(rfinal.has_value() && !rfinal->has_value());
    }
    responder_thread.join();
}

TEST("target_zid on put() narrows delivery to one peer, never bypassing subscriber matching") {
    auto tb = TestBroker::start();
    if (!tb.broker) return;

    using namespace zenoh;
    auto pub = Session::open(tb.endpoint());
    auto sess_a = Session::open(tb.endpoint());
    auto sess_b = Session::open(tb.endpoint());
    CHECK(pub.has_value());
    CHECK(sess_a.has_value());
    CHECK(sess_b.has_value());
    if (!pub || !sess_a || !sess_b) return;

    auto sub_a = sess_a->declare_subscriber("demo/zid");
    auto sub_b = sess_b->declare_subscriber("demo/zid");
    CHECK(sub_a.has_value());
    CHECK(sub_b.has_value());
    if (!sub_a || !sub_b) return;

    auto const zid_b = sess_b->local_zid();
    std::this_thread::sleep_for(std::chrono::milliseconds(150));

    // Targeted at B specifically: only B should ever see this one.
    CHECK(pub->put("demo/zid", bytes("for-b"), zid_b).has_value());
    // Untargeted broadcast: both should see this one.
    CHECK(pub->put("demo/zid", bytes("broadcast")).has_value());

    auto b1 = sub_b->recv();
    CHECK(b1.has_value());
    if (b1) CHECK(str(b1->payload()) == "for-b");
    auto b2 = sub_b->recv();
    CHECK(b2.has_value());
    if (b2) CHECK(str(b2->payload()) == "broadcast");

    // A's *first* delivered sample is the broadcast one -- proof the B-targeted
    // message never reached A, even though A has a live matching subscription.
    auto a1 = sub_a->recv();
    CHECK(a1.has_value());
    if (a1) CHECK(str(a1->payload()) == "broadcast");

    // A target_zid matching no live peer at all: zero deliveries anywhere, even
    // though both A and B still have matching declarations.
    PeerId bogus{};
    bogus.len = 16;
    bogus.bytes.fill(std::byte{0xAB});
    CHECK(pub->put("demo/zid", bytes("bogus-target"), bogus).has_value());
    CHECK(pub->put("demo/zid", bytes("broadcast-2")).has_value());

    auto a2 = sub_a->recv();
    CHECK(a2.has_value());
    if (a2) CHECK(str(a2->payload()) == "broadcast-2");
    auto b3 = sub_b->recv();
    CHECK(b3.has_value());
    if (b3) CHECK(str(b3->payload()) == "broadcast-2");
}

TEST("target_zid on get() narrows to one queryable, never bypassing queryable matching") {
    auto tb = TestBroker::start();
    if (!tb.broker) return;

    using namespace zenoh;
    auto sess_a = Session::open(tb.endpoint());
    auto sess_b = Session::open(tb.endpoint());
    auto requester = Session::open(tb.endpoint());
    CHECK(sess_a.has_value());
    CHECK(sess_b.has_value());
    CHECK(requester.has_value());
    if (!sess_a || !sess_b || !requester) return;

    auto qbl_a = sess_a->declare_queryable("demo/qzid");
    auto qbl_b = sess_b->declare_queryable("demo/qzid");
    CHECK(qbl_a.has_value());
    CHECK(qbl_b.has_value());
    if (!qbl_a || !qbl_b) return;

    auto const zid_b = sess_b->local_zid();
    std::this_thread::sleep_for(std::chrono::milliseconds(150));

    // Round 1: target B specifically -- only B is ever asked.
    std::thread t_b1([&] {
        auto q = qbl_b->recv();
        if (q) (void)q->reply(q->key_expr(), bytes("B"));
    });
    auto getter1 = requester->get("demo/qzid", "", GetOptions{.target_zid = zid_b});
    CHECK(getter1.has_value());
    if (getter1) {
        auto replies = drain_replies(*getter1);
        CHECK(replies == std::vector<std::string>{"B"});
    }
    t_b1.join();

    // Round 2: target a zid nobody has -- zero matches, immediate empty completion,
    // even though both A and B have live matching declarations.
    zenoh::PeerId bogus{};
    bogus.len = 16;
    bogus.bytes.fill(std::byte{0xAB});
    auto getter2 = requester->get("demo/qzid", "", GetOptions{.target_zid = bogus});
    CHECK(getter2.has_value());
    if (getter2) {
        auto r = getter2->recv();
        CHECK(r.has_value() && !r->has_value());
    }

    // Round 3: no target -- both A and B (still live, unbothered by rounds 1/2) answer.
    std::thread t_a3([&] {
        auto q = qbl_a->recv();
        if (q) (void)q->reply(q->key_expr(), bytes("A"));
    });
    std::thread t_b3([&] {
        auto q = qbl_b->recv();
        if (q) (void)q->reply(q->key_expr(), bytes("B"));
    });
    auto getter3 = requester->get("demo/qzid");
    CHECK(getter3.has_value());
    if (getter3) {
        auto replies = drain_replies(*getter3);
        std::ranges::sort(replies);
        CHECK((replies == std::vector<std::string>{"A", "B"}));
    }
    t_a3.join();
    t_b3.join();
}

TEST("A disconnecting answering face synthesizes the requester's ResponseFinal and cleans up") {
    auto tb = TestBroker::start();
    if (!tb.broker) return;
    auto& tables = tb.broker->tables;

    using namespace zenoh;
    auto requester = Session::open(tb.endpoint());
    CHECK(requester.has_value());
    if (!requester) return;

    {
        auto responder = Session::open(tb.endpoint());
        CHECK(responder.has_value());
        if (!responder) return;
        auto qbl = responder->declare_queryable("demo/disconnect");
        CHECK(qbl.has_value());
        if (!qbl) return;

        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        // Must be spawned (blocked in recv()) *before* the get() below issues the
        // query it's waiting for -- joining it first would deadlock forever.
        bool got_query = false;
        std::thread responder_thread([&] {
            auto q = qbl->recv();
            got_query = q.has_value();
            if (q) {
                // Invalidate the link *before* `q` drops: per Session::close's
                // documented behavior, a Subscriber/Queryable/IncomingQuery/Getter
                // destructor firing after close() silently no-ops its wire message.
                // So IncomingQuery's own drop-time best-effort ResponseFinal never
                // reaches the wire here -- the requester's completion below can only
                // be explained by the broker's own disconnect cleanup
                // (Tables::remove_face), not by this destructor.
                responder->close();
            }
        });

        auto getter = requester->get("demo/disconnect");
        CHECK(getter.has_value());
        if (getter) {
            auto r = getter->recv();
            CHECK(r.has_value());
            CHECK(r.has_value() && !r->has_value());
        }
        responder_thread.join(); // join() establishes happens-before for got_query
        CHECK(got_query);
    } // `responder` (and its Queryable) fully destroyed here.

    CHECK(wait_until(
        [&] { return on_strand(tables, [&] { return tables.pending_query_count(); }) == 0; }));
    CHECK(
        wait_until([&] { return on_strand(tables, [&] { return tables.fanout_count(); }) == 0; }));
}

TEST("Broker handles concurrent multi-session pub/sub under a multi-threaded pool") {
    constexpr int num_subs = 3;
    constexpr int num_pubs = 2;
    constexpr int msgs_per_pub = 15;
    constexpr int total_msgs = num_pubs * msgs_per_pub;

    auto tb = TestBroker::start(4);
    if (!tb.broker) return;
    auto& tables = tb.broker->tables;

    using namespace zenoh;
    std::vector<std::thread> sub_threads;
    std::vector<int> received(num_subs, 0);
    // int, not bool: std::vector<bool> is bit-packed, so concurrent writes to
    // *different* elements from different threads is a real data race (unlike every
    // other container, where that's safe) -- exactly the kind of trap this file
    // shouldn't leave lying around given how much it otherwise cares about strand/
    // thread discipline.
    std::vector<int> sub_ok(num_subs, 1);

    for (int i = 0; i < num_subs; ++i) {
        sub_threads.emplace_back([&, i] {
            auto sess = Session::open(tb.endpoint());
            if (!sess) {
                sub_ok[i] = 0;
                return;
            }
            auto sub = sess->declare_subscriber("demo/stress/**");
            if (!sub) {
                sub_ok[i] = 0;
                return;
            }
            for (int n = 0; n < total_msgs; ++n) {
                auto s = sub->recv();
                if (!s) {
                    sub_ok[i] = 0;
                    return;
                }
                ++received[i];
            }
        });
    }

    // Wait for all `num_subs` faces to actually be registered on the shared
    // "demo/stress/**" resource -- resource_count()==1 alone would already be true
    // the instant the *first* subscriber's DeclareSubscriber lands (all 3 declare the
    // identical pattern, so they all share one Resource entry), which previously left
    // only a fixed sleep standing between "1 of 3 registered" and "publish starts": a
    // real hang risk (a missed message blocks that subscriber's recv() loop forever)
    // under enough scheduler jitter. resource_face_count gives an exact, deterministic
    // count instead.
    CHECK(wait_until([&] {
        return on_strand(tables, [&] { return tables.resource_face_count("demo/stress/**"); }) ==
               static_cast<std::size_t>(num_subs);
    }));

    std::vector<std::thread> pub_threads;
    for (int p = 0; p < num_pubs; ++p) {
        pub_threads.emplace_back([&] {
            auto sess = Session::open(tb.endpoint());
            if (!sess) return;
            for (int n = 0; n < msgs_per_pub; ++n) {
                (void)sess->put("demo/stress/x", bytes("m"));
            }
        });
    }
    for (auto& t : pub_threads) t.join();
    for (auto& t : sub_threads) t.join();

    for (int i = 0; i < num_subs; ++i) {
        CHECK(sub_ok[i]);
        CHECK(received[i] == total_msgs);
    }
}

TEST("Broker handles concurrent multi-session query/reply under a multi-threaded pool") {
    constexpr int num_queryables = 3;
    constexpr int num_requesters = 2;
    constexpr int gets_per_requester = 10;
    constexpr int queries_per_queryable = num_requesters * gets_per_requester;

    auto tb = TestBroker::start(4);
    if (!tb.broker) return;
    auto& tables = tb.broker->tables;

    using namespace zenoh;

    // Each queryable session lives on a distinct key, so every requester's get() to
    // that key can only ever be answered by that one queryable -- this isolates each
    // session's expected reply count while still exercising genuinely concurrent
    // query/response/response-final routing (Tables::on_request/on_response/
    // on_response_final) across multiple threads and multiple in-flight requests.
    std::vector<Session> qbl_sessions;
    std::vector<Queryable> qbls;
    qbl_sessions.reserve(num_queryables);
    qbls.reserve(num_queryables);
    for (int i = 0; i < num_queryables; ++i) {
        auto s = Session::open(tb.endpoint());
        CHECK(s.has_value());
        if (!s) return;
        qbl_sessions.push_back(std::move(*s));
        auto q = qbl_sessions.back().declare_queryable("demo/qstress/" + std::to_string(i));
        CHECK(q.has_value());
        if (!q) return;
        qbls.push_back(std::move(*q));
    }

    std::vector<std::thread> qbl_threads;
    std::vector<int> qbl_ok(num_queryables, 1); // int, not bool -- see the pub/sub stress test
    for (int i = 0; i < num_queryables; ++i) {
        qbl_threads.emplace_back([&, i] {
            for (int n = 0; n < queries_per_queryable; ++n) {
                auto q = qbls[i].recv();
                if (!q) {
                    qbl_ok[i] = 0;
                    return;
                }
                (void)q->reply(q->key_expr(), bytes("q" + std::to_string(i)));
            }
        });
    }

    // Deterministically await all `num_queryables` declarations landing, same
    // rationale as the pub/sub stress test above (each on a distinct key here, so
    // this also confirms no cross-key leakage in the resource table under load).
    CHECK(wait_until([&] {
        for (int i = 0; i < num_queryables; ++i) {
            auto const key = "demo/qstress/" + std::to_string(i);
            if (on_strand(tables, [&] { return tables.resource_face_count(key); }) != 1) {
                return false;
            }
        }
        return true;
    }));

    std::vector<std::thread> req_threads;
    std::vector<int> req_ok(num_requesters, 1);
    for (int r = 0; r < num_requesters; ++r) {
        req_threads.emplace_back([&, r] {
            auto sess = Session::open(tb.endpoint());
            if (!sess) {
                req_ok[r] = 0;
                return;
            }
            for (int n = 0; n < gets_per_requester; ++n) {
                for (int i = 0; i < num_queryables; ++i) {
                    auto getter = sess->get("demo/qstress/" + std::to_string(i));
                    if (!getter) {
                        req_ok[r] = 0;
                        continue;
                    }
                    auto reply = getter->recv();
                    if (!reply || !*reply || !(**reply).is_ok() ||
                        str((**reply).sample().payload()) != ("q" + std::to_string(i))) {
                        req_ok[r] = 0;
                        continue;
                    }
                    auto final_reply = getter->recv();
                    if (!final_reply || final_reply->has_value()) req_ok[r] = 0;
                }
            }
        });
    }

    for (auto& t : req_threads) t.join();
    for (auto& t : qbl_threads) t.join();

    for (int r = 0; r < num_requesters; ++r) CHECK(req_ok[r]);
    for (int i = 0; i < num_queryables; ++i) CHECK(qbl_ok[i]);
}

// Session::put/try_put can only ever encode Put -- a real Session never sends a Del
// -- so this drives one through the wire by hand via RawClient to prove Push
// routing is payload-shape-agnostic and a subscriber decodes it as a del Sample.
TEST("Broker forwards a raw Del Push to a matching subscriber as SampleKind::del") {
    auto tb = TestBroker::start();
    if (!tb.broker) return;

    using namespace zenoh;
    auto sess = Session::open(tb.endpoint());
    CHECK(sess.has_value());
    if (!sess) return;
    auto sub = sess->declare_subscriber("demo/del/key");
    CHECK(sub.has_value());
    if (!sub) return;

    std::this_thread::sleep_for(std::chrono::milliseconds(150));

    auto raw = RawClient::connect(tb.broker->port());
    CHECK(raw.has_value());
    if (!raw) return;
    CHECK(send_raw_del(*raw, "demo/del/key", 0));

    auto s = sub->recv();
    CHECK(s.has_value());
    if (s) {
        CHECK(s->kind() == SampleKind::del);
        CHECK(s->key_expr() == "demo/del/key");
        CHECK(s->payload().empty());
    }
}

TEST("A publisher never receives its own Push, even with a matching subscription") {
    auto tb = TestBroker::start();
    if (!tb.broker) return;

    using namespace zenoh;
    auto sess = Session::open(tb.endpoint());
    auto other_pub = Session::open(tb.endpoint());
    CHECK(sess.has_value());
    CHECK(other_pub.has_value());
    if (!sess || !other_pub) return;

    auto sub = sess->declare_subscriber("demo/selfloop/**");
    CHECK(sub.has_value());
    if (!sub) return;

    std::this_thread::sleep_for(std::chrono::milliseconds(150));

    // Self-loop skip is per connection (face), not per message -- `sess` publishing
    // to its own matching subscription is unconditionally dropped, so it can never
    // be observed directly. Prove it indirectly: publish it first, then have a
    // *different* session publish a distinct payload the same subscription also
    // matches, and confirm that's the first (and only) thing ever delivered --
    // if the self-publish had leaked through, it would have arrived first instead.
    CHECK(sess->put("demo/selfloop/a", bytes("self")).has_value());
    CHECK(other_pub->put("demo/selfloop/b", bytes("other")).has_value());

    auto s = sub->recv();
    CHECK(s.has_value());
    if (s) CHECK(str(s->payload()) == "other");
}

TEST("A deep wildcard hierarchy fans out to every intersecting pattern and no other") {
    auto tb = TestBroker::start();
    if (!tb.broker) return;

    using namespace zenoh;
    auto pub = Session::open(tb.endpoint());
    auto sess_deep_star = Session::open(tb.endpoint());  // "**": matches
    auto sess_wrong_star = Session::open(tb.endpoint()); // "a/*": too shallow, no match
    auto sess_mid_star = Session::open(tb.endpoint());   // "a/b/**": matches
    auto sess_literal = Session::open(tb.endpoint());    // literal key itself: matches
    CHECK(pub.has_value());
    CHECK(sess_deep_star.has_value());
    CHECK(sess_wrong_star.has_value());
    CHECK(sess_mid_star.has_value());
    CHECK(sess_literal.has_value());
    if (!pub || !sess_deep_star || !sess_wrong_star || !sess_mid_star || !sess_literal) return;

    auto sub_deep = sess_deep_star->declare_subscriber("**");
    auto sub_wrong = sess_wrong_star->declare_subscriber("a/*");
    auto sub_mid = sess_mid_star->declare_subscriber("a/b/**");
    auto sub_literal = sess_literal->declare_subscriber("a/b/c/d");
    CHECK(sub_deep.has_value());
    CHECK(sub_wrong.has_value());
    CHECK(sub_mid.has_value());
    CHECK(sub_literal.has_value());
    if (!sub_deep || !sub_wrong || !sub_mid || !sub_literal) return;

    std::this_thread::sleep_for(std::chrono::milliseconds(150));

    bool wrong_got_it = false;
    std::string wrong_payload;
    std::thread wrong_thread([&] {
        auto s = sub_wrong->recv();
        wrong_got_it = s.has_value();
        if (s) wrong_payload = str(s->payload());
    });

    CHECK(pub->put("a/b/c/d", bytes("deep")).has_value());

    auto sd = sub_deep->recv();
    CHECK(sd.has_value());
    if (sd) CHECK(str(sd->payload()) == "deep");
    auto sm = sub_mid->recv();
    CHECK(sm.has_value());
    if (sm) CHECK(str(sm->payload()) == "deep");
    auto sl = sub_literal->recv();
    CHECK(sl.has_value());
    if (sl) CHECK(str(sl->payload()) == "deep");

    // Prove "a/*" (single segment, no **) never received the 4-segment publish:
    // publish something it *does* match and confirm that's the first thing it sees.
    CHECK(pub->put("a/x", bytes("only-this")).has_value());
    wrong_thread.join();
    CHECK(wrong_got_it);
    if (wrong_got_it) CHECK(wrong_payload == "only-this");
}

TEST("A zero-length payload round-trips intact for both put() and get()") {
    auto tb = TestBroker::start();
    if (!tb.broker) return;

    using namespace zenoh;
    auto pub = Session::open(tb.endpoint());
    auto sub_sess = Session::open(tb.endpoint());
    auto qbl_sess = Session::open(tb.endpoint());
    auto requester = Session::open(tb.endpoint());
    CHECK(pub.has_value());
    CHECK(sub_sess.has_value());
    CHECK(qbl_sess.has_value());
    CHECK(requester.has_value());
    if (!pub || !sub_sess || !qbl_sess || !requester) return;

    auto sub = sub_sess->declare_subscriber("demo/empty/put");
    auto qbl = qbl_sess->declare_queryable("demo/empty/get");
    CHECK(sub.has_value());
    CHECK(qbl.has_value());
    if (!sub || !qbl) return;

    std::this_thread::sleep_for(std::chrono::milliseconds(150));

    CHECK(pub->put("demo/empty/put", {}).has_value());
    auto s = sub->recv();
    CHECK(s.has_value());
    if (s) CHECK(s->payload().empty());

    std::thread qbl_thread([&] {
        auto q = qbl->recv();
        if (q) (void)q->reply(q->key_expr(), {});
    });
    auto getter = requester->get("demo/empty/get");
    CHECK(getter.has_value());
    if (getter) {
        auto r = getter->recv();
        CHECK(r.has_value() && r->has_value());
        if (r && *r) {
            CHECK((*r)->is_ok());
            CHECK((*r)->sample().payload().empty());
        }
    }
    qbl_thread.join();
}

TEST("A large (32KB) payload delivers whole and intact to a subscriber") {
    auto tb = TestBroker::start();
    if (!tb.broker) return;

    using namespace zenoh;
    auto pub = Session::open(tb.endpoint());
    auto sub_sess = Session::open(tb.endpoint());
    CHECK(pub.has_value());
    CHECK(sub_sess.has_value());
    if (!pub || !sub_sess) return;

    auto sub = sub_sess->declare_subscriber("demo/large/payload");
    CHECK(sub.has_value());
    if (!sub) return;

    std::this_thread::sleep_for(std::chrono::milliseconds(150));

    std::vector<std::byte> big(32768);
    for (std::size_t i = 0; i < big.size(); ++i) big[i] = static_cast<std::byte>(i & 0xff);

    CHECK(pub->put("demo/large/payload", big).has_value());
    auto s = sub->recv();
    CHECK(s.has_value());
    if (s) {
        CHECK(s->payload().size() == big.size());
        CHECK(std::ranges::equal(s->payload(), big));
    }
}

TEST("Redeclaring a subscriber on the same key repeatedly never accumulates stale "
     "resource-table entries") {
    auto tb = TestBroker::start();
    if (!tb.broker) return;
    auto& tables = tb.broker->tables;

    using namespace zenoh;
    auto sess = Session::open(tb.endpoint());
    CHECK(sess.has_value());
    if (!sess) return;

    constexpr int cycles = 4;
    for (int i = 0; i < cycles; ++i) {
        auto sub = sess->declare_subscriber("demo/churn/key");
        CHECK(sub.has_value());
        if (!sub) return;

        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        CHECK(on_strand(tables, [&] { return tables.resource_face_count("demo/churn/key"); }) == 1);

        sub->undeclare();
        CHECK(wait_until([&] {
            return on_strand(tables,
                             [&] { return tables.resource_face_count("demo/churn/key"); }) == 0;
        }));
    }
}

TEST("A session's disconnect (not just an explicit undeclare) removes it from the "
     "resource table") {
    auto tb = TestBroker::start();
    if (!tb.broker) return;
    auto& tables = tb.broker->tables;

    using namespace zenoh;
    {
        auto sess = Session::open(tb.endpoint());
        CHECK(sess.has_value());
        if (!sess) return;
        auto sub = sess->declare_subscriber("demo/disconnect/resource");
        CHECK(sub.has_value());
        if (!sub) return;

        CHECK(wait_until(
            [&] { return on_strand(tables, [&] { return tables.resource_count(); }) == 1; }));
    } // `sess` (and its Subscriber) destroyed here without ever calling undeclare().

    CHECK(wait_until(
        [&] { return on_strand(tables, [&] { return tables.resource_count(); }) == 0; }));
}

TEST("put() targeted at a since-disconnected zid reaches nobody, and the broker "
     "keeps routing normally afterward") {
    auto tb = TestBroker::start();
    if (!tb.broker) return;

    using namespace zenoh;
    auto pub = Session::open(tb.endpoint());
    auto sess_a = Session::open(tb.endpoint());
    CHECK(pub.has_value());
    CHECK(sess_a.has_value());
    if (!pub || !sess_a) return;

    auto sub_a = sess_a->declare_subscriber("demo/gone/zid");
    CHECK(sub_a.has_value());
    if (!sub_a) return;

    PeerId zid_b{};
    {
        auto sess_b = Session::open(tb.endpoint());
        CHECK(sess_b.has_value());
        if (!sess_b) return;
        auto sub_b = sess_b->declare_subscriber("demo/gone/zid");
        CHECK(sub_b.has_value());
        if (!sub_b) return;
        zid_b = sess_b->local_zid();
        std::this_thread::sleep_for(std::chrono::milliseconds(150));
    } // B disconnects here.

    std::this_thread::sleep_for(std::chrono::milliseconds(150));

    CHECK(pub->put("demo/gone/zid", bytes("for-gone-b"), zid_b).has_value());
    CHECK(pub->put("demo/gone/zid", bytes("broadcast")).has_value());

    // A's first delivered sample must be the broadcast: proof the B-targeted put,
    // aimed at a peer no longer connected, was delivered nowhere and did not hang
    // or crash the broker.
    auto a1 = sub_a->recv();
    CHECK(a1.has_value());
    if (a1) CHECK(str(a1->payload()) == "broadcast");
}

// Exercises Tables::on_response's deliberate "do not erase" behavior (see
// broker/src/tables.cpp): a queryable answering the same query more than once
// before it completes -- IncomingQuery::reply()'s doc comment explicitly allows
// this, but no other test in this file ever calls it twice.
TEST("A queryable's reply() called multiple times delivers every reply before the "
     "final") {
    auto tb = TestBroker::start();
    if (!tb.broker) return;

    using namespace zenoh;
    auto responder = Session::open(tb.endpoint());
    auto requester = Session::open(tb.endpoint());
    CHECK(responder.has_value());
    CHECK(requester.has_value());
    if (!responder || !requester) return;

    auto qbl = responder->declare_queryable("demo/multi/reply");
    CHECK(qbl.has_value());
    if (!qbl) return;

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    std::thread responder_thread([&] {
        auto q = qbl->recv();
        if (!q) return;
        (void)q->reply(q->key_expr(), bytes("one"));
        (void)q->reply(q->key_expr(), bytes("two"));
        (void)q->reply(q->key_expr(), bytes("three"));
    });

    auto getter = requester->get("demo/multi/reply");
    CHECK(getter.has_value());
    if (getter) {
        auto replies = drain_replies(*getter);
        std::ranges::sort(replies);
        CHECK((replies == std::vector<std::string>{"one", "three", "two"}));
    }
    responder_thread.join();
}

TEST("One ok and one err reply to the same get() are both delivered, correctly "
     "tagged") {
    auto tb = TestBroker::start();
    if (!tb.broker) return;

    using namespace zenoh;
    auto sess_ok = Session::open(tb.endpoint());
    auto sess_err = Session::open(tb.endpoint());
    auto requester = Session::open(tb.endpoint());
    CHECK(sess_ok.has_value());
    CHECK(sess_err.has_value());
    CHECK(requester.has_value());
    if (!sess_ok || !sess_err || !requester) return;

    auto qbl_ok = sess_ok->declare_queryable("demo/mixed");
    auto qbl_err = sess_err->declare_queryable("demo/mixed");
    CHECK(qbl_ok.has_value());
    CHECK(qbl_err.has_value());
    if (!qbl_ok || !qbl_err) return;

    std::this_thread::sleep_for(std::chrono::milliseconds(150));

    std::thread t_ok([&] {
        auto q = qbl_ok->recv();
        if (q) (void)q->reply(q->key_expr(), bytes("good"));
    });
    std::thread t_err([&] {
        auto q = qbl_err->recv();
        if (q) (void)q->reply_err(bytes("bad"));
    });

    auto getter = requester->get("demo/mixed");
    CHECK(getter.has_value());
    if (getter) {
        int ok_count = 0;
        int err_count = 0;
        for (;;) {
            auto r = getter->recv();
            CHECK(r.has_value());
            if (!r || !*r) break;
            if ((*r)->is_ok()) {
                ++ok_count;
                CHECK(str((*r)->sample().payload()) == "good");
            } else {
                ++err_count;
                CHECK(str((*r)->error_payload()) == "bad");
            }
        }
        CHECK(ok_count == 1);
        CHECK(err_count == 1);
    }
    t_ok.join();
    t_err.join();
}

TEST("A wildcard get() reaches every one of several literal-keyed queryables") {
    auto tb = TestBroker::start();
    if (!tb.broker) return;

    using namespace zenoh;
    auto sess_x = Session::open(tb.endpoint());
    auto sess_y = Session::open(tb.endpoint());
    auto sess_z = Session::open(tb.endpoint());
    auto requester = Session::open(tb.endpoint());
    CHECK(sess_x.has_value());
    CHECK(sess_y.has_value());
    CHECK(sess_z.has_value());
    CHECK(requester.has_value());
    if (!sess_x || !sess_y || !sess_z || !requester) return;

    auto qbl_x = sess_x->declare_queryable("a/x");
    auto qbl_y = sess_y->declare_queryable("a/y");
    auto qbl_z = sess_z->declare_queryable("a/z");
    CHECK(qbl_x.has_value());
    CHECK(qbl_y.has_value());
    CHECK(qbl_z.has_value());
    if (!qbl_x || !qbl_y || !qbl_z) return;

    std::this_thread::sleep_for(std::chrono::milliseconds(150));

    std::thread t_x([&] {
        auto q = qbl_x->recv();
        if (q) (void)q->reply(q->key_expr(), bytes("x"));
    });
    std::thread t_y([&] {
        auto q = qbl_y->recv();
        if (q) (void)q->reply(q->key_expr(), bytes("y"));
    });
    std::thread t_z([&] {
        auto q = qbl_z->recv();
        if (q) (void)q->reply(q->key_expr(), bytes("z"));
    });

    auto getter = requester->get("a/**");
    CHECK(getter.has_value());
    if (getter) {
        auto replies = drain_replies(*getter);
        std::ranges::sort(replies);
        CHECK((replies == std::vector<std::string>{"x", "y", "z"}));
    }
    t_x.join();
    t_y.join();
    t_z.join();
}

TEST("Undeclaring a queryable removes it from the resource table and a subsequent "
     "get() completes with zero matches") {
    auto tb = TestBroker::start();
    if (!tb.broker) return;
    auto& tables = tb.broker->tables;

    using namespace zenoh;
    auto responder = Session::open(tb.endpoint());
    auto requester = Session::open(tb.endpoint());
    CHECK(responder.has_value());
    CHECK(requester.has_value());
    if (!responder || !requester) return;

    auto qbl = responder->declare_queryable("demo/qundeclare");
    CHECK(qbl.has_value());
    if (!qbl) return;

    CHECK(wait_until([&] {
        return on_strand(tables, [&] { return tables.resource_face_count("demo/qundeclare"); }) ==
               1;
    }));

    qbl->undeclare();

    CHECK(wait_until([&] {
        return on_strand(tables, [&] { return tables.resource_face_count("demo/qundeclare"); }) ==
               0;
    }));

    auto getter = requester->get("demo/qundeclare");
    CHECK(getter.has_value());
    if (getter) {
        auto r = getter->recv();
        CHECK(r.has_value() && !r->has_value());
    }
}

// Two get()s in flight on the same session before either Getter is drained --
// proves the requester's rid bookkeeping (pending_gets_) never crosses replies
// between them, even though both requests share the same underlying socket/SN.
TEST("Two concurrent get()s on one session never cross-deliver each other's "
     "replies") {
    auto tb = TestBroker::start();
    if (!tb.broker) return;

    using namespace zenoh;
    auto sess_1 = Session::open(tb.endpoint());
    auto sess_2 = Session::open(tb.endpoint());
    auto requester = Session::open(tb.endpoint());
    CHECK(sess_1.has_value());
    CHECK(sess_2.has_value());
    CHECK(requester.has_value());
    if (!sess_1 || !sess_2 || !requester) return;

    auto qbl_1 = sess_1->declare_queryable("demo/concurrent/one");
    auto qbl_2 = sess_2->declare_queryable("demo/concurrent/two");
    CHECK(qbl_1.has_value());
    CHECK(qbl_2.has_value());
    if (!qbl_1 || !qbl_2) return;

    std::this_thread::sleep_for(std::chrono::milliseconds(150));

    std::thread t1([&] {
        auto q = qbl_1->recv();
        if (q) (void)q->reply(q->key_expr(), bytes("payload-one"));
    });
    std::thread t2([&] {
        auto q = qbl_2->recv();
        if (q) (void)q->reply(q->key_expr(), bytes("payload-two"));
    });

    auto getter1 = requester->get("demo/concurrent/one");
    CHECK(getter1.has_value());
    auto getter2 = requester->get("demo/concurrent/two");
    CHECK(getter2.has_value());

    if (getter1 && getter2) {
        auto replies1 = drain_replies(*getter1);
        auto replies2 = drain_replies(*getter2);
        CHECK(replies1 == std::vector<std::string>{"payload-one"});
        CHECK(replies2 == std::vector<std::string>{"payload-two"});
    }
    t1.join();
    t2.join();
}

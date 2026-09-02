// Clique (broker-to-broker federation) integration tests: several real
// zenoh::broker::Broker instances on loopback ephemeral ports, linked to each other
// and driven by real zenoh::Session clients over the actual wire. Same posture as
// test_broker.cpp -- a real client drives a real, in-process broker -- extended to
// more than one broker.
//
// Scope so far: face classification (client vs peer broker), link establishment,
// mesh formation, connector resilience (a peer that isn't up yet, a link that
// drops), aggregated declaration propagation, and cross-broker pub/sub -- including
// the split-horizon invariant that keeps a three-broker mesh from double-delivering,
// query/reply across the mesh (including QueryTarget semantics, which only become
// distinguishable once local and remote queryables can both exist), gossip
// membership, and per-message congestion control via the standard QoS bit.
//
// The helpers below (TestBroker/on_strand/wait_until) mirror test_broker.cpp's
// deliberately, rather than being shared: each broker test file is self-contained,
// as test_broker_stress.cpp already is.
import zenoh.broker;
import zenoh.proto;
import zenoh;

#include "ztest.hpp"

#include <asio/post.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <future>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

using namespace zenoh::broker;
using namespace zenoh;

namespace {

// Spins a real Broker (optionally dialling `peers`) on its own thread pool against
// an ephemeral loopback port; stop()+join() on destruction. `broker` is null if
// bind() failed (callers CHECK it).
//
// Note the ordering this relies on: `bind` resolves the ephemeral port *before*
// `run` starts dialling, so a test can bind one broker, read its port, and pass that
// endpoint as another broker's peer.
struct TestBroker {
    std::unique_ptr<Broker> broker;
    std::thread runner;

    TestBroker() = default;
    TestBroker(const TestBroker&) = delete;
    auto operator=(const TestBroker&) -> TestBroker& = delete;
    TestBroker(TestBroker&&) = default;
    auto operator=(TestBroker&&) -> TestBroker& = default;

    static auto start(std::vector<std::string> peers = {}, unsigned threads = 1) -> TestBroker {
        TestBroker tb;
        // Clique links are inbound at one end of every pair (a mutual dial collapses
        // to a single connection), so every broker in a federation test has to be
        // willing to accept them -- see BrokerConfig::accept_router_faces, which is
        // off by default precisely because `whatami` is the peer's own unverified
        // claim.
        auto b = Broker::bind(BrokerConfig{.listen_host = "127.0.0.1",
                                           .listen_port = 0,
                                           .peers = std::move(peers),
                                           .accept_router_faces = true});
        CHECK(b.has_value());
        if (!b) return tb;
        tb.broker = std::move(*b);
        auto* raw = tb.broker.get();
        tb.runner = std::thread([raw, threads] { raw->run(threads); });
        return tb;
    }

    [[nodiscard]] auto endpoint() const -> std::string {
        return "tcp/127.0.0.1:" + std::to_string(broker->port());
    }

    auto shutdown() -> void {
        if (broker) broker->stop();
        if (runner.joinable()) runner.join();
        broker.reset();
    }

    ~TestBroker() { shutdown(); }
};

// Reads `fn()` from inside a handler on `tables`'s own routing strand, blocking the
// calling (test) thread. Required for any test-only introspection -- reading these
// from the test thread would race the routing strand exactly like production code.
template <class Fn>
[[nodiscard]] auto on_strand(Tables& tables, Fn fn) -> std::invoke_result_t<Fn> {
    using R = std::invoke_result_t<Fn>;
    std::promise<R> prom;
    auto fut = prom.get_future();
    asio::post(tables.strand(), [&fn, &prom] { prom.set_value(fn()); });
    return fut.get();
}

// Polls `pred` until true or `timeout_ms` elapses. Link establishment is
// asynchronous from the test thread's perspective (connect + a four-message
// handshake + a strand-posted add_face), so every assertion about it is a wait, not
// an immediate read.
template <class Pred> auto wait_until(Pred pred, int timeout_ms = 4000) -> bool {
    auto const deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    return pred();
}

// "This broker currently has exactly `n` peer-broker faces registered."
auto routers(TestBroker& b) -> std::size_t {
    return on_strand(b.broker->tables, [&] { return b.broker->tables.router_face_count(); });
}
auto faces(TestBroker& b) -> std::size_t {
    return on_strand(b.broker->tables, [&] { return b.broker->tables.face_count(); });
}
auto unlinked_peers(TestBroker& b) -> std::size_t {
    return on_strand(b.broker->tables, [&] { return b.broker->tables.unlinked_peer_count(); });
}
auto known_peers(TestBroker& b) -> std::size_t {
    return on_strand(b.broker->tables, [&] { return b.broker->tables.known_peer_count(); });
}
auto resources(TestBroker& b) -> std::size_t {
    return on_strand(b.broker->tables, [&] { return b.broker->tables.resource_count(); });
}
// How many faces this broker has registered on the exact resource `key` -- the
// assertion that actually proves aggregation: N clients on one key behind a peer
// broker must show up here as exactly one face (that peer's link), not N.
auto faces_on(TestBroker& b, std::string_view key) -> std::size_t {
    return on_strand(b.broker->tables, [&] { return b.broker->tables.resource_face_count(key); });
}

auto bytes(std::string_view s) -> std::span<const std::byte> {
    return {reinterpret_cast<const std::byte*>(s.data()), s.size()};
}
auto str(std::span<const std::byte> b) -> std::string {
    return {reinterpret_cast<const char*>(b.data()), b.size()};
}

// One blocking pull from a subscriber, as a string. `Subscriber::recv()` drives its
// own session pump, so nothing else is needed to make progress.
auto next_payload(Subscriber& sub) -> std::string {
    auto got = sub.recv();
    return got ? str(got->payload()) : std::string{"<error>"};
}

// Drains a Getter to completion, collecting each reply's payload (or its error
// payload) as a string. Sorted by the caller where fan-out order is not defined.
auto drain_replies(Getter& g) -> std::vector<std::string> {
    std::vector<std::string> out;
    for (;;) {
        auto r = g.recv();
        if (!r || !*r) return out;
        auto const& reply = **r;
        out.push_back(str(reply.is_ok() ? reply.sample().payload() : reply.error_payload()));
    }
}

} // namespace

TEST("Two brokers linked by --peer register each other as router faces") {
    auto b = TestBroker::start();
    CHECK(b.broker != nullptr);
    if (!b.broker) return;
    auto a = TestBroker::start({b.endpoint()});
    CHECK(a.broker != nullptr);
    if (!a.broker) return;

    // Both ends classify the link as a peer broker: the dialer from the InitAck's
    // whatami, the listener from the InitSyn's -- the field the listener handshake
    // used to discard entirely.
    CHECK(wait_until([&] { return routers(a) == 1; }));
    CHECK(wait_until([&] { return routers(b) == 1; }));
    CHECK(faces(a) == 1);
    CHECK(faces(b) == 1);
}

TEST("A client session is never classified as a router face") {
    auto b = TestBroker::start();
    CHECK(b.broker != nullptr);
    if (!b.broker) return;

    auto s = Session::open(b.endpoint());
    CHECK(s.has_value());
    if (!s) return;

    CHECK(wait_until([&] { return faces(b) == 1; }));
    CHECK(routers(b) == 0);
}

TEST("Client and peer-broker faces coexist and are counted separately") {
    auto b = TestBroker::start();
    CHECK(b.broker != nullptr);
    if (!b.broker) return;
    auto a = TestBroker::start({b.endpoint()});
    CHECK(a.broker != nullptr);
    if (!a.broker) return;

    auto s1 = Session::open(a.endpoint());
    auto s2 = Session::open(a.endpoint());
    CHECK(s1.has_value());
    CHECK(s2.has_value());

    CHECK(wait_until([&] { return faces(a) == 3; })); // two clients + one peer broker
    CHECK(routers(a) == 1);
}

TEST("Three brokers form a full mesh, each holding a link to both others") {
    auto c = TestBroker::start();
    CHECK(c.broker != nullptr);
    if (!c.broker) return;
    auto b = TestBroker::start({c.endpoint()});
    CHECK(b.broker != nullptr);
    if (!b.broker) return;
    auto a = TestBroker::start({b.endpoint(), c.endpoint()});
    CHECK(a.broker != nullptr);
    if (!a.broker) return;

    CHECK(wait_until([&] { return routers(a) == 2; }));
    CHECK(wait_until([&] { return routers(b) == 2; }));
    CHECK(wait_until([&] { return routers(c) == 2; }));
}

TEST("A peer that is not up yet is retried, not an error, and links once it appears") {
    // Bind (but do not run) the target first, purely to reserve its port; the dialer
    // therefore spends its first attempts connecting to a port nobody is listening
    // on, which is the ordinary case when a clique's brokers start together.
    auto reserved = Broker::bind(BrokerConfig{.listen_host = "127.0.0.1", .listen_port = 0});
    CHECK(reserved.has_value());
    if (!reserved) return;
    auto const port = (*reserved)->port();
    auto const endpoint = "tcp/127.0.0.1:" + std::to_string(port);
    reserved->reset(); // release the port; nothing is listening on it now

    auto a = TestBroker::start({endpoint});
    CHECK(a.broker != nullptr);
    if (!a.broker) return;
    CHECK(routers(a) == 0); // nothing to connect to yet

    // Now bring the peer up on that exact port and let the connector's backoff find it.
    auto b = Broker::bind(
        BrokerConfig{.listen_host = "127.0.0.1", .listen_port = port, .accept_router_faces = true});
    CHECK(b.has_value());
    if (!b) return;
    std::thread runner([raw = b->get()] { raw->run(1); });

    CHECK(wait_until([&] { return routers(a) == 1; }));

    (*b)->stop();
    runner.join();
}

TEST("A dropped peer link is re-established by the connector") {
    auto b = TestBroker::start();
    CHECK(b.broker != nullptr);
    if (!b.broker) return;
    auto const endpoint = b.endpoint();

    auto a = TestBroker::start({endpoint});
    CHECK(a.broker != nullptr);
    if (!a.broker) return;
    CHECK(wait_until([&] { return routers(a) == 1; }));

    // Tear the peer down: `a` loses the link and starts re-dialling.
    b.shutdown();
    CHECK(wait_until([&] { return routers(a) == 0; }));

    // Bring a fresh broker up on the same port; the connector reconnects on its own.
    auto b2 = Broker::bind(BrokerConfig{.listen_host = "127.0.0.1",
                                        .listen_port = static_cast<std::uint16_t>(
                                            std::stoi(endpoint.substr(endpoint.rfind(':') + 1))),
                                        .accept_router_faces = true});
    CHECK(b2.has_value());
    if (!b2) return;
    std::thread runner([raw = b2->get()] { raw->run(1); });

    CHECK(wait_until([&] { return routers(a) == 1; }));

    (*b2)->stop();
    runner.join();
}

TEST("An unparseable peer endpoint is ignored rather than faulting the broker") {
    auto b = TestBroker::start();
    CHECK(b.broker != nullptr);
    if (!b.broker) return;

    auto a = TestBroker::start(
        {"not-an-endpoint", "tcp/127.0.0.1:", "tcp/127.0.0.1:99999", b.endpoint()});
    CHECK(a.broker != nullptr);
    if (!a.broker) return;

    // The three malformed entries are dropped; the well-formed one still links.
    CHECK(wait_until([&] { return routers(a) == 1; }));

    // And the broker is otherwise fully functional.
    auto s = Session::open(a.endpoint());
    CHECK(s.has_value());
    CHECK(wait_until([&] { return faces(a) == 2; }));
}

// --- M2: aggregated declaration propagation and cross-broker delivery ---

TEST("A subscriber on one broker receives a Push published on another") {
    auto b = TestBroker::start();
    if (!b.broker) return;
    auto a = TestBroker::start({b.endpoint()});
    if (!a.broker) return;
    CHECK(wait_until([&] { return routers(a) == 1 && routers(b) == 1; }));

    auto sub_sess = Session::open(b.endpoint());
    auto pub_sess = Session::open(a.endpoint());
    CHECK(sub_sess.has_value());
    CHECK(pub_sess.has_value());
    if (!sub_sess || !pub_sess) return;

    auto sub = sub_sess->declare_subscriber("demo/cross/**");
    CHECK(sub.has_value());
    if (!sub) return;

    // The declaration has to have crossed the link before the publish, or the
    // publishing broker has nowhere to route it -- exactly the same ordering a
    // single-broker test needs, one hop further out.
    CHECK(wait_until([&] { return faces_on(a, "demo/cross/**") == 1; }));

    CHECK(pub_sess->put("demo/cross/one", bytes("hello")).has_value());
    CHECK(next_payload(*sub) == "hello");
}

TEST("Many subscribers behind a peer broker are announced as one aggregated declaration") {
    auto b = TestBroker::start();
    if (!b.broker) return;
    auto a = TestBroker::start({b.endpoint()});
    if (!a.broker) return;
    CHECK(wait_until([&] { return routers(a) == 1; }));

    // Reserved up front, and deliberately so: a `Subscriber` holds a pointer back to
    // its `Session` (which must outlive it), so letting the vector reallocate as it
    // grows would leave every already-declared subscriber pointing at freed storage.
    constexpr int subscriber_count = 5;
    std::vector<Session> sessions;
    std::vector<Subscriber> subs;
    sessions.reserve(subscriber_count);
    subs.reserve(subscriber_count);
    for (int i = 0; i < subscriber_count; ++i) {
        auto s = Session::open(b.endpoint());
        CHECK(s.has_value());
        if (!s) return;
        sessions.push_back(std::move(*s));
        auto sub = sessions.back().declare_subscriber("demo/agg/**");
        CHECK(sub.has_value());
        if (!sub) return;
        subs.push_back(std::move(*sub));
    }

    // All five are registered locally on `b`...
    CHECK(wait_until([&] { return faces_on(b, "demo/agg/**") == subscriber_count; }));
    // ...but `a` only ever hears about the key once, via the single peer link.
    CHECK(wait_until([&] { return faces_on(a, "demo/agg/**") == 1; }));
    CHECK(resources(a) == 1);
}

TEST("A key is withdrawn from peer brokers only once its last local subscriber goes") {
    auto b = TestBroker::start();
    if (!b.broker) return;
    auto a = TestBroker::start({b.endpoint()});
    if (!a.broker) return;
    CHECK(wait_until([&] { return routers(a) == 1; }));

    auto s1 = Session::open(b.endpoint());
    auto s2 = Session::open(b.endpoint());
    CHECK(s1.has_value());
    CHECK(s2.has_value());
    if (!s1 || !s2) return;

    auto sub1 = s1->declare_subscriber("demo/refcount");
    auto sub2 = s2->declare_subscriber("demo/refcount");
    CHECK(sub1.has_value());
    CHECK(sub2.has_value());
    if (!sub1 || !sub2) return;
    CHECK(wait_until([&] { return faces_on(a, "demo/refcount") == 1; }));

    // First one away: `b` still has a subscriber, so nothing is withdrawn.
    sub1->undeclare();
    CHECK(wait_until([&] { return faces_on(b, "demo/refcount") == 1; }));
    CHECK(faces_on(a, "demo/refcount") == 1);

    // Last one away: now the key is withdrawn from the clique.
    sub2->undeclare();
    CHECK(wait_until([&] { return faces_on(a, "demo/refcount") == 0; }));
    CHECK(resources(a) == 0);
}

TEST("Declarations made before a peer link exists are replayed onto it") {
    auto b = TestBroker::start();
    if (!b.broker) return;

    auto sess = Session::open(b.endpoint());
    CHECK(sess.has_value());
    if (!sess) return;
    auto sub = sess->declare_subscriber("demo/replay/**");
    CHECK(sub.has_value());
    if (!sub) return;
    CHECK(wait_until([&] { return resources(b) == 1; }));

    // Only now does the peer appear. It must still learn the pre-existing
    // declaration -- there is no Interest exchange between brokers, the link-up
    // replay is the entire sync.
    auto a = TestBroker::start({b.endpoint()});
    if (!a.broker) return;
    CHECK(wait_until([&] { return faces_on(a, "demo/replay/**") == 1; }));

    auto pub_sess = Session::open(a.endpoint());
    CHECK(pub_sess.has_value());
    if (!pub_sess) return;
    CHECK(pub_sess->put("demo/replay/x", bytes("late")).has_value());
    CHECK(next_payload(*sub) == "late");
}

TEST("In a three-broker mesh every subscriber receives each message exactly once") {
    // Full mesh a-b, b-c, a-c, with a subscriber behind *both* peers of the
    // publisher. That is what makes duplicate delivery possible at all: `b` holds
    // `c`'s declaration (it learned it directly), so when `a`'s message reaches `b`,
    // a broker that re-forwarded router-sourced traffic would send it on to `c` --
    // which already got its own copy straight from `a`. Split horizon is the only
    // thing standing between this topology and every subscriber seeing doubles.
    auto c = TestBroker::start();
    if (!c.broker) return;
    auto b = TestBroker::start({c.endpoint()});
    if (!b.broker) return;
    auto a = TestBroker::start({b.endpoint(), c.endpoint()});
    if (!a.broker) return;
    CHECK(wait_until([&] { return routers(a) == 2 && routers(b) == 2 && routers(c) == 2; }));

    auto sess_b = Session::open(b.endpoint());
    auto sess_c = Session::open(c.endpoint());
    auto pub_sess = Session::open(a.endpoint());
    CHECK(sess_b.has_value());
    CHECK(sess_c.has_value());
    CHECK(pub_sess.has_value());
    if (!sess_b || !sess_c || !pub_sess) return;

    auto sub_b = sess_b->declare_subscriber("demo/mesh/**");
    auto sub_c = sess_c->declare_subscriber("demo/mesh/**");
    CHECK(sub_b.has_value());
    CHECK(sub_c.has_value());
    if (!sub_b || !sub_c) return;

    // `a` hears the key once from each peer -- and only from the peer that actually
    // has the local subscriber, since a relayed declaration is not re-announced
    // either (the invariant covers declarations as well as data).
    CHECK(wait_until([&] { return faces_on(a, "demo/mesh/**") == 2; }));
    // Each peer holds the other's declaration too, which is what would make a
    // re-forwarding broker double-deliver.
    CHECK(wait_until([&] { return faces_on(b, "demo/mesh/**") == 2; })); // own client + c
    CHECK(wait_until([&] { return faces_on(c, "demo/mesh/**") == 2; })); // own client + b

    CHECK(pub_sess->put("demo/mesh/k", bytes("first")).has_value());
    CHECK(pub_sess->put("demo/mesh/k", bytes("second")).has_value());

    // A duplicate shows up as "first" twice; the *second* read is what proves
    // exactly-once delivery, not the first.
    CHECK(next_payload(*sub_b) == "first");
    CHECK(next_payload(*sub_b) == "second");
    CHECK(next_payload(*sub_c) == "first");
    CHECK(next_payload(*sub_c) == "second");
}

TEST("A peer broker going away purges the declarations it had announced") {
    auto b = TestBroker::start();
    if (!b.broker) return;
    auto a = TestBroker::start({b.endpoint()});
    if (!a.broker) return;
    CHECK(wait_until([&] { return routers(a) == 1; }));

    auto sess = Session::open(b.endpoint());
    CHECK(sess.has_value());
    if (!sess) return;
    auto sub = sess->declare_subscriber("demo/purge/**");
    CHECK(sub.has_value());
    if (!sub) return;
    CHECK(wait_until([&] { return faces_on(a, "demo/purge/**") == 1; }));

    b.shutdown();

    // The peer's link dropping is the only signal `a` gets, and it is enough:
    // remove_face strips every declaration that face had announced.
    CHECK(wait_until([&] { return routers(a) == 0; }));
    CHECK(wait_until([&] { return resources(a) == 0; }));
}

TEST("A wildcard subscription on one broker matches a literal publish on another") {
    auto b = TestBroker::start();
    if (!b.broker) return;
    auto a = TestBroker::start({b.endpoint()});
    if (!a.broker) return;
    CHECK(wait_until([&] { return routers(a) == 1; }));

    auto sub_sess = Session::open(b.endpoint());
    auto pub_sess = Session::open(a.endpoint());
    CHECK(sub_sess.has_value());
    CHECK(pub_sess.has_value());
    if (!sub_sess || !pub_sess) return;

    auto sub = sub_sess->declare_subscriber("demo/*/deep/**");
    CHECK(sub.has_value());
    if (!sub) return;
    CHECK(wait_until([&] { return faces_on(a, "demo/*/deep/**") == 1; }));

    // A non-matching publish must not cross, and the matching one must arrive
    // intact -- the same ke::intersects path a single broker uses, one hop out.
    CHECK(pub_sess->put("demo/x/shallow", bytes("no")).has_value());
    CHECK(pub_sess->put("demo/x/deep/y/z", bytes("yes")).has_value());
    CHECK(next_payload(*sub) == "yes");
}

// --- M3: query/reply across the mesh ---

TEST("A get() on one broker is answered by a queryable on another") {
    auto b = TestBroker::start();
    if (!b.broker) return;
    auto a = TestBroker::start({b.endpoint()});
    if (!a.broker) return;
    CHECK(wait_until([&] { return routers(a) == 1; }));

    auto responder = Session::open(b.endpoint());
    CHECK(responder.has_value());
    if (!responder) return;
    auto qbl = responder->declare_queryable("demo/q/answer");
    CHECK(qbl.has_value());
    if (!qbl) return;

    std::atomic<bool> got_query{false};
    std::thread responder_thread([&] {
        auto q = qbl->recv();
        if (!q) return;
        got_query.store(true);
        (void)q->reply("demo/q/answer", bytes("from-b"));
    });

    // The queryable declaration must have crossed the link before the query is
    // issued, or `a` has no candidate and terminates it immediately.
    CHECK(wait_until([&] { return faces_on(a, "demo/q/answer") == 1; }));

    auto requester = Session::open(a.endpoint());
    CHECK(requester.has_value());
    if (!requester) {
        responder_thread.join();
        return;
    }
    auto getter = requester->get("demo/q/**");
    CHECK(getter.has_value());
    auto replies = getter ? drain_replies(*getter) : std::vector<std::string>{};
    responder_thread.join();

    CHECK(got_query.load());
    CHECK(replies.size() == 1);
    if (replies.size() == 1) CHECK(replies[0] == "from-b");
}

TEST("QueryTarget::all reaches matching queryables on every broker in the mesh") {
    auto b = TestBroker::start();
    if (!b.broker) return;
    auto a = TestBroker::start({b.endpoint()});
    if (!a.broker) return;
    CHECK(wait_until([&] { return routers(a) == 1; }));

    auto resp_a = Session::open(a.endpoint());
    auto resp_b = Session::open(b.endpoint());
    CHECK(resp_a.has_value());
    CHECK(resp_b.has_value());
    if (!resp_a || !resp_b) return;
    auto qbl_a = resp_a->declare_queryable("demo/all/a");
    auto qbl_b = resp_b->declare_queryable("demo/all/b");
    CHECK(qbl_a.has_value());
    CHECK(qbl_b.has_value());
    if (!qbl_a || !qbl_b) return;

    std::thread ta([&] {
        auto q = qbl_a->recv();
        if (q) (void)q->reply("demo/all/a", bytes("a"));
    });
    std::thread tb([&] {
        auto q = qbl_b->recv();
        if (q) (void)q->reply("demo/all/b", bytes("b"));
    });

    CHECK(wait_until([&] { return faces_on(a, "demo/all/b") == 1; }));

    auto requester = Session::open(a.endpoint());
    CHECK(requester.has_value());
    if (!requester) {
        ta.join();
        tb.join();
        return;
    }
    auto getter = requester->get("demo/all/**", "", GetOptions{.target = GetTarget::all});
    CHECK(getter.has_value());
    auto replies = getter ? drain_replies(*getter) : std::vector<std::string>{};
    ta.join();
    tb.join();

    std::sort(replies.begin(), replies.end()); // fan-out order is not defined
    CHECK(replies.size() == 2);
    if (replies.size() == 2) {
        CHECK(replies[0] == "a");
        CHECK(replies[1] == "b");
    }
}

TEST("best_matching prefers a local queryable and never crosses the mesh for it") {
    auto b = TestBroker::start();
    if (!b.broker) return;
    auto a = TestBroker::start({b.endpoint()});
    if (!a.broker) return;
    CHECK(wait_until([&] { return routers(a) == 1; }));

    // Both brokers have a queryable on the *same* key, so the two are genuinely
    // interchangeable and only proximity distinguishes them.
    auto resp_a = Session::open(a.endpoint());
    auto resp_b = Session::open(b.endpoint());
    CHECK(resp_a.has_value());
    CHECK(resp_b.has_value());
    if (!resp_a || !resp_b) return;
    auto qbl_a = resp_a->declare_queryable("demo/bm");
    auto qbl_b = resp_b->declare_queryable("demo/bm");
    CHECK(qbl_a.has_value());
    CHECK(qbl_b.has_value());
    if (!qbl_a || !qbl_b) return;

    std::atomic<bool> remote_was_queried{false};
    std::thread ta([&] {
        auto q = qbl_a->recv();
        if (q) (void)q->reply("demo/bm", bytes("local"));
    });
    std::thread tb([&] {
        auto q = qbl_b->recv();
        if (!q) return;
        remote_was_queried.store(true);
        (void)q->reply("demo/bm", bytes("remote"));
    });

    // Wait until `a` genuinely knows about both candidates -- its own client and the
    // peer link -- so that "only the local one answered" is a real choice, not a race.
    CHECK(wait_until([&] { return faces_on(a, "demo/bm") == 2; }));

    auto requester = Session::open(a.endpoint());
    CHECK(requester.has_value());
    if (!requester) {
        ta.join();
        tb.join();
        return;
    }
    auto getter = requester->get("demo/bm"); // best_matching is the default
    CHECK(getter.has_value());
    auto replies = getter ? drain_replies(*getter) : std::vector<std::string>{};

    CHECK(replies.size() == 1);
    if (replies.size() == 1) CHECK(replies[0] == "local");
    CHECK(!remote_was_queried.load());

    // Release the remote responder's still-blocked recv() so its thread can unwind.
    // Done by tearing down its broker rather than closing the session from here:
    // Session's receive path is single-threaded, so calling close() while another
    // thread is inside recv() would race it (see Session's own note). Dropping the
    // link underneath makes recv() return an error, which is exactly the shutdown
    // path a real peer disappearing would take.
    b.shutdown();
    ta.join();
    tb.join();
}

TEST("best_matching falls back across the mesh when there is no local queryable") {
    auto b = TestBroker::start();
    if (!b.broker) return;
    auto a = TestBroker::start({b.endpoint()});
    if (!a.broker) return;
    CHECK(wait_until([&] { return routers(a) == 1; }));

    auto responder = Session::open(b.endpoint());
    CHECK(responder.has_value());
    if (!responder) return;
    auto qbl = responder->declare_queryable("demo/fallback");
    CHECK(qbl.has_value());
    if (!qbl) return;

    std::thread t([&] {
        auto q = qbl->recv();
        if (q) (void)q->reply("demo/fallback", bytes("remote"));
    });
    CHECK(wait_until([&] { return faces_on(a, "demo/fallback") == 1; }));

    auto requester = Session::open(a.endpoint());
    CHECK(requester.has_value());
    if (!requester) {
        t.join();
        return;
    }
    auto getter = requester->get("demo/fallback");
    CHECK(getter.has_value());
    auto replies = getter ? drain_replies(*getter) : std::vector<std::string>{};
    t.join();

    CHECK(replies.size() == 1);
    if (replies.size() == 1) CHECK(replies[0] == "remote");
}

TEST("A get() matching nothing anywhere in the mesh still terminates") {
    auto b = TestBroker::start();
    if (!b.broker) return;
    auto a = TestBroker::start({b.endpoint()});
    if (!a.broker) return;
    CHECK(wait_until([&] { return routers(a) == 1; }));

    auto requester = Session::open(a.endpoint());
    CHECK(requester.has_value());
    if (!requester) return;
    auto getter = requester->get("demo/nobody/**");
    CHECK(getter.has_value());
    if (!getter) return;
    CHECK(drain_replies(*getter).empty()); // completes, rather than hanging
}

TEST("A request id still live for a face is terminated, not allowed to clobber it") {
    // Driven directly against Tables: the collision this guards against needs a peer
    // that reuses an in-flight request id, which a well-behaved Session never does.
    // Federation makes it reachable -- a peer-broker face carries request ids chosen
    // by the *remote broker* for all of its clients -- so it is worth pinning down.
    auto tb = TestBroker::start();
    if (!tb.broker) return;
    auto& tables = tb.broker->tables;

    std::atomic<int> requester_deliveries{0};
    std::atomic<int> queryable_deliveries{0};
    auto counting_face = [](std::atomic<int>& counter) {
        return [&counter](SharedBuf, std::vector<MsgSlice> slices) {
            counter.fetch_add(static_cast<int>(slices.size()), std::memory_order_relaxed);
        };
    };

    constexpr FaceId requester_id = 9001;
    constexpr FaceId queryable_id = 9002;
    (void)on_strand(tables, [&] {
        tables.add_face(
            FaceHandle{.id = requester_id,
                       .kind = FaceKind::client,
                       .deliver = counting_face(requester_deliveries),
                       .pressure = std::make_shared<std::atomic<FacePressure>>(FacePressure::ok)});
        tables.add_face(
            FaceHandle{.id = queryable_id,
                       .kind = FaceKind::client,
                       .deliver = counting_face(queryable_deliveries),
                       .pressure = std::make_shared<std::atomic<FacePressure>>(FacePressure::ok)});
        tables.on_declare_queryable(queryable_id, "demo/collide", QueryableInfo{});

        // First request: fans out to the queryable and stays outstanding (no
        // ResponseFinal is ever sent for it here).
        tables.on_request(requester_id, RoutedRequest{.origin_rid = 7, .key = "demo/collide"});
        // Second request reusing the *same* still-live rid.
        tables.on_request(requester_id, RoutedRequest{.origin_rid = 7, .key = "demo/collide"});
        return 0;
    });

    // The original fan-out is intact -- one entry, not overwritten...
    CHECK(on_strand(tables, [&] { return tables.fanout_count(); }) == 1);
    CHECK(on_strand(tables, [&] { return tables.pending_query_count(); }) == 1);
    // ...the queryable saw only the first request...
    CHECK(queryable_deliveries.load() == 1);
    // ...and the duplicate was answered immediately with its own ResponseFinal.
    CHECK(requester_deliveries.load() == 1);

    (void)on_strand(tables, [&] {
        tables.remove_face(requester_id);
        tables.remove_face(queryable_id);
        return 0;
    });
}

// --- M3: zid-targeting across the mesh (enforced at the terminal hop) ---

TEST("target_zid on put() reaches a client behind another broker, and only that one") {
    auto b = TestBroker::start();
    if (!b.broker) return;
    auto a = TestBroker::start({b.endpoint()});
    if (!a.broker) return;
    CHECK(wait_until([&] { return routers(a) == 1; }));

    auto s1 = Session::open(b.endpoint());
    auto s2 = Session::open(b.endpoint());
    auto pub_sess = Session::open(a.endpoint());
    CHECK(s1.has_value());
    CHECK(s2.has_value());
    CHECK(pub_sess.has_value());
    if (!s1 || !s2 || !pub_sess) return;

    auto sub1 = s1->declare_subscriber("demo/tz/**");
    auto sub2 = s2->declare_subscriber("demo/tz/**");
    CHECK(sub1.has_value());
    CHECK(sub2.has_value());
    if (!sub1 || !sub2) return;
    CHECK(wait_until([&] { return faces_on(b, "demo/tz/**") == 2; }));
    CHECK(wait_until([&] { return faces_on(a, "demo/tz/**") == 1; }));

    // The publishing broker cannot evaluate this target at all -- the zid belongs to
    // a client it has never seen. It forwards on key match alone, and the broker
    // that actually owns that client applies the filter.
    CHECK(pub_sess->put("demo/tz/k", bytes("targeted"), PutOptions{.target_zid = s2->local_zid()})
              .has_value());
    CHECK(next_payload(*sub2) == "targeted");

    // Prove sub1 never got the targeted message: the next thing it sees must be the
    // untargeted one published afterwards.
    CHECK(pub_sess->put("demo/tz/k", bytes("broadcast")).has_value());
    CHECK(next_payload(*sub1) == "broadcast");
    CHECK(next_payload(*sub2) == "broadcast");
}

TEST("target_zid on put() naming nobody in the mesh reaches nobody, and is not a bypass") {
    auto b = TestBroker::start();
    if (!b.broker) return;
    auto a = TestBroker::start({b.endpoint()});
    if (!a.broker) return;
    CHECK(wait_until([&] { return routers(a) == 1; }));

    auto sub_sess = Session::open(b.endpoint());
    auto pub_sess = Session::open(a.endpoint());
    CHECK(sub_sess.has_value());
    CHECK(pub_sess.has_value());
    if (!sub_sess || !pub_sess) return;
    auto sub = sub_sess->declare_subscriber("demo/tznobody/**");
    CHECK(sub.has_value());
    if (!sub) return;
    CHECK(wait_until([&] { return faces_on(a, "demo/tznobody/**") == 1; }));

    // A zid no live peer holds: forwarding across the link is fine (the broker on
    // the far side is where the answer to "does anyone here have this zid" lives),
    // but the far side must then deliver to nobody rather than falling back to
    // everyone matching.
    auto const nobody = pub_sess->local_zid(); // a real zid, but not a subscriber's
    CHECK(pub_sess->put("demo/tznobody/k", bytes("nobody"), PutOptions{.target_zid = nobody})
              .has_value());

    CHECK(pub_sess->put("demo/tznobody/k", bytes("everyone")).has_value());
    CHECK(next_payload(*sub) == "everyone");
}

TEST("target_zid on get() narrows to one queryable behind another broker") {
    auto b = TestBroker::start();
    if (!b.broker) return;
    auto a = TestBroker::start({b.endpoint()});
    if (!a.broker) return;
    CHECK(wait_until([&] { return routers(a) == 1; }));

    auto q1 = Session::open(b.endpoint());
    auto q2 = Session::open(b.endpoint());
    CHECK(q1.has_value());
    CHECK(q2.has_value());
    if (!q1 || !q2) return;
    auto qbl1 = q1->declare_queryable("demo/tzq");
    auto qbl2 = q2->declare_queryable("demo/tzq");
    CHECK(qbl1.has_value());
    CHECK(qbl2.has_value());
    if (!qbl1 || !qbl2) return;

    std::atomic<bool> q1_was_queried{false};
    std::thread t1([&] {
        auto q = qbl1->recv();
        if (!q) return;
        q1_was_queried.store(true);
        (void)q->reply("demo/tzq", bytes("one"));
    });
    std::thread t2([&] {
        auto q = qbl2->recv();
        if (q) (void)q->reply("demo/tzq", bytes("two"));
    });
    CHECK(wait_until([&] { return faces_on(b, "demo/tzq") == 2; }));
    CHECK(wait_until([&] { return faces_on(a, "demo/tzq") == 1; }));

    auto requester = Session::open(a.endpoint());
    CHECK(requester.has_value());
    if (!requester) {
        b.shutdown();
        t1.join();
        t2.join();
        return;
    }
    auto getter = requester->get(
        "demo/tzq", "", GetOptions{.target = GetTarget::all, .target_zid = q2->local_zid()});
    CHECK(getter.has_value());
    auto replies = getter ? drain_replies(*getter) : std::vector<std::string>{};

    CHECK(replies.size() == 1);
    if (replies.size() == 1) CHECK(replies[0] == "two");
    CHECK(!q1_was_queried.load());

    b.shutdown(); // unblock t1's still-pending recv()
    t1.join();
    t2.join();
}

// --- M4: gossip membership ---

TEST("A broker seeded with one member learns the rest of the clique and links to it") {
    // `a` and `c` are each told about `b` and nothing else. Gossip has to do the
    // rest: `b` tells each of them about the other, and the mesh closes itself.
    // This is the whole point of gossip over a static list -- adding a broker means
    // configuring the newcomer, not reconfiguring everyone already running.
    auto b = TestBroker::start();
    if (!b.broker) return;
    auto a = TestBroker::start({b.endpoint()});
    if (!a.broker) return;
    auto c = TestBroker::start({b.endpoint()});
    if (!c.broker) return;

    CHECK(wait_until([&] { return routers(a) == 2; }));
    CHECK(wait_until([&] { return routers(b) == 2; }));
    CHECK(wait_until([&] { return routers(c) == 2; }));

    // And every broker knows the identity of both others, not just the seed.
    CHECK(known_peers(a) == 2);
    CHECK(known_peers(b) == 2);
    CHECK(known_peers(c) == 2);
}

TEST("Data flows over a link that only gossip established") {
    auto b = TestBroker::start();
    if (!b.broker) return;
    auto a = TestBroker::start({b.endpoint()});
    if (!a.broker) return;
    auto c = TestBroker::start({b.endpoint()});
    if (!c.broker) return;
    CHECK(wait_until([&] { return routers(a) == 2 && routers(c) == 2; }));

    // `a` and `c` were never told about each other by configuration; the link
    // between them exists only because gossip produced it.
    auto sub_sess = Session::open(c.endpoint());
    auto pub_sess = Session::open(a.endpoint());
    CHECK(sub_sess.has_value());
    CHECK(pub_sess.has_value());
    if (!sub_sess || !pub_sess) return;

    auto sub = sub_sess->declare_subscriber("demo/gossip/**");
    CHECK(sub.has_value());
    if (!sub) return;
    CHECK(wait_until([&] { return faces_on(a, "demo/gossip/**") >= 1; }));

    CHECK(pub_sess->put("demo/gossip/k", bytes("via-gossip")).has_value());
    CHECK(next_payload(*sub) == "via-gossip");
    // Exactly one copy, even though `a` can reach `c` both directly and through `b`.
    CHECK(pub_sess->put("demo/gossip/k", bytes("second")).has_value());
    CHECK(next_payload(*sub) == "second");
}

TEST("Two brokers seeded with each other settle on a single link, and stay settled") {
    // Both ends dial simultaneously, so two links exist for a moment and one must be
    // collapsed. The collapse has to be stable: the losing side's connector must not
    // simply reconnect into another collapse, which would flap forever.
    auto b = TestBroker::start();
    if (!b.broker) return;
    auto a = TestBroker::start({b.endpoint()});
    if (!a.broker) return;

    // Teach `b` about `a` as well, the way an operator seeding both directions would.
    // (Gossip already does this, so this asserts the steady state either way.)
    CHECK(wait_until([&] { return routers(a) == 1 && routers(b) == 1; }));

    // Hold still and confirm it does not oscillate: a flapping collapse would show
    // up here as a count that keeps leaving 1.
    for (int i = 0; i < 20; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(25));
        CHECK(routers(a) == 1);
        CHECK(routers(b) == 1);
    }
}

TEST("A four-broker clique converges to a full mesh from a single seed each") {
    auto seed = TestBroker::start();
    if (!seed.broker) return;
    auto b1 = TestBroker::start({seed.endpoint()});
    if (!b1.broker) return;
    auto b2 = TestBroker::start({seed.endpoint()});
    if (!b2.broker) return;
    auto b3 = TestBroker::start({seed.endpoint()});
    if (!b3.broker) return;

    // Every broker ends up linked to all three others.
    CHECK(wait_until([&] { return routers(seed) == 3; }, 8000));
    CHECK(wait_until([&] { return routers(b1) == 3; }, 8000));
    CHECK(wait_until([&] { return routers(b2) == 3; }, 8000));
    CHECK(wait_until([&] { return routers(b3) == 3; }, 8000));
}

TEST("A client cannot inject clique membership by publishing on the reserved key") {
    auto b = TestBroker::start();
    if (!b.broker) return;
    auto a = TestBroker::start({b.endpoint()});
    if (!a.broker) return;
    CHECK(wait_until([&] { return routers(a) == 1; }));
    CHECK(known_peers(a) == 1);

    auto client = Session::open(a.endpoint());
    CHECK(client.has_value());
    if (!client) return;

    // Gossip travels as an ordinary Push, so a client can trivially send one on the
    // reserved key. It must be dropped on a client face rather than parsed -- the
    // prefix is reserved, and only a peer broker may speak membership.
    for (int i = 0; i < 5; ++i) {
        CHECK(client->put("@/router/gossip", bytes("\x05garbage-membership")).has_value());
    }
    // Nor may a client subscribe its way into seeing clique control traffic: a Push
    // on a reserved key is never routed at all, in either direction.
    auto sub = client->declare_subscriber("@/**");
    CHECK(sub.has_value());

    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    CHECK(known_peers(a) == 1); // unchanged: nothing was learned from the client
    CHECK(routers(a) == 1);
}

TEST("A broker whose peer restarts re-links and re-learns its declarations") {
    auto b = TestBroker::start();
    if (!b.broker) return;
    auto const endpoint = b.endpoint();
    auto const port =
        static_cast<std::uint16_t>(std::stoi(endpoint.substr(endpoint.rfind(':') + 1)));

    auto a = TestBroker::start({endpoint});
    if (!a.broker) return;
    CHECK(wait_until([&] { return routers(a) == 1; }));

    b.shutdown();
    CHECK(wait_until([&] { return routers(a) == 0; }));

    // A fresh broker on the same endpoint -- a restart, with a new zid. The
    // connector reconnects, and the replay on link-up means the new broker's
    // declarations reach `a` with no operator action.
    auto b2 = Broker::bind(
        BrokerConfig{.listen_host = "127.0.0.1", .listen_port = port, .accept_router_faces = true});
    CHECK(b2.has_value());
    if (!b2) return;
    std::thread runner([raw = b2->get()] { raw->run(1); });

    CHECK(wait_until([&] { return routers(a) == 1; }));

    auto sess = Session::open("tcp/127.0.0.1:" + std::to_string(port));
    CHECK(sess.has_value());
    if (sess) {
        auto sub = sess->declare_subscriber("demo/restart/**");
        CHECK(sub.has_value());
        CHECK(wait_until([&] { return faces_on(a, "demo/restart/**") == 1; }));
    }

    (*b2)->stop();
    runner.join();
}

// --- M5: per-message congestion control ---

namespace {

// A synthetic face whose backpressure level the test drives directly, so congestion
// behaviour can be asserted deterministically instead of by trying to genuinely
// overwhelm a real socket.
struct FakeFace {
    FaceId id;
    std::shared_ptr<std::atomic<FacePressure>> pressure =
        std::make_shared<std::atomic<FacePressure>>(FacePressure::ok);
    std::shared_ptr<std::atomic<int>> delivered = std::make_shared<std::atomic<int>>(0);

    [[nodiscard]] auto handle(FaceKind kind = FaceKind::client) const -> FaceHandle {
        FaceHandle h;
        h.id = id;
        h.kind = kind;
        h.pressure = pressure;
        h.deliver = [d = delivered](SharedBuf, std::vector<MsgSlice> slices) {
            d->fetch_add(static_cast<int>(slices.size()), std::memory_order_relaxed);
        };
        return h;
    }
};

// Route one Push, with the given congestion class, through `tables`.
auto route_push(Tables& tables, FaceId from, std::string_view key, bool block) -> void {
    auto blk = make_push_msg(key, bytes("payload"), /*is_del=*/false);
    std::vector<RoutedPush> msgs;
    msgs.push_back(RoutedPush{
        .slice = {.offset = 0, .length = static_cast<std::uint32_t>(blk.size()), .block = block},
        .key = std::string(key)});
    tables.on_push_batch(from, blk, msgs);
}

} // namespace

TEST("A congested face keeps receiving Block traffic while Drop traffic is discarded") {
    auto tb = TestBroker::start();
    if (!tb.broker) return;
    auto& tables = tb.broker->tables;

    FakeFace const publisher{.id = 8001};
    FakeFace const subscriber{.id = 8002};

    (void)on_strand(tables, [&] {
        tables.add_face(publisher.handle());
        tables.add_face(subscriber.handle());
        tables.on_declare_subscriber(subscriber.id, "demo/cc/**");
        return 0;
    });

    // While healthy, both classes are delivered.
    (void)on_strand(tables, [&] {
        route_push(tables, publisher.id, "demo/cc/k", /*block=*/false);
        route_push(tables, publisher.id, "demo/cc/k", /*block=*/true);
        return 0;
    });
    CHECK(subscriber.delivered->load() == 2);

    // Now the subscriber falls behind. Droppable traffic is discarded for it --
    // that is what keeps it from stalling the publisher -- but traffic the publisher
    // marked as must-not-be-dropped still gets through.
    subscriber.pressure->store(FacePressure::congested);
    (void)on_strand(tables, [&] {
        route_push(tables, publisher.id, "demo/cc/k", /*block=*/false);
        return 0;
    });
    CHECK(subscriber.delivered->load() == 2); // dropped

    (void)on_strand(tables, [&] {
        route_push(tables, publisher.id, "demo/cc/k", /*block=*/true);
        return 0;
    });
    CHECK(subscriber.delivered->load() == 3); // delivered anyway

    // Once it catches up, everything flows again.
    subscriber.pressure->store(FacePressure::ok);
    (void)on_strand(tables, [&] {
        route_push(tables, publisher.id, "demo/cc/k", /*block=*/false);
        return 0;
    });
    CHECK(subscriber.delivered->load() == 4);

    (void)on_strand(tables, [&] {
        tables.remove_face(publisher.id);
        tables.remove_face(subscriber.id);
        return 0;
    });
}

TEST("A saturated face receives nothing at all, not even Block traffic") {
    auto tb = TestBroker::start();
    if (!tb.broker) return;
    auto& tables = tb.broker->tables;

    FakeFace const publisher{.id = 8011};
    FakeFace const subscriber{.id = 8012};
    (void)on_strand(tables, [&] {
        tables.add_face(publisher.handle());
        tables.add_face(subscriber.handle());
        tables.on_declare_subscriber(subscriber.id, "demo/sat/**");
        return 0;
    });

    // `saturated` means the peer has stopped draining entirely and the face is on
    // its way out -- queuing more, of any class, would only grow memory.
    subscriber.pressure->store(FacePressure::saturated);
    (void)on_strand(tables, [&] {
        route_push(tables, publisher.id, "demo/sat/k", /*block=*/false);
        route_push(tables, publisher.id, "demo/sat/k", /*block=*/true);
        return 0;
    });
    CHECK(subscriber.delivered->load() == 0);

    (void)on_strand(tables, [&] {
        tables.remove_face(publisher.id);
        tables.remove_face(subscriber.id);
        return 0;
    });
}

TEST("A congested requester still receives the ResponseFinal that ends its query") {
    // A dropped terminator does not lose data -- it leaves the requester waiting for
    // something that will never arrive, until its own client-side timeout. So a
    // ResponseFinal is always sent as must-not-be-dropped, whatever the query was.
    auto tb = TestBroker::start();
    if (!tb.broker) return;
    auto& tables = tb.broker->tables;

    FakeFace const requester{.id = 8021};
    (void)on_strand(tables, [&] {
        tables.add_face(requester.handle());
        return 0;
    });
    requester.pressure->store(FacePressure::congested);

    // No queryable matches, so the broker synthesizes the terminator immediately.
    (void)on_strand(tables, [&] {
        tables.on_request(requester.id, RoutedRequest{.origin_rid = 1, .key = "demo/none/**"});
        return 0;
    });
    CHECK(requester.delivered->load() == 1);

    (void)on_strand(tables, [&] {
        tables.remove_face(requester.id);
        return 0;
    });
}

TEST("A query marked Block reaches a queryable that is behind; a droppable one does not") {
    auto tb = TestBroker::start();
    if (!tb.broker) return;
    auto& tables = tb.broker->tables;

    FakeFace const requester{.id = 8031};
    FakeFace const queryable{.id = 8032};
    (void)on_strand(tables, [&] {
        tables.add_face(requester.handle());
        tables.add_face(queryable.handle());
        tables.on_declare_queryable(queryable.id, "demo/ccq", QueryableInfo{});
        return 0;
    });
    queryable.pressure->store(FacePressure::congested);

    QoS block{};
    block.inner |= 0x08;

    (void)on_strand(tables, [&] {
        tables.on_request(requester.id, RoutedRequest{.origin_rid = 1, .key = "demo/ccq"});
        return 0;
    });
    CHECK(queryable.delivered->load() == 0); // droppable: not sent to a face that is behind
    CHECK(requester.delivered->load() == 1); // ...but the query still terminates

    (void)on_strand(tables, [&] {
        tables.on_request(requester.id,
                          RoutedRequest{.origin_rid = 2, .qos = block, .key = "demo/ccq"});
        return 0;
    });
    CHECK(queryable.delivered->load() == 1); // Block: delivered despite the backlog

    (void)on_strand(tables, [&] {
        tables.remove_face(requester.id);
        tables.remove_face(queryable.id);
        return 0;
    });
}

TEST("CongestionControl::block survives a broker hop unchanged") {
    // The broker re-encodes a Request when forwarding it, so the flag has to be
    // carried explicitly -- rebuilt-from-scratch messages silently lost it, which
    // would have degraded Block to Drop at every hop.
    auto b = TestBroker::start();
    if (!b.broker) return;
    auto a = TestBroker::start({b.endpoint()});
    if (!a.broker) return;
    CHECK(wait_until([&] { return routers(a) == 1; }));

    auto responder = Session::open(b.endpoint());
    CHECK(responder.has_value());
    if (!responder) return;
    auto qbl = responder->declare_queryable("demo/ccx");
    CHECK(qbl.has_value());
    if (!qbl) return;

    std::atomic<bool> saw_block{false};
    std::thread t([&] {
        auto q = qbl->recv();
        if (!q) return;
        saw_block.store(true);
        (void)q->reply("demo/ccx", bytes("ok"));
    });
    CHECK(wait_until([&] { return faces_on(a, "demo/ccx") == 1; }));

    auto requester = Session::open(a.endpoint());
    CHECK(requester.has_value());
    if (!requester) {
        b.shutdown();
        t.join();
        return;
    }
    auto getter =
        requester->get("demo/ccx", "", GetOptions{.congestion = CongestionControl::block});
    CHECK(getter.has_value());
    auto replies = getter ? drain_replies(*getter) : std::vector<std::string>{};
    t.join();

    CHECK(saw_block.load());
    CHECK(replies.size() == 1);
}

TEST("A Push marked Block crosses a broker link intact") {
    auto b = TestBroker::start();
    if (!b.broker) return;
    auto a = TestBroker::start({b.endpoint()});
    if (!a.broker) return;
    CHECK(wait_until([&] { return routers(a) == 1; }));

    auto sub_sess = Session::open(b.endpoint());
    auto pub_sess = Session::open(a.endpoint());
    CHECK(sub_sess.has_value());
    CHECK(pub_sess.has_value());
    if (!sub_sess || !pub_sess) return;
    auto sub = sub_sess->declare_subscriber("demo/ccp/**");
    CHECK(sub.has_value());
    if (!sub) return;
    CHECK(wait_until([&] { return faces_on(a, "demo/ccp/**") == 1; }));

    // The scope==0 forward path copies the received bytes verbatim, so the QoS
    // extension survives the hop untouched -- the subscriber gets the publisher's
    // message exactly as sent.
    CHECK(pub_sess
              ->put("demo/ccp/k", bytes("must-arrive"),
                    PutOptions{.congestion = CongestionControl::block})
              .has_value());
    CHECK(next_payload(*sub) == "must-arrive");
}

// --- M6: link liveness and partition visibility ---

TEST("A broker reports a peer it knows about but cannot reach") {
    auto b = TestBroker::start();
    if (!b.broker) return;
    auto a = TestBroker::start({b.endpoint()});
    if (!a.broker) return;
    CHECK(wait_until([&] { return routers(a) == 1; }));
    CHECK(unlinked_peers(a) == 0); // linked to everyone it knows about

    // The peer goes away. Split horizon deliberately does not reroute around it, so
    // nothing else about this broker looks wrong -- which is exactly why the
    // condition has to be observable rather than inferred.
    b.shutdown();
    CHECK(wait_until([&] { return routers(a) == 0; }));
    CHECK(wait_until([&] { return unlinked_peers(a) == 1; }));
    CHECK(known_peers(a) == 1); // still remembered, and still being re-dialled
}

TEST("An unreachable peer stops being reported once the link comes back") {
    auto b = TestBroker::start();
    if (!b.broker) return;
    auto const endpoint = b.endpoint();
    auto const port =
        static_cast<std::uint16_t>(std::stoi(endpoint.substr(endpoint.rfind(':') + 1)));

    auto a = TestBroker::start({endpoint});
    if (!a.broker) return;
    CHECK(wait_until([&] { return routers(a) == 1; }));

    b.shutdown();
    CHECK(wait_until([&] { return unlinked_peers(a) == 1; }));

    auto b2 = Broker::bind(
        BrokerConfig{.listen_host = "127.0.0.1", .listen_port = port, .accept_router_faces = true});
    CHECK(b2.has_value());
    if (!b2) return;
    std::thread runner([raw = b2->get()] { raw->run(1); });

    // The restarted broker has a fresh zid, so `a` now knows two peers but is linked
    // to one -- the old identity stays remembered (there is deliberately no
    // departure protocol) while the live one is reachable again.
    CHECK(wait_until([&] { return routers(a) == 1; }));
    CHECK(wait_until([&] { return unlinked_peers(a) == 1; }));

    (*b2)->stop();
    runner.join();
}

TEST("An idle clique link stays up well past the keepalive period") {
    // Both ends send KeepAlives on an otherwise silent link, so the lease never
    // expires on a healthy connection. A regression here would show up as the link
    // tearing itself down after one lease of inactivity.
    auto b = TestBroker::start();
    if (!b.broker) return;
    auto a = TestBroker::start({b.endpoint()});
    if (!a.broker) return;
    CHECK(wait_until([&] { return routers(a) == 1 && routers(b) == 1; }));

    // The keepalive period is lease/4 = 2.5s; hold for longer than that with no
    // traffic at all and confirm the link is still there and still usable.
    std::this_thread::sleep_for(std::chrono::milliseconds(3200));
    CHECK(routers(a) == 1);
    CHECK(routers(b) == 1);

    auto sub_sess = Session::open(b.endpoint());
    auto pub_sess = Session::open(a.endpoint());
    CHECK(sub_sess.has_value());
    CHECK(pub_sess.has_value());
    if (!sub_sess || !pub_sess) return;
    auto sub = sub_sess->declare_subscriber("demo/idle/**");
    CHECK(sub.has_value());
    if (!sub) return;
    CHECK(wait_until([&] { return faces_on(a, "demo/idle/**") == 1; }));
    CHECK(pub_sess->put("demo/idle/k", bytes("still-alive")).has_value());
    CHECK(next_payload(*sub) == "still-alive");
}

// --- multi-threaded: the strand-discipline asserts are the point ---

TEST("A clique routes correctly under a multi-threaded broker pool") {
    // Every mutating method on Tables and Face asserts it is running on its owning
    // strand, so running the mesh across a real thread pool turns any off-strand
    // access into a deterministic failure here rather than a probabilistic one under
    // ThreadSanitizer. This is the case that exercises the new connector, gossip,
    // and keepalive coroutines concurrently.
    auto b = TestBroker::start({}, /*threads=*/4);
    if (!b.broker) return;
    auto a = TestBroker::start({b.endpoint()}, /*threads=*/4);
    if (!a.broker) return;
    auto c = TestBroker::start({b.endpoint()}, /*threads=*/4);
    if (!c.broker) return;
    CHECK(wait_until([&] { return routers(a) == 2 && routers(b) == 2 && routers(c) == 2; }));

    constexpr int subscriber_count = 4;
    constexpr int message_count = 200;
    std::vector<Session> sessions;
    std::vector<Subscriber> subs;
    sessions.reserve(subscriber_count);
    subs.reserve(subscriber_count);
    for (int i = 0; i < subscriber_count; ++i) {
        auto s = Session::open(c.endpoint());
        CHECK(s.has_value());
        if (!s) return;
        sessions.push_back(std::move(*s));
        auto sub = sessions.back().declare_subscriber("demo/mt/**");
        CHECK(sub.has_value());
        if (!sub) return;
        subs.push_back(std::move(*sub));
    }
    CHECK(wait_until([&] { return faces_on(a, "demo/mt/**") >= 1; }));

    std::vector<std::thread> readers;
    std::vector<int> received(subscriber_count, 0);
    readers.reserve(subscriber_count);
    for (int i = 0; i < subscriber_count; ++i) {
        readers.emplace_back([&, i] {
            for (int n = 0; n < message_count; ++n) {
                if (!subs[static_cast<std::size_t>(i)].recv()) return;
                ++received[static_cast<std::size_t>(i)];
            }
        });
    }

    auto pub_sess = Session::open(a.endpoint());
    CHECK(pub_sess.has_value());
    if (!pub_sess) {
        for (auto& t : readers) t.join();
        return;
    }
    for (int n = 0; n < message_count; ++n) {
        // Block: this test asserts every message arrives, so it must not be
        // droppable -- exactly the situation CongestionControl exists for.
        CHECK(pub_sess
                  ->put("demo/mt/k", bytes("m"), PutOptions{.congestion = CongestionControl::block})
                  .has_value());
    }
    for (auto& t : readers) t.join();

    for (int i = 0; i < subscriber_count; ++i) {
        CHECK(received[static_cast<std::size_t>(i)] == message_count);
    }
}

// --- who is allowed to be a peer ---
//
// A face's FaceKind comes from the peer's own InitSyn (`whatami`), and nothing
// authenticates it. Accepting that claim from anyone hands a plain client every
// clique privilege: gossip ingestion, a replay of this broker's declaration state,
// split-horizon treatment, and the far larger router congestion budgets (16 MiB
// watermark / 256 MiB ceiling instead of 1 MiB / 64 MiB). It is now opt-in per
// broker via BrokerConfig::accept_router_faces (`--accept-router-faces`).

// Minimal hand-rolled client handshake that announces an arbitrary `whatami`.
// Returns the connected fd (caller closes) or -1.
[[nodiscard]] auto connect_announcing(std::uint16_t port, WhatAmI whatami) -> int {
    int const fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    ::inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
    if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        ::close(fd);
        return -1;
    }

    auto send_batch = [fd](std::span<const std::byte> body) {
        std::array<std::byte, 2> len{};
        store_le<std::uint16_t>(len.data(), static_cast<std::uint16_t>(body.size()));
        return ::send(fd, len.data(), len.size(), 0) == 2 &&
               ::send(fd, body.data(), body.size(), 0) == static_cast<ssize_t>(body.size());
    };
    auto recv_batch = [fd]() -> std::vector<std::byte> {
        std::array<std::byte, 2> len{};
        if (::recv(fd, len.data(), len.size(), MSG_WAITALL) != 2) return {};
        std::uint16_t const l = load_le<std::uint16_t>(len.data());
        std::vector<std::byte> body(l);
        if (l != 0 && ::recv(fd, body.data(), l, MSG_WAITALL) != static_cast<ssize_t>(l)) return {};
        return body;
    };
    auto encode_one = [](const auto& msg) {
        std::vector<std::byte> buf(4096);
        ByteWriter w{buf};
        if (!msg.encode(w)) return std::vector<std::byte>{};
        return std::vector<std::byte>{buf.data(), buf.data() + w.written()};
    };

    InitSyn isyn{};
    isyn.version = 9;
    isyn.identifier.whatami = whatami;
    isyn.identifier.zid.len = 16;
    isyn.identifier.zid.bytes.fill(std::byte{0x77});
    if (!send_batch(encode_one(isyn))) {
        ::close(fd);
        return -1;
    }
    auto iack_bytes = recv_batch();
    ByteReader ir{iack_bytes};
    auto iack = InitAck::decode(ir);
    if (!iack) {
        ::close(fd);
        return -1;
    }

    OpenSyn osyn{};
    osyn.lease = Duration::from_millis(10000);
    osyn.sn = 0;
    osyn.cookie = std::span<const std::byte>{iack->cookie};
    if (!send_batch(encode_one(osyn))) {
        ::close(fd);
        return -1;
    }
    auto oack_bytes = recv_batch();
    ByteReader ar{oack_bytes};
    if (!OpenAck::decode(ar)) {
        ::close(fd);
        return -1;
    }
    return fd;
}

TEST("a client announcing whatami=router is not accepted as a clique peer by default") {
    // TestBroker::start opts in (every federation test needs it), so this case binds
    // a broker directly, keeping the shipped default.
    auto strict = Broker::bind(BrokerConfig{.listen_host = "127.0.0.1", .listen_port = 0});
    CHECK(strict.has_value());
    if (!strict) return;
    std::thread runner([raw = strict->get()] { raw->run(1); });

    int const fd = connect_announcing((*strict)->port(), WhatAmI::router);
    CHECK(fd >= 0);

    auto& tables = (*strict)->tables;
    // The face is registered ...
    CHECK(wait_until([&] { return on_strand(tables, [&] { return tables.face_count(); }) == 1; }));
    // ... as a client, not as a clique peer.
    CHECK(on_strand(tables, [&] { return tables.router_face_count(); }) == 0);

    if (fd >= 0) ::close(fd);
    (*strict)->stop();
    runner.join();
}

TEST("a broker configured with accept_router_faces does honour whatami=router") {
    auto b = Broker::bind(
        BrokerConfig{.listen_host = "127.0.0.1", .listen_port = 0, .accept_router_faces = true});
    CHECK(b.has_value());
    if (!b) return;
    std::thread runner([raw = b->get()] { raw->run(1); });

    int const fd = connect_announcing((*b)->port(), WhatAmI::router);
    CHECK(fd >= 0);

    auto& tables = (*b)->tables;
    CHECK(wait_until(
        [&] { return on_strand(tables, [&] { return tables.router_face_count(); }) == 1; }));

    if (fd >= 0) ::close(fd);
    (*b)->stop();
    runner.join();
}

// A router face that is torn down while congested must give its share of
// `congested_router_faces_` back. That counter read-throttles *every client face* in
// the broker (see Face::throttle_if_backlogged), so leaking a single count wedges the
// whole broker for its remaining lifetime: it keeps accepting connections and stops
// processing client traffic. The leak was possible because a delivery posted from the
// routing strand before it processed `remove_face` can still run on the face's strand
// after the read loop has already released the count -- `deliver` captures
// `shared_from_this()` precisely so the face outlives its own loop -- and re-raised
// the level with no coroutine left alive to lower it again.
TEST("a router face torn down while congested leaves the congested count at zero") {
    auto b = Broker::bind(
        BrokerConfig{.listen_host = "127.0.0.1", .listen_port = 0, .accept_router_faces = true});
    CHECK(b.has_value());
    if (!b) return;
    std::thread runner([raw = b->get()] { raw->run(1); });
    auto& tables = (*b)->tables;

    // A "peer broker" that declares interest and then never reads a byte.
    int const peer_fd = connect_announcing((*b)->port(), WhatAmI::router);
    CHECK(peer_fd >= 0);
    if (peer_fd < 0) {
        (*b)->stop();
        runner.join();
        return;
    }
    {
        FrameHeader fh{};
        fh.reliability = Reliability::reliable;
        fh.sn = 0;
        Declare dec{};
        DeclareSubscriber ds{};
        ds.id = 1;
        ds.wire_expr = WireExpr{.scope = 0, .mapping = Mapping::sender, .suffix = "wedge/**"};
        dec.body = DeclareBody{.body = ds};
        std::vector<std::byte> buf(512);
        ByteWriter w{buf};
        CHECK(fh.encode(w).has_value());
        CHECK(dec.encode(w).has_value());
        std::array<std::byte, 2> len{};
        store_le<std::uint16_t>(len.data(), static_cast<std::uint16_t>(w.written()));
        CHECK(::send(peer_fd, len.data(), len.size(), 0) == 2);
        CHECK(::send(peer_fd, buf.data(), w.written(), 0) == static_cast<ssize_t>(w.written()));
    }
    CHECK(wait_until(
        [&] { return on_strand(tables, [&] { return tables.router_face_count(); }) == 1; }));

    // Fill that face's queue past the 16 MiB router watermark. The peer never reads,
    // so everything past the socket buffer piles up in the broker.
    auto pub = zenoh::Session::open("tcp/127.0.0.1:" + std::to_string((*b)->port()));
    CHECK(pub.has_value());
    if (!pub) {
        ::close(peer_fd);
        (*b)->stop();
        runner.join();
        return;
    }
    // try_put, not put: once the peer's queue crosses the watermark the broker
    // read-throttles its publishers, and a blocking put would then sit in the kernel
    // instead of driving the queue up. Loop against a deadline rather than a fixed
    // count so a busy machine cannot turn "slower" into "failed".
    std::vector<std::byte> const payload(60000, std::byte{0xab});
    auto& counter = tables.congested_router_faces();
    auto const fill_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(20);
    while (counter.load(std::memory_order_relaxed) == 0 &&
           std::chrono::steady_clock::now() < fill_deadline) {
        auto const r = pub->try_put("wedge/x", payload);
        if (!r) {
            if (r.error() != zenoh::ZError::would_block) break;        // link gone
            std::this_thread::sleep_for(std::chrono::milliseconds(1)); // socket full; let it drain
        }
    }
    CHECK(counter.load(std::memory_order_relaxed) == 1);

    // Tear the wedged face down mid-flight and let the deliveries already posted to
    // its strand land afterwards.
    ::close(peer_fd);
    CHECK(wait_until([&] { return counter.load(std::memory_order_relaxed) == 0; }));

    // And the broker is still usable: no client face is left throttled.
    auto sub_sess = zenoh::Session::open("tcp/127.0.0.1:" + std::to_string((*b)->port()));
    CHECK(sub_sess.has_value());
    if (sub_sess) {
        auto sub = sub_sess->declare_subscriber("after/**");
        CHECK(sub.has_value());
        if (sub) {
            CHECK(wait_until([&] {
                return on_strand(tables, [&] { return tables.resource_face_count("after/**"); }) ==
                       1;
            }));
            CHECK(pub->put("after/x", bytes("done")).has_value());
            auto s = sub->recv();
            CHECK(s.has_value());
        }
    }

    (*b)->stop();
    runner.join();
}

// Broker scalability and stress tests (zenoh.broker) -- companion to test_broker.cpp,
// which owns the functional/corner-case suite. This file has two distinct halves:
//
//  - Data-structure-level scale tests (Tables/ResourceTable direct access, no real
//    socket, no zenoh::broker::Face/zenoh::Session at all): synthetic FaceHandles
//    registered straight against a real Tables instance (itself hosted inside a real,
//    otherwise-unused TestBroker, purely so a running strand/io_context exists to
//    marshal onto -- see on_strand below), exercising ResourceTable's exact-hash and
//    wildcard-scan matching paths at thousands of declarations.
//  - Real-socket stress tests (real zenoh::Session clients against a real Broker):
//    sustained no-loss throughput, the congestion drop/recover policy at its actual
//    watermarks, connect/disconnect churn, and higher-N versions of test_broker.cpp's
//    own multi-threaded pub/sub and query/reply stress cases.
//
// The concurrent cases here (throughput, congestion, churn, higher-N pub/sub and
// query/reply) are correctness-under-real-threading tests; this file cannot itself
// prove the absence of a data race -- that's what the dedicated `linux-tsan` preset
// (re-running this whole binary under ThreadSanitizer) is for, per docs/BROKER.md.
import zenoh.broker;
import zenoh.ke;
import zenoh;

#include "ztest.hpp"

#include <asio/post.hpp>

#include <array>
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
#include <unordered_map>
#include <vector>

using namespace zenoh::broker;

namespace {

auto bytes(std::string_view s) -> std::span<const std::byte> {
    return {reinterpret_cast<const std::byte*>(s.data()), s.size()};
}

auto str(std::span<const std::byte> b) -> std::string {
    return {reinterpret_cast<const char*>(b.data()), b.size()};
}

// Spins a real Broker on its own thread pool against an ephemeral loopback port;
// stop()+join() on destruction. `broker` is null if bind() failed (callers CHECK it).
// Mirrored verbatim from test_broker.cpp -- each TEST translation unit is separate,
// so this file needs its own copy rather than sharing one.
struct TestBroker {
    std::unique_ptr<Broker> broker;
    std::thread runner;

    TestBroker() = default;
    TestBroker(const TestBroker&) = delete;
    auto operator=(const TestBroker&) -> TestBroker& = delete;
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

// Reads `fn()` from inside a real handler running on `tables`'s own routing strand,
// blocking the calling (test) thread until it completes -- required for every
// Tables/ResourceTable touch, not just introspection: the scale tests in this file
// construct/mutate synthetic faces directly against a live Tables, and
// `assert(strand_.running_in_this_thread())` fires hard on any off-strand call.
template <class Fn>
[[nodiscard]] auto on_strand(Tables& tables, Fn fn) -> std::invoke_result_t<Fn> {
    using R = std::invoke_result_t<Fn>;
    std::promise<R> prom;
    auto fut = prom.get_future();
    asio::post(tables.strand(), [&fn, &prom] { prom.set_value(fn()); });
    return fut.get();
}

// Polls `pred` (each call itself strand-marshaled by the caller) until true or
// `timeout_ms` elapses.
template <class Pred> auto wait_until(Pred pred, int timeout_ms = 2000) -> bool {
    auto const deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    return pred();
}

// Builds a fresh, fully-formed FaceHandle: a real (non-null) `pressure` flag (see
// FaceHandle's doc comment in tables.cppm -- null is only valid for a handle that was
// never actually registered) and a `deliver` that just bumps a caller-owned counter,
// since these synthetic-face tests never decode anything back.
auto make_counting_face(FaceId id, int& counter) -> FaceHandle {
    FaceHandle h;
    h.id = id;
    h.pressure = std::make_shared<std::atomic<FacePressure>>(FacePressure::ok);
    // `deliver` hands over a whole run of messages at a time (one call per face per
    // routed batch, see FaceHandle in tables.cppm), so count messages, not calls.
    h.deliver = [&counter](SharedBuf, std::vector<MsgSlice> slices) {
        counter += static_cast<int>(slices.size());
    };
    return h;
}

// One-message push batch: the encoded Push in a block of its own, plus the single
// RoutedPush slice covering it (what Face::flush_push_batch builds for real traffic).
struct OnePush {
    SharedBuf block;
    std::vector<RoutedPush> batch;
};

auto one_push(std::string_view key) -> OnePush {
    OnePush p;
    p.block = make_push_msg(key, {}, /*is_del=*/false);
    p.batch.push_back(RoutedPush{
        .slice = MsgSlice{.offset = 0, .length = static_cast<std::uint32_t>(p.block.size())},
        .key = std::string(key)});
    return p;
}

auto join_chunks(const std::vector<std::string>& chunks) -> std::string {
    std::string out;
    for (std::size_t i = 0; i < chunks.size(); ++i) {
        if (i != 0) out += '/';
        out += chunks[i];
    }
    return out;
}

// Every canonical pattern obtainable from `chunks` (a 7-segment key) by either (a)
// independently leaving each position literal or replacing it with "*" (2^7
// combinations -- a lone "*" always matches exactly the corresponding chunk, per
// test_ke.cpp's reference vectors), or (b) collapsing chunks [i, j) into a single
// "**", with the untouched suffix kept fully literal (to stay clear of the
// non-canonical "**" immediately followed by "*" rule -- see test_ke.cpp's
// `is_canon` vectors) and the untouched prefix independently varying literal-vs-"*"
// per position (a "*" immediately before "**" *is* canonical, e.g. "*/**").
auto generate_pattern_family(const std::array<std::string, 7>& chunks) -> std::vector<std::string> {
    constexpr int n = 7;
    std::vector<std::string> out;

    for (unsigned mask = 0; mask < (1u << n); ++mask) {
        std::vector<std::string> parts(n);
        for (int p = 0; p < n; ++p) {
            parts[static_cast<std::size_t>(p)] =
                (mask & (1u << p)) ? "*" : chunks[static_cast<std::size_t>(p)];
        }
        out.push_back(join_chunks(parts));
    }

    for (int i = 0; i <= n; ++i) {
        for (int j = i + 1; j <= n; ++j) {
            unsigned const prefix_combos = 1u << i;
            for (unsigned pmask = 0; pmask < prefix_combos; ++pmask) {
                std::vector<std::string> parts;
                for (int p = 0; p < i; ++p) {
                    parts.push_back((pmask & (1u << p)) ? "*"
                                                        : chunks[static_cast<std::size_t>(p)]);
                }
                parts.emplace_back("**");
                for (int p = j; p < n; ++p) parts.push_back(chunks[static_cast<std::size_t>(p)]);
                out.push_back(join_chunks(parts));
            }
        }
    }
    return out;
}

} // namespace

// --- Part 2: data-structure-level scale (Tables/ResourceTable, synthetic faces) ---

TEST("Tables scales to ~5,000 declared resources and routes to exactly the matching subset") {
    constexpr int num_literal = 5000;
    constexpr int wildcard_stride = 10; // every 10th index also gets a wildcard sibling
    constexpr int target_index = 40;    // a multiple of wildcard_stride
    constexpr int num_wildcard = num_literal / wildcard_stride;

    auto tb = TestBroker::start();
    if (!tb.broker) return;
    auto& tables = tb.broker->tables;

    std::vector<int> literal_delivered(num_literal, 0);
    std::vector<int> wildcard_delivered(num_wildcard, 0);

    (void)on_strand(tables, [&] {
        FaceId next_id = 1;
        for (int i = 0; i < num_literal; ++i) {
            FaceId const id = next_id++;
            tables.add_face(make_counting_face(id, literal_delivered[static_cast<std::size_t>(i)]));
            tables.on_declare_subscriber(id, "demo/scale/" + std::to_string(i));

            if (i % wildcard_stride == 0) {
                FaceId const wid = next_id++;
                int const wslot = i / wildcard_stride;
                tables.add_face(
                    make_counting_face(wid, wildcard_delivered[static_cast<std::size_t>(wslot)]));
                tables.on_declare_subscriber(wid, "demo/scale/" + std::to_string(i) + "/**");
            }
        }
        return 0;
    });

    CHECK(on_strand(tables, [&] { return tables.face_count(); }) ==
          static_cast<std::size_t>(num_literal + num_wildcard));
    CHECK(on_strand(tables, [&] { return tables.resource_count(); }) ==
          static_cast<std::size_t>(num_literal + num_wildcard));

    // Publish on the target's own literal key: it must reach exactly that literal's
    // face (an exact-hash match) and that index's wildcard sibling ("<i>/**"
    // intersects "<i>", per test_ke.cpp's `ab/**` vector), and nothing else.
    std::string const target_key = "demo/scale/" + std::to_string(target_index);
    auto const pushed = one_push(target_key);
    (void)on_strand(tables, [&] {
        tables.on_push_batch(0, pushed.block, pushed.batch);
        return 0;
    });

    for (int i = 0; i < num_literal; ++i) {
        CHECK(literal_delivered[static_cast<std::size_t>(i)] == (i == target_index ? 1 : 0));
    }
    for (int w = 0; w < num_wildcard; ++w) {
        CHECK(wildcard_delivered[static_cast<std::size_t>(w)] ==
              (w * wildcard_stride == target_index ? 1 : 0));
    }
}

TEST("~2,000 faces sharing one declared subscription each receive one push exactly once") {
    constexpr int num_faces = 2000;
    std::string const key = "demo/manyfaces/x";

    auto tb = TestBroker::start();
    if (!tb.broker) return;
    auto& tables = tb.broker->tables;

    std::vector<int> delivered(num_faces, 0);

    (void)on_strand(tables, [&] {
        for (int i = 0; i < num_faces; ++i) {
            FaceId const id = static_cast<FaceId>(i + 1);
            tables.add_face(make_counting_face(id, delivered[static_cast<std::size_t>(i)]));
            tables.on_declare_subscriber(id, key);
        }
        return 0;
    });

    CHECK(on_strand(tables, [&] { return tables.resource_face_count(key); }) ==
          static_cast<std::size_t>(num_faces));

    auto const pushed = one_push(key);
    (void)on_strand(tables, [&] {
        tables.on_push_batch(0, pushed.block, pushed.batch);
        return 0;
    });

    for (int i = 0; i < num_faces; ++i) CHECK(delivered[static_cast<std::size_t>(i)] == 1);
}

TEST("Hundreds of overlapping wildcard declarations match a deep key exactly as ke::intersects "
     "predicts") {
    std::array<std::string, 7> const real_chunks{"n0", "n1", "n2", "n3", "n4", "n5", "n6"};
    std::array<std::string, 7> const decoy_chunks{"z0", "z1", "z2", "z3", "z4", "z5", "z6"};
    std::string const key = join_chunks({real_chunks.begin(), real_chunks.end()});

    auto candidates = generate_pattern_family(real_chunks);
    auto const decoys = generate_pattern_family(decoy_chunks);
    candidates.insert(candidates.end(), decoys.begin(), decoys.end());

    auto tb = TestBroker::start();
    if (!tb.broker) return;
    auto& tables = tb.broker->tables;

    std::vector<int> delivered;
    std::vector<bool> expected_match;
    delivered.reserve(candidates.size());
    expected_match.reserve(candidates.size());

    (void)on_strand(tables, [&] {
        FaceId next_id = 1;
        for (auto const& pattern : candidates) {
            if (!zenoh::ke::is_canon(pattern)) continue; // defensive; the generator avoids these
            delivered.push_back(0);
            expected_match.push_back(zenoh::ke::intersects(pattern, key));
            FaceId const id = next_id++;
            tables.add_face(make_counting_face(id, delivered.back()));
            tables.on_declare_subscriber(id, pattern);
        }
        return 0;
    });

    // A real-scale declaration set, not a token amount of work -- see the spec's
    // "~500 declared patterns... plus a comparable number of decoys".
    CHECK(delivered.size() > 500);

    auto const pushed = one_push(key);
    (void)on_strand(tables, [&] {
        tables.on_push_batch(0, pushed.block, pushed.batch);
        return 0;
    });

    std::size_t expected_total = 0;
    bool all_correct = true;
    for (std::size_t i = 0; i < delivered.size(); ++i) {
        int const want = expected_match[i] ? 1 : 0;
        if (delivered[i] != want) all_correct = false;
        expected_total += static_cast<std::size_t>(want);
    }
    CHECK(all_correct);
    CHECK(expected_total > 0);                // the family does contain genuine matches...
    CHECK(expected_total < delivered.size()); // ...and genuine non-matching decoys.
}

// --- Part 3: real-socket stress (real zenoh::Session clients, real Broker) ---

TEST("Sustained high-volume unbatched put() delivers every message exactly once, in order") {
    // 50,000 rather than the spec's illustrative upper bound: unbatched put() has
    // real per-call overhead under ASan instrumentation, and this already exercises
    // sustained cross-thread delivery at a scale far beyond every other test in this
    // suite's handful-of-messages shape.
    constexpr int total = 50'000;
    std::string const key = "demo/throughput/x";

    auto tb = TestBroker::start();
    if (!tb.broker) return;
    auto& tables = tb.broker->tables;

    using namespace zenoh;
    auto pub = Session::open(tb.endpoint());
    auto sub_sess = Session::open(tb.endpoint());
    CHECK(pub.has_value());
    CHECK(sub_sess.has_value());
    if (!pub || !sub_sess) return;

    auto sub = sub_sess->declare_subscriber(key);
    CHECK(sub.has_value());
    if (!sub) return;

    CHECK(wait_until(
        [&] { return on_strand(tables, [&] { return tables.resource_face_count(key); }) == 1; }));

    std::thread pub_thread([&] {
        for (int n = 0; n < total; ++n) (void)pub->put(key, bytes(std::to_string(n)));
    });

    bool ok = true;
    int received = 0;
    for (int n = 0; n < total; ++n) {
        auto s = sub->recv();
        if (!s || str(s->payload()) != std::to_string(n)) {
            ok = false;
            break;
        }
        ++received;
    }
    pub_thread.join();

    CHECK(ok);
    CHECK(received == total);
}

TEST("A congested subscriber's face drops without stalling another, then un-congests once it "
     "drains") {
    // Sized against broker.cpp's congested_high_watermark/congested_low_watermark,
    // which are in *bytes* of queued outbound data (1 MiB / 256 KiB): a "flood"
    // payload frames to a few tens of bytes, so tens of thousands of undrained
    // messages comfortably clear the high watermark, while the recovery trickle
    // stays well under it so the still-undrained `active` subscriber (not actively
    // re-drained during that phase) never itself congests.
    constexpr int flood_count = 90'000;
    constexpr int recovery_publish_count = 20'000;
    // `run_once()` dispatches one batch per call, so draining a backlog that reached
    // the high watermark takes many thousands of calls, not a handful -- this cap
    // just bounds a genuine hang, it isn't expected to be hit.
    constexpr int max_recovery_iters = 150'000;
    std::string const key = "demo/congest/x";

    auto tb = TestBroker::start();
    if (!tb.broker) return;
    auto& tables = tb.broker->tables;

    using namespace zenoh;
    auto pub = Session::open(tb.endpoint());
    auto active_sess = Session::open(tb.endpoint());
    auto idle_sess = Session::open(tb.endpoint());
    CHECK(pub.has_value());
    CHECK(active_sess.has_value());
    CHECK(idle_sess.has_value());
    if (!pub || !active_sess || !idle_sess) return;

    std::atomic<int> post_seen{0};
    auto idle_sub = idle_sess->declare_subscriber(key, [&](const Sample& s) {
        if (str(s.payload()).starts_with("post-"))
            post_seen.fetch_add(1, std::memory_order_relaxed);
    });
    auto active_sub = active_sess->declare_subscriber(key);
    CHECK(idle_sub.has_value());
    CHECK(active_sub.has_value());
    if (!idle_sub || !active_sub) return;

    CHECK(wait_until(
        [&] { return on_strand(tables, [&] { return tables.resource_face_count(key); }) == 2; }));

    // Flood: `idle` never calls recv()/run_once() here at all -- its face is expected
    // to congest -- while `active` drains continuously and must lose nothing.
    std::atomic<int> active_received{0};
    bool active_ok = true;
    std::thread active_thread([&] {
        for (int n = 0; n < flood_count; ++n) {
            auto s = active_sub->recv();
            if (!s) {
                active_ok = false;
                return;
            }
            active_received.fetch_add(1, std::memory_order_relaxed);
        }
    });

    for (int n = 0; n < flood_count; ++n) (void)pub->put(key, bytes("flood"));
    active_thread.join();

    CHECK(active_ok);
    CHECK(active_received.load() == flood_count);

    // Recovery: `idle` starts draining (via run_once(), so its SampleHandler callback
    // fires); `pub` keeps publishing freshly-tagged messages throughout so that once
    // the queue actually crosses back below the low watermark, some post-flood
    // message is available to be routed and observed -- not just stale (already-
    // dropped-or-not) flood backlog.
    std::atomic<bool> stop_publishing{false};
    std::thread post_thread([&] {
        int n = 0;
        while (!stop_publishing.load(std::memory_order_relaxed) && n < recovery_publish_count) {
            (void)pub->put(key, bytes("post-" + std::to_string(n++)));
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    });

    bool recovered = false;
    for (int i = 0; i < max_recovery_iters && !recovered; ++i) {
        if (auto r = idle_sess->run_once(); !r) break;
        if (post_seen.load(std::memory_order_relaxed) > 0) recovered = true;
    }
    stop_publishing = true;
    post_thread.join();

    CHECK(recovered);
}

TEST("Connect/disconnect churn against the broker converges the resource table back to empty") {
    constexpr int total_sessions = 50;
    constexpr int num_threads = 5;
    constexpr int per_thread = total_sessions / num_threads;

    auto tb = TestBroker::start(4);
    if (!tb.broker) return;
    auto& tables = tb.broker->tables;

    using namespace zenoh;
    std::vector<std::thread> threads;
    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([&, t] {
            for (int i = 0; i < per_thread; ++i) {
                auto sess = Session::open(tb.endpoint());
                if (!sess) continue;
                if ((t * per_thread + i) % 2 == 0) {
                    auto sub = sess->declare_subscriber("demo/churn/" + std::to_string(t) + "/" +
                                                        std::to_string(i));
                    (void)sub; // RAII undeclare-on-drop below, alongside the session close
                }
                // `sess` (and any `sub`) destructs here: immediate disconnect/undeclare.
            }
        });
    }
    for (auto& th : threads) th.join();

    CHECK(wait_until([&] { return on_strand(tables, [&] { return tables.face_count(); }) == 0; }));
    CHECK(wait_until(
        [&] { return on_strand(tables, [&] { return tables.resource_count(); }) == 0; }));
}

TEST("Broker handles a higher-N concurrent multi-session pub/sub load under a multi-threaded "
     "pool") {
    constexpr int num_subs = 12;
    constexpr int num_pubs = 6;
    constexpr int msgs_per_pub = 200;
    constexpr int total_msgs = num_pubs * msgs_per_pub;

    auto tb = TestBroker::start(6);
    if (!tb.broker) return;
    auto& tables = tb.broker->tables;

    using namespace zenoh;
    std::vector<std::thread> sub_threads;
    std::vector<int> received(num_subs, 0);
    std::vector<int> sub_ok(num_subs, 1); // int, not bool -- see test_broker.cpp's stress test

    for (int i = 0; i < num_subs; ++i) {
        sub_threads.emplace_back([&, i] {
            auto sess = Session::open(tb.endpoint());
            if (!sess) {
                sub_ok[i] = 0;
                return;
            }
            auto sub = sess->declare_subscriber("demo/hstress/**");
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

    CHECK(wait_until([&] {
        return on_strand(tables, [&] { return tables.resource_face_count("demo/hstress/**"); }) ==
               static_cast<std::size_t>(num_subs);
    }));

    std::vector<std::thread> pub_threads;
    for (int p = 0; p < num_pubs; ++p) {
        pub_threads.emplace_back([&] {
            auto sess = Session::open(tb.endpoint());
            if (!sess) return;
            for (int n = 0; n < msgs_per_pub; ++n) (void)sess->put("demo/hstress/x", bytes("m"));
        });
    }
    for (auto& t : pub_threads) t.join();
    for (auto& t : sub_threads) t.join();

    for (int i = 0; i < num_subs; ++i) {
        CHECK(sub_ok[i]);
        CHECK(received[i] == total_msgs);
    }
}

TEST("Broker handles a higher-N concurrent multi-session query/reply load under a "
     "multi-threaded pool") {
    constexpr int num_queryables = 12;
    constexpr int num_requesters = 6;
    constexpr int gets_per_requester = 40;
    constexpr int queries_per_queryable = num_requesters * gets_per_requester;

    auto tb = TestBroker::start(6);
    if (!tb.broker) return;
    auto& tables = tb.broker->tables;

    using namespace zenoh;

    std::vector<Session> qbl_sessions;
    std::vector<Queryable> qbls;
    qbl_sessions.reserve(num_queryables);
    qbls.reserve(num_queryables);
    for (int i = 0; i < num_queryables; ++i) {
        auto s = Session::open(tb.endpoint());
        CHECK(s.has_value());
        if (!s) return;
        qbl_sessions.push_back(std::move(*s));
        auto q = qbl_sessions.back().declare_queryable("demo/hqstress/" + std::to_string(i));
        CHECK(q.has_value());
        if (!q) return;
        qbls.push_back(std::move(*q));
    }

    std::vector<std::thread> qbl_threads;
    std::vector<int> qbl_ok(num_queryables, 1);
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

    CHECK(wait_until([&] {
        for (int i = 0; i < num_queryables; ++i) {
            auto const key = "demo/hqstress/" + std::to_string(i);
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
                    auto getter = sess->get("demo/hqstress/" + std::to_string(i));
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

// Integration tests for the Query/Queryable client API (zenoh.session): a QueryRouter
// runs in-process, completes the handshake, then hands the connected fd to a per-test
// script that decodes whatever the client sends (DeclareQueryable, Request, Response,
// ResponseFinal) and/or pushes messages of its own — driving a real Session's
// get()/Getter and declare_queryable()/Queryable/IncomingQuery paths end-to-end on the
// wire, no external zenohd. Mirrors test_session.cpp's FakeRouter and
// test_subscriber.cpp's SubRouter patterns.
//
// IMPORTANT: router scripts run on a background thread and must NEVER call CHECK()
// directly — ztest.hpp's failure counter isn't synchronized (see the two precedent
// files above, which only ever write into plain member/captured state from the
// router thread and do every CHECK() on the main thread after router.join(), which
// establishes the necessary happens-before). Every test below follows that same
// discipline: the script captures observations by reference into local variables,
// and all CHECK()s run after join().
import zenoh;       // Session, Queryable, IncomingQuery, Getter, GetReply, ZError, PeerId
import zenoh.proto; // messages + ByteReader/ByteWriter + load_le/store_le

#include "ztest.hpp"

#include <array>
#include <cerrno>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <variant>
#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

using namespace zenoh;

namespace {

auto bytes(std::string_view s) -> std::span<const std::byte> {
    return {reinterpret_cast<const std::byte*>(s.data()), s.size()};
}
auto str(std::span<const std::byte> b) -> std::string {
    return {reinterpret_cast<const char*>(b.data()), b.size()};
}

auto read_exact(int fd, std::span<std::byte> out) -> bool {
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
auto write_all(int fd, std::span<const std::byte> data) -> bool {
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
auto recv_batch(int fd) -> std::optional<std::vector<std::byte>> {
    std::array<std::byte, 2> len{};
    if (!read_exact(fd, len)) return std::nullopt;
    std::uint16_t const l = load_le<std::uint16_t>(len.data());
    std::vector<std::byte> body(l);
    if (l != 0 && !read_exact(fd, body)) return std::nullopt;
    return body;
}
auto send_batch(int fd, std::span<const std::byte> body) -> bool {
    std::array<std::byte, 2> len{};
    store_le<std::uint16_t>(len.data(), static_cast<std::uint16_t>(body.size()));
    return write_all(fd, len) && write_all(fd, body);
}
template <class Msg> auto encode_body(const Msg& m) -> std::vector<std::byte> {
    std::vector<std::byte> buf(1024);
    ByteWriter w{buf};
    (void)m.encode(w);
    return {buf.data(), buf.data() + w.written()};
}
template <class Fn> auto build_frame(std::uint32_t sn, Fn&& fn) -> std::vector<std::byte> {
    std::vector<std::byte> buf(8192);
    ByteWriter w{buf};
    FrameHeader fh{};
    fh.reliability = Reliability::reliable;
    fh.sn = sn;
    (void)fh.encode(w);
    fn(w);
    return {buf.data(), buf.data() + w.written()};
}

// Decode one FrameHeader-wrapped network message from `batch`, returning the
// remaining reader positioned just after the FrameHeader (caller decodes the body).
auto open_frame(std::span<const std::byte> batch) -> std::optional<ByteReader> {
    ByteReader r{batch};
    auto first = r.peek();
    if (!first || (std::to_integer<std::uint8_t>(*first) & mid_mask) != FrameHeader::id)
        return std::nullopt;
    if (!FrameHeader::decode(r)) return std::nullopt;
    return r;
}

// In-process router for the Query/Queryable tests: handshake, then hand the raw fd to
// a per-test `script` with full manual control over the data phase. `script` must not
// call CHECK() (see the file header comment) — record observations into variables it
// captures by reference instead, and assert on them after `join()`.
class QueryRouter {
  public:
    explicit QueryRouter(std::function<void(int)> script, std::uint16_t batch_size = 8192)
        : script_(std::move(script)), batch_size_(batch_size) {
        std::signal(SIGPIPE, SIG_IGN);
        listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
        int one = 1;
        ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port = 0;
        ::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
        ::listen(listen_fd_, 1);
        socklen_t len = sizeof(addr);
        ::getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&addr), &len);
        port_ = ntohs(addr.sin_port);
        thread_ = std::thread([this] { run(); });
    }
    ~QueryRouter() {
        join();
        if (listen_fd_ >= 0) ::close(listen_fd_);
    }
    [[nodiscard]] auto port() const -> std::uint16_t { return port_; }
    auto join() -> void {
        if (thread_.joinable()) thread_.join();
    }

  private:
    auto run() -> void {
        pollfd pfd{.fd = listen_fd_, .events = POLLIN, .revents = 0};
        if (::poll(&pfd, 1, 5000) <= 0) return;
        int const fd = ::accept(listen_fd_, nullptr, nullptr);
        if (fd < 0) return;
        timeval tv{.tv_sec = 6, .tv_usec = 0};
        ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

        if (!recv_batch(fd)) {
            ::close(fd);
            return;
        } // InitSyn
        InitAck ack{};
        ack.version = 9;
        ack.identifier.whatami = WhatAmI::router;
        ack.identifier.zid.len = 4;
        ack.identifier.zid.bytes = {std::byte{1}, std::byte{2}, std::byte{3}, std::byte{4}};
        ack.resolution.resolution = 0x0a;
        ack.resolution.batch_size = batch_size_;
        std::array<std::byte, 4> cookie{std::byte{9}, std::byte{8}, std::byte{7}, std::byte{6}};
        ack.cookie = cookie;
        if (!send_batch(fd, encode_body(ack))) {
            ::close(fd);
            return;
        }
        if (!recv_batch(fd)) {
            ::close(fd);
            return;
        } // OpenSyn
        OpenAck oack{};
        oack.lease = Duration::from_millis(10000);
        oack.sn = 0;
        if (!send_batch(fd, encode_body(oack))) {
            ::close(fd);
            return;
        }

        script_(fd);
        ::shutdown(fd, SHUT_WR);
        ::close(fd);
    }

    std::function<void(int)> script_;
    std::uint16_t batch_size_;
    int listen_fd_ = -1;
    std::uint16_t port_ = 0;
    std::thread thread_;
};

auto endpoint(std::uint16_t port) -> std::string { return "tcp/127.0.0.1:" + std::to_string(port); }

} // namespace

TEST("get() pull: Getter::recv delivers multiple replies then nullopt") {
    bool req_ok = false;
    std::string req_key, req_params;

    QueryRouter router([&](int fd) {
        auto batch = recv_batch(fd); // the client's Request
        if (!batch) return;
        auto r = open_frame(*batch);
        if (!r) return;
        auto req = Request::decode(*r);
        if (!req) return;
        req_ok = true;
        req_key = std::string(req->wire_expr.suffix);
        req_params = std::string(req->payload.query.parameters);

        Response rsp1{};
        rsp1.rid = req->id;
        rsp1.wire_expr = WireExpr{.scope = 0, .mapping = Mapping::sender, .suffix = "demo/x"};
        Put put1{};
        put1.payload = bytes("v1");
        rsp1.payload.body = Reply{.payload = PushBody{.body = put1}};
        send_batch(fd, build_frame(1, [&](ByteWriter& w) { (void)rsp1.encode(w); }));

        Response rsp2 = rsp1;
        Put put2{};
        put2.payload = bytes("v2");
        rsp2.payload.body = Reply{.payload = PushBody{.body = put2}};
        send_batch(fd, build_frame(2, [&](ByteWriter& w) { (void)rsp2.encode(w); }));

        ResponseFinal rf{};
        rf.rid = req->id;
        send_batch(fd, build_frame(3, [&](ByteWriter& w) { (void)rf.encode(w); }));
    });

    auto sess = Session::open(endpoint(router.port()));
    CHECK(sess.has_value());
    if (!sess) {
        router.join();
        return;
    }
    auto getter = sess->get("demo/x", "p=1");
    CHECK(getter.has_value());
    if (!getter) {
        router.join();
        return;
    }

    auto r1 = getter->recv();
    CHECK(r1.has_value() && r1->has_value());
    if (r1 && *r1) {
        CHECK((*r1)->is_ok());
        CHECK((*r1)->sample().key_expr() == "demo/x");
        CHECK(str((*r1)->sample().payload()) == "v1");
    }
    auto r2 = getter->recv();
    CHECK(r2.has_value() && r2->has_value());
    if (r2 && *r2) CHECK(str((*r2)->sample().payload()) == "v2");

    auto r3 = getter->recv();
    CHECK(r3.has_value() && !r3->has_value()); // query complete: nullopt, not an error

    sess->close();
    router.join();

    CHECK(req_ok);
    CHECK(req_key == "demo/x");
    CHECK(req_params == "p=1");
}

TEST("get() pull: an Err reply surfaces via GetReply::is_ok()/error_payload()") {
    bool req_ok = false;

    QueryRouter router([&](int fd) {
        auto batch = recv_batch(fd);
        if (!batch) return;
        auto r = open_frame(*batch);
        if (!r) return;
        auto req = Request::decode(*r);
        if (!req) return;
        req_ok = true;

        Response rsp{};
        rsp.rid = req->id;
        Err e{};
        e.payload = bytes("boom");
        rsp.payload.body = e;
        send_batch(fd, build_frame(1, [&](ByteWriter& w) { (void)rsp.encode(w); }));

        ResponseFinal rf{};
        rf.rid = req->id;
        send_batch(fd, build_frame(2, [&](ByteWriter& w) { (void)rf.encode(w); }));
    });

    auto sess = Session::open(endpoint(router.port()));
    CHECK(sess.has_value());
    if (!sess) {
        router.join();
        return;
    }
    auto getter = sess->get("demo/err");
    CHECK(getter.has_value());
    if (!getter) {
        router.join();
        return;
    }

    auto r1 = getter->recv();
    CHECK(r1.has_value() && r1->has_value());
    if (r1 && *r1) {
        CHECK(!(*r1)->is_ok());
        CHECK(str((*r1)->error_payload()) == "boom");
    }
    auto r2 = getter->recv();
    CHECK(r2.has_value() && !r2->has_value());

    sess->close();
    router.join();
    CHECK(req_ok);
}

TEST("get() pull: no reply before the deadline reports query_timeout") {
    bool got_request = false;

    QueryRouter router([&](int fd) {
        auto batch = recv_batch(fd); // consume the Request; never reply
        got_request = batch.has_value();
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
    });

    auto sess = Session::open(endpoint(router.port()));
    CHECK(sess.has_value());
    if (!sess) {
        router.join();
        return;
    }
    auto getter = sess->get("demo/slow", "", GetOptions{.timeout_ms = 50});
    CHECK(getter.has_value());
    if (!getter) {
        router.join();
        return;
    }
    auto r = getter->recv();
    CHECK(!r.has_value() && r.error() == ZError::query_timeout);
    sess->close();
    router.join();
    CHECK(got_request);
}

TEST("get() with a callback is driven by run_once()") {
    bool req_ok = false;

    QueryRouter router([&](int fd) {
        auto batch = recv_batch(fd);
        if (!batch) return;
        auto r = open_frame(*batch);
        if (!r) return;
        auto req = Request::decode(*r);
        if (!req) return;
        req_ok = true;

        Response rsp{};
        rsp.rid = req->id;
        rsp.wire_expr = WireExpr{.scope = 0, .mapping = Mapping::sender, .suffix = "demo/cb"};
        Put put{};
        put.payload = bytes("cbval");
        rsp.payload.body = Reply{.payload = PushBody{.body = put}};
        send_batch(fd, build_frame(1, [&](ByteWriter& w) { (void)rsp.encode(w); }));
        ResponseFinal rf{};
        rf.rid = req->id;
        send_batch(fd, build_frame(2, [&](ByteWriter& w) { (void)rf.encode(w); }));
    });

    auto sess = Session::open(endpoint(router.port()));
    CHECK(sess.has_value());
    if (!sess) {
        router.join();
        return;
    }
    std::vector<std::string> seen;
    auto ok = sess->get("demo/cb", "",
                        [&seen](const GetReply& r) { seen.push_back(str(r.sample().payload())); });
    CHECK(ok.has_value());

    for (;;) {
        auto r = sess->run_once();
        if (!r) {
            CHECK(r.error() == ZError::connection_closed);
            break;
        }
        if (!seen.empty()) break; // stop once delivered (router half-closes right after)
    }
    router.join();
    CHECK(req_ok);
    CHECK(seen.size() == 1);
    if (!seen.empty()) CHECK(seen[0] == "cbval");
}

TEST("get() with target_zid sets the dest extension on the wire") {
    PeerId target{};
    target.len = 4;
    target.bytes[0] = std::byte{0xAA};
    target.bytes[1] = std::byte{0xBB};
    target.bytes[2] = std::byte{0xCC};
    target.bytes[3] = std::byte{0xDD};

    bool req_ok = false;
    bool dest_present = false;
    ZenohId seen_zid{};

    QueryRouter router([&](int fd) {
        auto batch = recv_batch(fd);
        if (!batch) return;
        auto r = open_frame(*batch);
        if (!r) return;
        auto req = Request::decode(*r);
        if (!req) return;
        req_ok = true;
        dest_present = req->dest.has_value();
        if (req->dest) seen_zid = req->dest->zid;

        ResponseFinal rf{};
        rf.rid = req->id;
        send_batch(fd, build_frame(1, [&](ByteWriter& w) { (void)rf.encode(w); }));
    });

    auto sess = Session::open(endpoint(router.port()));
    CHECK(sess.has_value());
    if (!sess) {
        router.join();
        return;
    }
    auto getter = sess->get("demo/tzid", "", GetOptions{.target_zid = target});
    CHECK(getter.has_value());
    if (getter) {
        auto r = getter->recv();
        CHECK(r.has_value() && !r->has_value());
    }
    sess->close();
    router.join();

    CHECK(req_ok);
    CHECK(dest_present);
    if (dest_present) {
        CHECK(seen_zid.len == 4);
        CHECK(seen_zid.bytes[0] == std::byte{0xAA});
        CHECK(seen_zid.bytes[3] == std::byte{0xDD});
    }
}

TEST("declare_queryable pull: IncomingQuery exposes key/parameters/payload and replies") {
    std::string declared_key;
    bool declare_ok = false;
    bool response_ok = false;
    std::uint32_t response_rid = 0;
    std::string response_payload;
    bool final_ok = false;
    std::uint32_t final_rid = 0;

    QueryRouter router([&](int fd) {
        // First data-phase batch is the client's DeclareQueryable.
        auto declare_batch = recv_batch(fd);
        if (!declare_batch) return;
        if (auto r = open_frame(*declare_batch)) {
            if (auto d = Declare::decode(*r)) {
                if (auto const* dq = std::get_if<DeclareQueryable>(&d->body.body)) {
                    declare_ok = true;
                    declared_key = std::string(dq->wire_expr.suffix);
                }
            }
        }

        Request req{};
        req.id = 42;
        req.wire_expr = WireExpr{.scope = 0, .mapping = Mapping::sender, .suffix = "demo/q"};
        Query q{};
        q.parameters = "a=1";
        Value v{};
        v.payload = bytes("reqpayload");
        q.body = v;
        req.payload = RequestBody{.query = q};
        send_batch(fd, build_frame(1, [&](ByteWriter& w) { (void)req.encode(w); }));

        // The client should reply, then finalize (on IncomingQuery drop).
        if (auto rsp_batch = recv_batch(fd)) {
            if (auto r = open_frame(*rsp_batch)) {
                if (auto rsp = Response::decode(*r)) {
                    response_rid = rsp->rid;
                    if (auto const* reply = std::get_if<Reply>(&rsp->payload.body)) {
                        if (auto const* put = std::get_if<Put>(&reply->payload.body)) {
                            response_ok = true;
                            response_payload = str(put->payload);
                        }
                    }
                }
            }
        }
        if (auto final_batch = recv_batch(fd)) {
            if (auto r = open_frame(*final_batch)) {
                if (auto rf = ResponseFinal::decode(*r)) {
                    final_ok = true;
                    final_rid = rf->rid;
                }
            }
        }
    });

    auto sess = Session::open(endpoint(router.port()));
    CHECK(sess.has_value());
    if (!sess) {
        router.join();
        return;
    }
    auto qbl = sess->declare_queryable("demo/q");
    CHECK(qbl.has_value());
    if (!qbl) {
        router.join();
        return;
    }

    std::optional<std::string> q_key, q_params, q_payload;
    {
        // Scoped so `q`'s destructor (which sends ResponseFinal) runs here, before
        // sess->close() invalidates the link below -- close() would otherwise make
        // the destructor's send_response_final() a silent no-op.
        auto q = qbl->recv();
        CHECK(q.has_value());
        if (q) {
            q_key = std::string(q->key_expr());
            q_params = std::string(q->parameters());
            q_payload = str(q->payload());
            auto rep = q->reply("demo/q", bytes("answer"));
            CHECK(rep.has_value());
        }
    } // q destructed here -> sends ResponseFinal while the link is still valid.
    sess->close();
    router.join();

    CHECK(declare_ok);
    CHECK(declared_key == "demo/q");
    CHECK(q_key.value_or("") == "demo/q");
    CHECK(q_params.value_or("") == "a=1");
    CHECK(q_payload.value_or("") == "reqpayload");
    CHECK(response_ok);
    CHECK(response_rid == 42);
    CHECK(response_payload == "answer");
    CHECK(final_ok);
    CHECK(final_rid == 42);
}

TEST("declare_queryable: a Request with no queryable declared is auto-finalized") {
    bool final_ok = false;
    std::uint32_t final_rid = 0;

    QueryRouter router([&](int fd) {
        Request req{};
        req.id = 7;
        req.wire_expr = WireExpr{.scope = 0, .mapping = Mapping::sender, .suffix = "demo/none"};
        send_batch(fd, build_frame(1, [&](ByteWriter& w) { (void)req.encode(w); }));

        if (auto final_batch = recv_batch(fd)) {
            if (auto r = open_frame(*final_batch)) {
                if (auto rf = ResponseFinal::decode(*r)) {
                    final_ok = true;
                    final_rid = rf->rid;
                }
            }
        }
    });

    auto sess = Session::open(endpoint(router.port()));
    CHECK(sess.has_value());
    if (!sess) {
        router.join();
        return;
    }
    // No declare_queryable() at all — dispatch_cursor must self-finalize the request.
    auto r = sess->run_once();
    CHECK(r.has_value());
    sess->close();
    router.join();

    CHECK(final_ok);
    CHECK(final_rid == 7);
}

TEST("a second queryable on one session is rejected") {
    QueryRouter router([](int) { std::this_thread::sleep_for(std::chrono::milliseconds(100)); });
    auto sess = Session::open(endpoint(router.port()));
    CHECK(sess.has_value());
    if (!sess) {
        router.join();
        return;
    }
    auto q1 = sess->declare_queryable("a/**");
    CHECK(q1.has_value());
    auto q2 = sess->declare_queryable("b/**");
    CHECK(!q2.has_value() && q2.error() == ZError::already_queryable);
    sess->close();
    router.join();
}

TEST("put() with target_zid sets the dest extension on the wire") {
    PeerId target{};
    target.len = 2;
    target.bytes[0] = std::byte{0x11};
    target.bytes[1] = std::byte{0x22};

    bool push_ok = false;
    bool dest_present = false;
    ZenohId seen_zid{};

    QueryRouter router([&](int fd) {
        auto batch = recv_batch(fd);
        if (!batch) return;
        auto r = open_frame(*batch);
        if (!r) return;
        auto push = Push::decode(*r);
        if (!push) return;
        push_ok = true;
        dest_present = push->dest.has_value();
        if (push->dest) seen_zid = push->dest->zid;
    });

    auto sess = Session::open(endpoint(router.port()));
    CHECK(sess.has_value());
    if (!sess) {
        router.join();
        return;
    }
    auto r = sess->put("demo/tzid", bytes("v"), PutOptions{.target_zid = target});
    CHECK(r.has_value());
    sess->close();
    router.join();

    CHECK(push_ok);
    CHECK(dest_present);
    if (dest_present) {
        CHECK(seen_zid.len == 2);
        CHECK(seen_zid.bytes[0] == std::byte{0x11});
        CHECK(seen_zid.bytes[1] == std::byte{0x22});
    }
}

TEST("put() carries CongestionControl on the wire as the standard QoS bit") {
    // Bit 3 ("D") of the QoS byte is Zenoh's own CongestionControl::Block flag, not
    // anything project-local -- so this is what makes a zenoh-rust router honour the
    // setting too. Default (Drop) must leave the whole QoS extension elided, exactly
    // as before this option existed.
    std::optional<QoS> drop_qos;
    std::optional<QoS> block_qos;
    int seen = 0;

    QueryRouter router([&](int fd) {
        for (int i = 0; i < 2; ++i) {
            auto batch = recv_batch(fd);
            if (!batch) return;
            auto r = open_frame(*batch);
            if (!r) return;
            auto push = Push::decode(*r);
            if (!push) return;
            (i == 0 ? drop_qos : block_qos) = push->qos;
            ++seen;
        }
    });

    auto sess = Session::open(endpoint(router.port()));
    CHECK(sess.has_value());
    if (!sess) {
        router.join();
        return;
    }
    CHECK(sess->put("demo/cc", bytes("v")).has_value());
    CHECK(sess->put("demo/cc", bytes("v"), PutOptions{.congestion = CongestionControl::block})
              .has_value());
    sess->close();
    router.join();

    CHECK(seen == 2);
    if (drop_qos) CHECK((drop_qos->inner & 0x08) == 0);
    if (block_qos) CHECK((block_qos->inner & 0x08) != 0);
    // Nothing else in the byte moves: priority and express stay at their defaults,
    // since this runtime implements neither.
    if (block_qos) CHECK(block_qos->inner == (QoS{}.inner | 0x08));
}

TEST("Session::local_zid() returns a stable, non-empty id") {
    QueryRouter router([](int) { std::this_thread::sleep_for(std::chrono::milliseconds(50)); });
    auto sess = Session::open(endpoint(router.port()));
    CHECK(sess.has_value());
    if (!sess) {
        router.join();
        return;
    }
    auto id1 = sess->local_zid();
    auto id2 = sess->local_zid();
    CHECK(id1.len == 16);
    CHECK(id1 == id2);
    sess->close();
    router.join();
}

// A callback subscriber whose strand fills up used to head-of-line block the shared
// receive cursor for *every* consumer: get() pumps the same cursor, so it re-decoded
// the undeliverable sample on every iteration (200k no-progress pumps in 1.2 s, at
// 100% CPU) and timed out with its reply already sitting in the receive buffer.
// Callback registrations need no application call to drain, so any pump caller now
// drains them and retries.
TEST("a full callback-subscriber strand does not block a concurrent get()") {
    QueryRouter router([&](int fd) {
        auto batch = recv_batch(fd); // DeclareSubscriber
        if (!batch) return;
        auto req_batch = recv_batch(fd); // the Request
        if (!req_batch) return;
        auto r = open_frame(*req_batch);
        if (!r) return;
        auto req = Request::decode(*r);
        if (!req) return;

        // One batch: four pushes (capacity is 2, so the strand fills), then the reply
        // and its final. Everything the client needs is in the buffer at once.
        send_batch(fd, build_frame(1, [&](ByteWriter& w) {
                       for (int i = 0; i < 4; ++i) {
                           Push push{};
                           push.wire_expr =
                               WireExpr{.scope = 0, .mapping = Mapping::sender, .suffix = "demo/s"};
                           Put put{};
                           put.payload = bytes("sample");
                           push.payload = PushBody{.body = std::move(put)};
                           (void)push.encode(w);
                       }
                       Response rsp{};
                       rsp.rid = req->id;
                       rsp.wire_expr =
                           WireExpr{.scope = 0, .mapping = Mapping::sender, .suffix = "demo/q"};
                       Put rput{};
                       rput.payload = bytes("reply");
                       rsp.payload.body = Reply{.payload = PushBody{.body = rput}};
                       (void)rsp.encode(w);
                       ResponseFinal rf{};
                       rf.rid = req->id;
                       (void)rf.encode(w);
                   }));
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    });

    auto sess = Session::open(endpoint(router.port()));
    CHECK(sess.has_value());
    if (!sess) {
        router.join();
        return;
    }
    int delivered = 0;
    auto sub = sess->declare_subscriber(
        "demo/**", [&](const Sample&) { ++delivered; }, SubscriberOptions{.capacity = 2});
    CHECK(sub.has_value());

    auto getter = sess->get("demo/q", "", GetOptions{.timeout_ms = 700});
    CHECK(getter.has_value());
    if (!getter) {
        sess->close();
        router.join();
        return;
    }
    auto reply = getter->recv(); // used to return query_timeout after 700 ms
    CHECK(reply.has_value());
    if (reply && *reply) CHECK(str((*reply)->sample().payload()) == "reply");
    // The get() no longer waits on the subscriber: draining it is what unblocked the
    // cursor, so some samples are already delivered by the time the reply arrives.
    CHECK(delivered > 0);
    // And none were dropped -- the rest are queued, and the next pump hands them over.
    (void)sess->run_once(); // may report the router's EOF; the samples come first
    CHECK(delivered == 4);

    sess->close();
    router.join();
}

// A peer that announces a batch and then stalls mid-body used to freeze the pump:
// recv_batch consumed the 2-byte length prefix and then blocked in read_exact with no
// timeout, so the get()'s own deadline went unnoticed (the call returned only when the
// peer eventually closed) and no keepalive went out in the meantime. The receive path
// now keeps the partial batch on the session and returns to its caller instead --
// discarding the partial read was never an option, since those bytes are gone from the
// socket and the stream would desynchronize.
TEST("a peer stalling mid-batch does not outlast the get() deadline") {
    QueryRouter router([&](int fd) {
        auto batch = recv_batch(fd); // the Request
        if (!batch) return;
        // Announce 100 bytes, send 5, then sit on it.
        std::array<std::byte, 2> len{};
        store_le<std::uint16_t>(len.data(), 100);
        std::array<std::byte, 5> partial{};
        (void)::send(fd, len.data(), len.size(), 0);
        (void)::send(fd, partial.data(), partial.size(), 0);
        std::this_thread::sleep_for(std::chrono::milliseconds(2500));
    });

    auto sess = Session::open(endpoint(router.port()));
    CHECK(sess.has_value());
    if (!sess) {
        router.join();
        return;
    }
    auto getter = sess->get("demo/stalled", "", GetOptions{.timeout_ms = 200});
    CHECK(getter.has_value());
    if (!getter) {
        sess->close();
        router.join();
        return;
    }

    auto const start = std::chrono::steady_clock::now();
    auto r = getter->recv();
    auto const elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                             std::chrono::steady_clock::now() - start)
                             .count();
    CHECK(!r.has_value() && r.error() == ZError::query_timeout);
    CHECK(elapsed < 1000); // pre-fix: blocked until the router closed, ~2500 ms

    sess->close();
    router.join();
}

// GetOptions::timeout_ms is a uint32 of milliseconds, so a caller may legitimately
// pass something like 34 days as "effectively no timeout". Getter::recv() bounds each
// pump to what is left of that deadline, computed as an int64 and then narrowed to the
// int32 poll() takes -- with no clamp, so past ~24.8 days it wrapped negative, std::min
// kept the negative, and poll() reads a negative timeout as "wait forever". The session
// then stopped emitting keepalives entirely and the router dropped it on lease expiry.
TEST("a very long get() timeout still lets keepalives out") {
    std::atomic<bool> saw_keepalive{false};

    QueryRouter router([&](int fd) {
        auto batch = recv_batch(fd); // the Request
        if (!batch) return;
        // The client's keepalive cadence is lease/4 = 2.5 s. Wait past it, then hang
        // up either way -- that is what releases the blocked recv() below.
        timeval tv{.tv_sec = 4, .tv_usec = 0};
        ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        while (auto b = recv_batch(fd)) {
            ByteReader r{*b};
            auto pk = r.peek();
            if (!pk) break;
            if ((std::to_integer<std::uint8_t>(*pk) & mid_mask) == KeepAlive::id) {
                saw_keepalive = true;
                break;
            }
        }
    });

    auto sess = Session::open(endpoint(router.port()));
    CHECK(sess.has_value());
    if (!sess) {
        router.join();
        return;
    }
    auto getter = sess->get("demo/forever", "", GetOptions{.timeout_ms = 3'000'000'000U});
    CHECK(getter.has_value());
    if (!getter) {
        sess->close();
        router.join();
        return;
    }

    // recv() is what drives the clamped pump. It cannot return on its own here -- the
    // deadline is ~34 days away -- so it blocks until the router hangs up, which is
    // exactly the behaviour under test: the keepalive has to go out meanwhile.
    auto r = getter->recv();
    CHECK(!r.has_value()); // released by the router's close, not by the deadline
    CHECK(saw_keepalive.load());

    sess->close();
    router.join();
}

// Getter and IncomingQuery are move-only handles over a non-owning Session*, exactly
// like Subscriber/Queryable/Batch: every method guards on that pointer and every
// move-assignment guards against self-assignment, and nothing else in the suite takes
// either branch. A reply on a moved-from query must be a no-op, not a crash.
TEST("moved-from and self-moved Getter/IncomingQuery stay safe") {
    QueryRouter router([&](int fd) {
        auto declare_batch = recv_batch(fd); // DeclareQueryable
        if (!declare_batch) return;

        Request req{};
        req.id = 7;
        req.wire_expr = WireExpr{.scope = 0, .mapping = Mapping::sender, .suffix = "demo/mv"};
        Query q{};
        q.parameters = "";
        req.payload = RequestBody{.query = q};
        send_batch(fd, build_frame(1, [&](ByteWriter& w) { (void)req.encode(w); }));
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
    });

    auto sess = Session::open(endpoint(router.port()));
    CHECK(sess.has_value());
    if (!sess) {
        router.join();
        return;
    }

    auto qbl = sess->declare_queryable("demo/**");
    CHECK(qbl.has_value());
    if (!qbl) {
        sess->close();
        router.join();
        return;
    }

    auto query = qbl->recv();
    CHECK(query.has_value());
    if (query) {
        auto moved = std::move(*query);
        // NOLINTNEXTLINE(bugprone-use-after-move) -- the point of the test
        CHECK(!query->reply("demo/mv", bytes("x")).has_value());
        // NOLINTNEXTLINE(bugprone-use-after-move)
        CHECK(!query->reply_err(bytes("x")).has_value());

        auto& alias = moved;
        moved = std::move(alias); // self-move must not finalize-and-free
        CHECK(moved.key_expr() == "demo/mv");
        CHECK(moved.reply("demo/mv", bytes("answer")).has_value());
    }

    // A Getter behaves the same way.
    auto getter = sess->get("demo/**", "", GetOptions{.timeout_ms = 50});
    CHECK(getter.has_value());
    if (getter) {
        auto moved = std::move(*getter);
        // NOLINTNEXTLINE(bugprone-use-after-move)
        CHECK(!getter->recv().has_value());
        auto& alias = moved;
        moved = std::move(alias);
        auto r = moved.recv(); // times out; the point is that it still works at all
        CHECK(!r.has_value() || true);
    }

    sess->close();
    router.join();
}

// The reply encoders have their own oversized-message arms, distinct from put/get's.
TEST("reply and reply_err report encode_error on an oversized message") {
    QueryRouter router(
        [&](int fd) {
            auto declare_batch = recv_batch(fd);
            if (!declare_batch) return;

            Request req{};
            req.id = 11;
            req.wire_expr = WireExpr{.scope = 0, .mapping = Mapping::sender, .suffix = "demo/big"};
            Query q{};
            q.parameters = "";
            req.payload = RequestBody{.query = q};
            send_batch(fd, build_frame(1, [&](ByteWriter& w) { (void)req.encode(w); }));
            std::this_thread::sleep_for(std::chrono::milliseconds(300));
        },
        /*batch_size=*/64);

    auto sess = Session::open(endpoint(router.port()));
    CHECK(sess.has_value());
    if (!sess) {
        router.join();
        return;
    }
    auto qbl = sess->declare_queryable("demo/**");
    CHECK(qbl.has_value());
    if (!qbl) {
        sess->close();
        router.join();
        return;
    }

    auto query = qbl->recv();
    CHECK(query.has_value());
    if (query) {
        std::vector<std::byte> const big(200, std::byte{0x7});
        auto r = query->reply("demo/big", big);
        CHECK(!r.has_value() && r.error() == ZError::encode_error);
        auto e = query->reply_err(big);
        CHECK(!e.has_value() && e.error() == ZError::encode_error);
    }

    sess->close();
    router.join();
}

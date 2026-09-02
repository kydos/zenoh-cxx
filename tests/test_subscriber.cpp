// Integration tests for the subscriber receive path (zenoh.session): a SubRouter runs
// in-process, completes the handshake, reads the client's DeclareSubscriber, then
// pushes Frames of Put/Del (and DeclareKeyExpr) so a real Session drives its
// run()/run_once() and Subscriber::recv() decode-loop, resmap, conflation, keepalive,
// and fault paths — all on the wire, no external zenohd.
import zenoh;       // Session, Subscriber, Sample, ZError, StrandMode
import zenoh.proto; // messages + ByteReader/ByteWriter + load_le/store_le

#include "ztest.hpp"

#include <array>
#include <atomic>
#include <cerrno>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <cstdio>
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

// Build a Frame body: FrameHeader(sn) followed by whatever `fn` appends.
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
auto put_msg(ByteWriter& w, std::string_view key, std::string_view val, std::uint16_t scope = 0) {
    Push push{};
    push.wire_expr = WireExpr{.scope = scope, .mapping = Mapping::sender, .suffix = key};
    Put put{};
    put.payload = bytes(val);
    push.payload = PushBody{.body = std::move(put)};
    (void)push.encode(w);
}
auto del_msg(ByteWriter& w, std::string_view key) {
    Push push{};
    push.wire_expr = WireExpr{.scope = 0, .mapping = Mapping::sender, .suffix = key};
    push.payload = PushBody{.body = Del{}};
    (void)push.encode(w);
}

// In-process router for one subscriber client: handshake, capture the client's
// DeclareSubscriber key, then hand the connected fd to a per-test `script`.
class SubRouter {
  public:
    explicit SubRouter(std::function<void(int)> script, std::uint16_t batch_size = 8192)
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
    ~SubRouter() {
        join();
        if (listen_fd_ >= 0) ::close(listen_fd_);
    }
    [[nodiscard]] auto port() const -> std::uint16_t { return port_; }
    [[nodiscard]] auto declared_key() const -> std::string { return declared_key_; }
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

        // The first data-phase batch from the client is the DeclareSubscriber.
        if (auto b = recv_batch(fd)) {
            ByteReader r{*b};
            if (auto first = r.peek();
                first && (std::to_integer<std::uint8_t>(*first) & mid_mask) == FrameHeader::id) {
                if (FrameHeader::decode(r)) {
                    if (auto d = Declare::decode(r)) {
                        if (auto const* ds = std::get_if<DeclareSubscriber>(&d->body.body))
                            declared_key_ = std::string(ds->wire_expr.suffix);
                    }
                }
            }
        } else {
            ::close(fd);
            return;
        }

        script_(fd);
        // Half-close (FIN after the pushed data) so the client reads all samples before
        // observing EOF, without an abrupt close() risking an RST.
        ::shutdown(fd, SHUT_WR);
        // Then drain whatever the client still sends (its Close, a keepalive) before
        // close(). Closing a socket that still has unread data in its receive queue
        // makes the kernel send an RST instead of finishing the FIN exchange, and an
        // RST lets the peer's stack discard data it has received but not yet handed to
        // the application -- i.e. samples this router already sent. That was a rare
        // whole-suite flake, only ever seen with the machine under load.
        timeval drain_tv{.tv_sec = 0, .tv_usec = 200000};
        ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &drain_tv, sizeof(drain_tv));
        std::array<std::byte, 512> sink{};
        while (::recv(fd, sink.data(), sink.size(), 0) > 0) {
        }
        ::close(fd);
    }

    std::function<void(int)> script_;
    std::uint16_t batch_size_;
    int listen_fd_ = -1;
    std::uint16_t port_ = 0;
    std::string declared_key_;
    std::thread thread_;
};

auto endpoint(std::uint16_t port) -> std::string { return "tcp/127.0.0.1:" + std::to_string(port); }

} // namespace

TEST("Subscriber::recv delivers pushed PUT samples in order and reports the key") {
    SubRouter router([](int fd) {
        auto f = build_frame(100, [](ByteWriter& w) {
            put_msg(w, "demo/a", "one");
            put_msg(w, "demo/b", "two");
        });
        send_batch(fd, f);
        send_batch(fd, build_frame(101, [](ByteWriter& w) { put_msg(w, "demo/c", "three"); }));
    });

    auto sess = Session::open(endpoint(router.port()));
    CHECK(sess.has_value());
    if (!sess) {
        router.join();
        return;
    }
    auto sub = sess->declare_subscriber("demo/**");
    CHECK(sub.has_value());
    if (!sub) {
        router.join();
        return;
    }

    std::vector<std::string> keys;
    std::vector<std::string> vals;
    for (int i = 0; i < 3; ++i) {
        auto s = sub->recv();
        CHECK(s.has_value());
        if (s) {
            CHECK(s->kind() == SampleKind::put);
            keys.push_back(std::string(s->key_expr()));
            vals.push_back(str(s->payload()));
        }
    }
    sess->close();
    router.join();

    CHECK(router.declared_key() == "demo/**");
    CHECK(keys.size() == 3);
    if (keys.size() == 3) {
        CHECK(keys[0] == "demo/a" && vals[0] == "one");
        CHECK(keys[1] == "demo/b" && vals[1] == "two");
        CHECK(keys[2] == "demo/c" && vals[2] == "three");
    }
}

TEST("Subscriber::recv surfaces a DELETE sample") {
    SubRouter router([](int fd) {
        send_batch(fd, build_frame(7, [](ByteWriter& w) { del_msg(w, "demo/gone"); }));
    });
    auto sess = Session::open(endpoint(router.port()));
    CHECK(sess.has_value());
    if (!sess) {
        router.join();
        return;
    }
    auto sub = sess->declare_subscriber("demo/**");
    CHECK(sub.has_value());

    auto s = sub->recv();
    CHECK(s.has_value());
    if (s) {
        CHECK(s->kind() == SampleKind::del);
        CHECK(s->payload().empty());
        CHECK(s->key_expr() == "demo/gone");
    }
    sess->close();
    router.join();
}

TEST("run()/run_once() drive a callback subscriber") {
    SubRouter router([](int fd) {
        send_batch(fd, build_frame(1, [](ByteWriter& w) {
                       put_msg(w, "k/1", "a");
                       put_msg(w, "k/2", "b");
                       put_msg(w, "k/3", "c");
                   }));
    });
    auto sess = Session::open(endpoint(router.port()));
    CHECK(sess.has_value());
    if (!sess) {
        router.join();
        return;
    }

    std::vector<std::string> seen;
    auto sub = sess->declare_subscriber(
        "k/**", [&seen](const Sample& s) { seen.push_back(std::string(s.key_expr())); });
    CHECK(sub.has_value());

    // Pump until the router closes (EOF). All three are delivered to the callback.
    for (;;) {
        auto r = sess->run_once();
        if (!r) {
            CHECK(r.error() == ZError::connection_closed);
            break;
        }
    }
    router.join();
    CHECK(seen.size() == 3);
    if (seen.size() == 3) CHECK(seen[0] == "k/1" && seen[1] == "k/2" && seen[2] == "k/3");
}

TEST("the resmap resolves a router-declared numeric keyexpr id") {
    SubRouter router([](int fd) {
        // Router binds id 7 -> "demo/x", then pushes on {scope:7, suffix:"/y"}.
        send_batch(fd, build_frame(1, [](ByteWriter& w) {
                       Declare d{};
                       DeclareKeyExpr dk{};
                       dk.id = 7;
                       dk.wire_expr =
                           WireExpr{.scope = 0, .mapping = Mapping::receiver, .suffix = "demo/x"};
                       d.body = DeclareBody{.body = dk};
                       (void)d.encode(w);
                   }));
        send_batch(fd, build_frame(2, [](ByteWriter& w) { put_msg(w, "/y", "v", /*scope=*/7); }));
    });
    auto sess = Session::open(endpoint(router.port()));
    CHECK(sess.has_value());
    if (!sess) {
        router.join();
        return;
    }
    auto sub = sess->declare_subscriber("demo/**");
    CHECK(sub.has_value());

    auto s = sub->recv();
    CHECK(s.has_value());
    if (s) {
        CHECK(s->key_expr() == "demo/x/y"); // resmap[7] ++ residual suffix
        CHECK(str(s->payload()) == "v");
    }
    sess->close();
    router.join();
}

TEST("a small strand resumes mid-frame across recv() (backpressure)") {
    // Two distinct keys in ONE frame, capacity 1: the second post hits a full strand,
    // the cursor pauses, and the next recv() resumes decoding the same frame.
    SubRouter router([](int fd) {
        send_batch(fd, build_frame(1, [](ByteWriter& w) {
                       put_msg(w, "demo/a", "1");
                       put_msg(w, "demo/b", "2");
                   }));
    });
    auto sess = Session::open(endpoint(router.port()));
    CHECK(sess.has_value());
    if (!sess) {
        router.join();
        return;
    }
    auto sub = sess->declare_subscriber("demo/**", SubscriberOptions{.capacity = 1});
    CHECK(sub.has_value());

    auto a = sub->recv();
    auto b = sub->recv();
    CHECK(a.has_value() && b.has_value());
    if (a && b) {
        CHECK(a->key_expr() == "demo/a" && str(a->payload()) == "1");
        CHECK(b->key_expr() == "demo/b" && str(b->payload()) == "2"); // resumed message
    }
    sess->close();
    router.join();
}

TEST("a last_value subscriber conflates same-key samples under backpressure") {
    // Capacity 2, three same-key puts in one frame: v1,v2 fill the strand, v3 conflates
    // the most-recent (v2) and re-tails -> the consumer sees v1 then v3 (v2 dropped).
    SubRouter router([](int fd) {
        send_batch(fd, build_frame(1, [](ByteWriter& w) {
                       put_msg(w, "demo/k", "v1");
                       put_msg(w, "demo/k", "v2");
                       put_msg(w, "demo/k", "v3");
                   }));
    });
    auto sess = Session::open(endpoint(router.port()));
    CHECK(sess.has_value());
    if (!sess) {
        router.join();
        return;
    }
    auto sub = sess->declare_subscriber(
        "demo/**", SubscriberOptions{.capacity = 2, .mode = StrandMode::last_value});
    CHECK(sub.has_value());

    auto a = sub->recv();
    auto b = sub->recv();
    CHECK(a.has_value() && b.has_value());
    if (a && b) {
        CHECK(str(a->payload()) == "v1");
        CHECK(str(b->payload()) == "v3"); // v2 conflated away
    }
    sess->close();
    router.join();
}

TEST("a malformed frame faults the subscriber permanently (sticky)") {
    SubRouter router([](int fd) {
        // FrameHeader then an unknown network message id (0x1f) the decoder can't skip.
        auto f = build_frame(1, [](ByteWriter& w) { (void)w.write_byte(std::byte{0x1f}); });
        send_batch(fd, f);
        // Keep the connection open so a timeout (not EOF) would be the only other source
        // of an error — the fault must come from the decode, not the close.
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    });
    auto sess = Session::open(endpoint(router.port()));
    CHECK(sess.has_value());
    if (!sess) {
        router.join();
        return;
    }
    auto sub = sess->declare_subscriber("demo/**");
    CHECK(sub.has_value());

    auto a = sub->recv();
    CHECK(!a.has_value() && a.error() == ZError::protocol_error);
    auto b = sub->recv(); // sticky: still faulted, no resync
    CHECK(!b.has_value() && b.error() == ZError::protocol_error);
    sess->close();
    router.join();
}

TEST("Subscriber::recv reports connection_closed on EOF") {
    SubRouter router([](int) { /* push nothing, just close */ });
    auto sess = Session::open(endpoint(router.port()));
    CHECK(sess.has_value());
    if (!sess) {
        router.join();
        return;
    }
    auto sub = sess->declare_subscriber("demo/**");
    CHECK(sub.has_value());
    auto s = sub->recv();
    CHECK(!s.has_value() && s.error() == ZError::connection_closed);
    router.join();
}

TEST("a second subscriber on one session is rejected") {
    SubRouter router([](int) { std::this_thread::sleep_for(std::chrono::milliseconds(100)); });
    auto sess = Session::open(endpoint(router.port()));
    CHECK(sess.has_value());
    if (!sess) {
        router.join();
        return;
    }
    auto s1 = sess->declare_subscriber("a/**");
    CHECK(s1.has_value());
    auto s2 = sess->declare_subscriber("b/**");
    CHECK(!s2.has_value() && s2.error() == ZError::already_subscribed);
    sess->close();
    router.join();
}

TEST("an idle subscriber emits a KeepAlive within the lease") {
    std::atomic<bool> saw_keepalive{false};
    SubRouter router([&saw_keepalive](int fd) {
        // Push nothing; the idle client must send a KeepAlive (lease/4 = 2.5s cadence).
        if (auto b = recv_batch(fd)) {
            ByteReader r{*b};
            if (auto first = r.peek())
                saw_keepalive = (std::to_integer<std::uint8_t>(*first) & mid_mask) == KeepAlive::id;
        }
    });
    auto sess = Session::open(endpoint(router.port()));
    CHECK(sess.has_value());
    if (!sess) {
        router.join();
        return;
    }
    auto sub = sess->declare_subscriber("demo/**");
    CHECK(sub.has_value());

    // Drive the pump on this thread; the router closes after reading one batch, so the
    // following recv returns connection_closed once the keepalive has been observed.
    auto s = sub->recv();
    CHECK(!s.has_value() && s.error() == ZError::connection_closed);
    router.join();
    CHECK(saw_keepalive.load());
}

// --- batch framing: a TCP batch is a *sequence* of transport messages ---
//
// The reference packs a KeepAlive (or a Close, or a second Frame) into whichever
// batch is currently staging -- zenoh-rust's `TransmissionPipeline::push_transport_
// message` appends to the batch that already carries a frame. Before these cases the
// session read exactly one transport message per batch: a trailing KeepAlive was
// mistaken for a network message and set the sticky protocol fault (killing the
// session for good), and a *leading* one made the frame behind it disappear.

TEST("a KeepAlive packed after a frame does not fault the session") {
    SubRouter router([](int fd) {
        auto body = build_frame(100, [](ByteWriter& w) { put_msg(w, "demo/a", "one"); });
        auto const ka = encode_body(KeepAlive{});
        body.insert(body.end(), ka.begin(), ka.end());
        send_batch(fd, body);
        // A second, ordinary batch: proves the session is still usable afterwards
        // rather than merely having survived the first one.
        send_batch(fd, build_frame(101, [](ByteWriter& w) { put_msg(w, "demo/b", "two"); }));
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    });
    auto sess = Session::open(endpoint(router.port()));
    CHECK(sess.has_value());
    if (!sess) {
        router.join();
        return;
    }
    auto sub = sess->declare_subscriber("demo/**");
    CHECK(sub.has_value());

    auto a = sub->recv();
    CHECK(a.has_value());
    if (a) CHECK(a->key_expr() == "demo/a" && str(a->payload()) == "one");
    auto b = sub->recv();
    CHECK(b.has_value());
    if (b) CHECK(b->key_expr() == "demo/b" && str(b->payload()) == "two");
    sess->close();
    router.join();
}

TEST("a KeepAlive packed before a frame does not discard the frame") {
    SubRouter router([](int fd) {
        auto body = encode_body(KeepAlive{});
        auto const frame = build_frame(100, [](ByteWriter& w) { put_msg(w, "demo/a", "one"); });
        body.insert(body.end(), frame.begin(), frame.end());
        send_batch(fd, body);
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    });
    auto sess = Session::open(endpoint(router.port()));
    CHECK(sess.has_value());
    if (!sess) {
        router.join();
        return;
    }
    auto sub = sess->declare_subscriber("demo/**");
    CHECK(sub.has_value());

    auto a = sub->recv();
    CHECK(a.has_value());
    if (a) CHECK(a->key_expr() == "demo/a" && str(a->payload()) == "one");
    sess->close();
    router.join();
}

TEST("two frames in one batch both deliver") {
    SubRouter router([](int fd) {
        auto body = build_frame(100, [](ByteWriter& w) { put_msg(w, "demo/a", "one"); });
        auto const second = build_frame(101, [](ByteWriter& w) { put_msg(w, "demo/b", "two"); });
        body.insert(body.end(), second.begin(), second.end());
        send_batch(fd, body);
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    });
    auto sess = Session::open(endpoint(router.port()));
    CHECK(sess.has_value());
    if (!sess) {
        router.join();
        return;
    }
    auto sub = sess->declare_subscriber("demo/**");
    CHECK(sub.has_value());

    auto a = sub->recv();
    CHECK(a.has_value());
    if (a) CHECK(a->key_expr() == "demo/a");
    auto b = sub->recv();
    CHECK(b.has_value());
    if (b) CHECK(b->key_expr() == "demo/b");
    sess->close();
    router.join();
}

TEST("a Close packed after a frame is honoured, and the frame still delivers") {
    SubRouter router([](int fd) {
        auto body = build_frame(100, [](ByteWriter& w) { put_msg(w, "demo/a", "one"); });
        Close close{};
        auto const c = encode_body(close);
        body.insert(body.end(), c.begin(), c.end());
        send_batch(fd, body);
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    });
    auto sess = Session::open(endpoint(router.port()));
    CHECK(sess.has_value());
    if (!sess) {
        router.join();
        return;
    }
    auto sub = sess->declare_subscriber("demo/**");
    CHECK(sub.has_value());

    auto a = sub->recv();
    CHECK(a.has_value());
    if (a) CHECK(a->key_expr() == "demo/a");
    auto b = sub->recv(); // the Close that rode along in the same batch
    CHECK(!b.has_value() && b.error() == ZError::connection_closed);
    sess->close();
    router.join();
}

// A callback handler is allowed to undeclare its own Subscriber -- "stop after N
// samples" is the obvious shape. run_once() used to re-dereference `sub_` on every
// iteration of its drain loop and call the handler *through* it, so undeclaring from
// inside the handler freed the registration the loop was standing on: the next
// iteration null-dereferenced it (ASan: SEGV on address 0x20), and the std::function
// being executed had been freed under its own feet. Needs >1 message queued to hit.
TEST("a callback that undeclares its own subscriber mid-drain does not crash") {
    SubRouter router([](int fd) {
        send_batch(fd, build_frame(1, [](ByteWriter& w) {
                       put_msg(w, "demo/a", "one");
                       put_msg(w, "demo/b", "two");
                       put_msg(w, "demo/c", "three");
                   }));
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    });
    auto sess = Session::open(endpoint(router.port()));
    CHECK(sess.has_value());
    if (!sess) {
        router.join();
        return;
    }

    int delivered = 0;
    Subscriber* self = nullptr;
    auto sub = sess->declare_subscriber("demo/**", [&](const Sample&) {
        ++delivered;
        if (self) self->undeclare(); // frees the registration run_once is draining
    });
    CHECK(sub.has_value());
    if (!sub) {
        sess->close();
        router.join();
        return;
    }
    self = &*sub;

    CHECK(sess->run_once().has_value()); // used to crash here
    CHECK(delivered == 1);               // and no further samples after the undeclare

    sess->close();
    router.join();
}

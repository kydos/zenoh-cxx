// Integration tests for the client runtime (zenoh.session + zenoh.runtime.tcp).
//
// A FakeRouter runs in-process on a loopback socket and speaks just enough of the
// transport protocol — InitAck / OpenAck, then decoding the data Frames — to drive a
// real Session through its handshake, put / try_put / batch, and close paths without
// needing an external zenohd. The router reuses the project's own codec to build its
// replies and to decode what the client sends, so the test is end-to-end on the wire.
import zenoh;       // Session, Batch, ZError
import zenoh.proto; // messages + ByteReader/ByteWriter + load_le/store_le

#include "ztest.hpp"

#include <array>
#include <chrono>
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
#include <cerrno>
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

// Encode a message body (no batch length prefix) into an owned buffer.
template <class Msg> auto encode_body(const Msg& m) -> std::vector<std::byte> {
    std::vector<std::byte> buf(512);
    ByteWriter w{buf};
    (void)m.encode(w);
    return {buf.data(), buf.data() + w.written()};
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
        return false; // EOF, timeout, or error
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

// Read one length-prefixed TCP batch (2-byte LE length + body).
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

struct Sample {
    std::string key;
    std::string value;
};

// A minimal in-process Zenoh router for one client connection.
class FakeRouter {
  public:
    explicit FakeRouter(std::uint16_t batch_size = 64) : batch_size_(batch_size) {
        std::signal(SIGPIPE, SIG_IGN);
        listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
        int one = 1;
        ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port = 0; // ephemeral
        ::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
        ::listen(listen_fd_, 1);
        socklen_t len = sizeof(addr);
        ::getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&addr), &len);
        port_ = ntohs(addr.sin_port);
        thread_ = std::thread([this] { run(); });
    }

    ~FakeRouter() {
        join();
        if (listen_fd_ >= 0) ::close(listen_fd_);
    }

    [[nodiscard]] auto port() const -> std::uint16_t { return port_; }

    auto join() -> void {
        if (thread_.joinable()) thread_.join();
    }

    [[nodiscard]] auto samples() const -> const std::vector<Sample>& { return samples_; }

  private:
    auto run() -> void {
        pollfd pfd{.fd = listen_fd_, .events = POLLIN, .revents = 0};
        if (::poll(&pfd, 1, 5000) <= 0) return; // no client connected
        int const fd = ::accept(listen_fd_, nullptr, nullptr);
        if (fd < 0) return;
        timeval tv{.tv_sec = 5, .tv_usec = 0};
        ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

        // --- handshake ---
        if (!recv_batch(fd)) {
            ::close(fd);
            return;
        } // InitSyn (consume)

        InitAck ack{};
        ack.version = 9;
        ack.identifier.whatami = WhatAmI::router;
        ack.identifier.zid.len = 4;
        ack.identifier.zid.bytes = {std::byte{0xA1}, std::byte{0xA2}, std::byte{0xA3},
                                    std::byte{0xA4}};
        ack.resolution.resolution = 0x0a;
        ack.resolution.batch_size = batch_size_;
        std::array<std::byte, 4> cookie{std::byte{1}, std::byte{2}, std::byte{3}, std::byte{4}};
        ack.cookie = cookie;
        if (!send_batch(fd, encode_body(ack))) {
            ::close(fd);
            return;
        }

        if (!recv_batch(fd)) {
            ::close(fd);
            return;
        } // OpenSyn (consume)

        OpenAck oack{};
        oack.lease = Duration::from_millis(10000);
        oack.sn = 0;
        if (!send_batch(fd, encode_body(oack))) {
            ::close(fd);
            return;
        }

        // --- data phase ---
        for (;;) {
            auto batch = recv_batch(fd);
            if (!batch) break; // EOF / timeout
            if (batch->empty()) continue;
            ByteReader r{*batch};
            auto first = r.peek();
            if (!first) continue;
            std::uint8_t const mid = std::to_integer<std::uint8_t>(*first) & mid_mask;
            if (mid == FrameHeader::id) {
                if (!FrameHeader::decode(r)) continue;
                while (r.remaining() > 0) {
                    auto push = Push::decode(r);
                    if (!push) break;
                    if (auto const* p = std::get_if<Put>(&push->payload.body)) {
                        samples_.push_back(
                            {std::string(push->wire_expr.suffix),
                             std::string(reinterpret_cast<const char*>(p->payload.data()),
                                         p->payload.size())});
                    }
                }
            } else if (mid == Close::id) {
                break;
            }
        }
        ::close(fd);
    }

    std::uint16_t batch_size_;
    int listen_fd_ = -1;
    std::uint16_t port_ = 0;
    std::thread thread_;
    std::vector<Sample> samples_;
};

auto endpoint(std::uint16_t port) -> std::string { return "tcp/127.0.0.1:" + std::to_string(port); }

// A bare listener that hands the accepted fd to a per-test `script` (which performs
// whatever handshake / abort / deaf behaviour the test needs). `rcvbuf`, if set,
// shrinks the receive window so a non-reading script induces client backpressure.
class RawRouter {
  public:
    explicit RawRouter(std::function<void(int)> script, int rcvbuf = 0)
        : script_(std::move(script)) {
        std::signal(SIGPIPE, SIG_IGN);
        listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
        int one = 1;
        ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
        if (rcvbuf > 0) ::setsockopt(listen_fd_, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port = 0;
        ::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
        ::listen(listen_fd_, 1);
        socklen_t len = sizeof(addr);
        ::getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&addr), &len);
        port_ = ntohs(addr.sin_port);
        thread_ = std::thread([this] {
            pollfd pfd{.fd = listen_fd_, .events = POLLIN, .revents = 0};
            if (::poll(&pfd, 1, 5000) <= 0) return;
            int const fd = ::accept(listen_fd_, nullptr, nullptr);
            if (fd < 0) return;
            timeval tv{.tv_sec = 5, .tv_usec = 0};
            ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
            script_(fd);
            ::shutdown(fd, SHUT_WR);
            timeval drain{.tv_sec = 0, .tv_usec = 300'000};
            ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &drain, sizeof(drain));
            std::array<std::byte, 256> sink{};
            while (::recv(fd, sink.data(), sink.size(), 0) > 0) {
            }
            ::close(fd);
        });
    }
    ~RawRouter() {
        join();
        if (listen_fd_ >= 0) ::close(listen_fd_);
    }
    [[nodiscard]] auto port() const -> std::uint16_t { return port_; }
    auto join() -> void {
        if (thread_.joinable()) thread_.join();
    }

  private:
    std::function<void(int)> script_;
    int listen_fd_ = -1;
    std::uint16_t port_ = 0;
    std::thread thread_;
};

// Complete the 4-way handshake on `fd` with a chosen batch size; returns false on I/O
// failure (used by RawRouter scripts that then misbehave or go deaf).
auto do_handshake(int fd, std::uint16_t batch_size) -> bool {
    if (!recv_batch(fd)) return false; // InitSyn
    InitAck ack{};
    ack.version = 9;
    ack.identifier.whatami = WhatAmI::router;
    ack.identifier.zid.len = 4;
    ack.identifier.zid.bytes = {std::byte{1}, std::byte{2}, std::byte{3}, std::byte{4}};
    ack.resolution.resolution = 0x0a;
    ack.resolution.batch_size = batch_size;
    std::array<std::byte, 4> cookie{std::byte{1}, std::byte{2}, std::byte{3}, std::byte{4}};
    ack.cookie = cookie;
    if (!send_batch(fd, encode_body(ack))) return false;
    if (!recv_batch(fd)) return false; // OpenSyn
    OpenAck oack{};
    oack.lease = Duration::from_millis(10000);
    oack.sn = 0;
    return send_batch(fd, encode_body(oack));
}

} // namespace

TEST("Session::open handshakes and put/try_put/batch reach the router") {
    FakeRouter router;
    auto sess = Session::open(endpoint(router.port()));
    CHECK(sess.has_value());
    if (!sess) {
        router.join();
        return;
    }

    CHECK(sess->put("demo/a", bytes("hello")).has_value());
    CHECK(sess->try_put("demo/b", bytes("world")).has_value());

    {
        auto b = sess->batch();
        CHECK(b.put("demo/c", bytes("x")).has_value());
        CHECK(b.put("demo/d", bytes("y")).has_value());
        CHECK(b.size() == 2);
        CHECK(b.flush().has_value());
        CHECK(b.empty());
    }

    // Small negotiated batch size (64) forces this run to span several frames,
    // exercising the batch overflow -> auto-flush path.
    {
        auto b = sess->batch();
        for (int i = 0; i < 10; ++i) CHECK(b.put("demo/e", bytes("payload-data")).has_value());
        CHECK(b.flush().has_value());
    }

    sess->close();
    router.join();

    auto const& s = router.samples();
    CHECK(s.size() == 14); // a, b, c, d, + 10x e
    if (s.size() >= 4) {
        CHECK(s[0].key == "demo/a" && s[0].value == "hello");
        CHECK(s[1].key == "demo/b" && s[1].value == "world");
        CHECK(s[2].key == "demo/c" && s[2].value == "x");
        CHECK(s[3].key == "demo/d" && s[3].value == "y");
    }
    int es = 0;
    for (auto const& x : s)
        if (x.key == "demo/e" && x.value == "payload-data") ++es;
    CHECK(es == 10);
}

TEST("Batch flushes on destruction and supports move-assignment") {
    FakeRouter router;
    auto sess = Session::open(endpoint(router.port()));
    CHECK(sess.has_value());
    if (!sess) {
        router.join();
        return;
    }

    // (1) A batch with buffered puts but no explicit flush() must flush on scope exit.
    {
        auto b = sess->batch();
        CHECK(b.put("demo/dtor", bytes("byebye")).has_value());
        // no flush — the destructor sends it
    }

    // (2) Move-assigning into a batch that still has buffered data flushes that data
    // first, then adopts the source's pending frame.
    {
        auto b1 = sess->batch();
        CHECK(b1.put("demo/mv-src", bytes("src")).has_value());
        auto b2 = sess->batch();
        CHECK(b2.put("demo/mv-dst", bytes("dst")).has_value());
        b2 = std::move(b1); // flushes demo/mv-dst, b2 now carries demo/mv-src
        CHECK(b2.flush().has_value());
    }

    sess->close();
    router.join();

    auto const& s = router.samples();
    CHECK(s.size() == 3);
    int dtor = 0, src = 0, dst = 0;
    for (auto const& x : s) {
        if (x.key == "demo/dtor") ++dtor;
        if (x.key == "demo/mv-src") ++src;
        if (x.key == "demo/mv-dst") ++dst;
    }
    CHECK(dtor == 1 && src == 1 && dst == 1);
}

TEST("Session::put rejects a payload too large for the negotiated batch") {
    FakeRouter router; // batch_size = 64
    auto sess = Session::open(endpoint(router.port()));
    CHECK(sess.has_value());
    if (!sess) {
        router.join();
        return;
    }
    std::vector<std::byte> big(200, std::byte{0x7});
    auto r = sess->put("demo/big", big);
    CHECK(!r.has_value() && r.error() == ZError::encode_error);
    sess->close();
    router.join();
}

TEST("Session::open rejects an unparseable endpoint") {
    auto s1 = Session::open("not-a-locator");
    CHECK(!s1.has_value() && s1.error() == ZError::bad_endpoint);
    auto s2 = Session::open("tcp/");
    CHECK(!s2.has_value() && s2.error() == ZError::bad_endpoint);
    auto s3 = Session::open("tcp/127.0.0.1:99999"); // port out of range
    CHECK(!s3.has_value() && s3.error() == ZError::bad_endpoint);
}

TEST("Session::open fails to connect to a closed port") {
    // Port 1 on loopback is virtually never listening -> connect refused.
    auto s = Session::open("tcp/127.0.0.1:1");
    CHECK(!s.has_value());
}

TEST("a moved-from Session leaves the moved-to one usable") {
    FakeRouter router;
    auto sess = Session::open(endpoint(router.port()));
    CHECK(sess.has_value());
    if (!sess) {
        router.join();
        return;
    }
    Session moved = std::move(*sess); // exercise the move constructor
    CHECK(moved.put("demo/m", bytes("moved")).has_value());
    moved.close();
    router.join();
    CHECK(router.samples().size() == 1);
    if (!router.samples().empty()) CHECK(router.samples()[0].key == "demo/m");
}

TEST("Batch move-construction and an oversize put are handled") {
    FakeRouter router; // batch_size 64
    auto sess = Session::open(endpoint(router.port()));
    CHECK(sess.has_value());
    if (!sess) {
        router.join();
        return;
    }
    {
        auto b1 = sess->batch();
        CHECK(b1.put("demo/mc", bytes("v")).has_value());
        Batch b2 = std::move(b1); // move-construction
        CHECK(b2.size() == 1);
        CHECK(b2.flush().has_value());
    }
    {
        auto b = sess->batch();
        std::vector<std::byte> big(200, std::byte{0x7}); // single put > batch_size 64
        auto r = b.put("demo/big", big);
        CHECK(!r.has_value() && r.error() == ZError::encode_error);
    }
    sess->close();
    router.join();
}

TEST("Session move-assignment transfers the live connection") {
    FakeRouter r1;
    FakeRouter r2;
    auto s1 = Session::open(endpoint(r1.port()));
    auto s2 = Session::open(endpoint(r2.port()));
    CHECK(s1.has_value() && s2.has_value());
    if (!s1 || !s2) {
        r1.join();
        r2.join();
        return;
    }
    *s1 = std::move(*s2); // closes s1's old link (r1), adopts s2's (r2)
    CHECK(s1->put("demo/ma", bytes("v")).has_value());
    s1->close();
    r1.join();
    r2.join();
    CHECK(r2.samples().size() == 1);
}

TEST("Session::open parses a bracketed IPv6 endpoint") {
    // Exercises the [ipv6]:port parse branch; the connect to ::1:1 then fails, which is
    // fine — only the parse path is under test.
    auto s = Session::open("tcp/[::1]:1");
    CHECK(!s.has_value());
}

TEST("Session::open fails when the router aborts mid-handshake") {
    { // close right after accept -> the client's InitAck read hits EOF
        RawRouter r([](int) {});
        auto s = Session::open(endpoint(r.port()));
        CHECK(!s.has_value());
        r.join();
    }
    { // send InitAck then close -> the client's OpenAck read hits EOF
        RawRouter r([](int fd) {
            if (!recv_batch(fd)) return; // InitSyn
            InitAck ack{};
            ack.version = 9;
            ack.identifier.whatami = WhatAmI::router;
            ack.identifier.zid.len = 4;
            ack.identifier.zid.bytes = {std::byte{1}, std::byte{2}, std::byte{3}, std::byte{4}};
            ack.resolution.resolution = 0x0a;
            ack.resolution.batch_size = 4096;
            std::array<std::byte, 4> cookie{std::byte{1}, std::byte{2}, std::byte{3}, std::byte{4}};
            ack.cookie = cookie;
            (void)send_batch(fd, encode_body(ack)); // then return -> no OpenAck
        });
        auto s = Session::open(endpoint(r.port()));
        CHECK(!s.has_value());
        r.join();
    }
}

TEST("try_put buffers under backpressure and drains via flush_pending") {
    // Handshake with a large batch size, then go deaf with a tiny receive window so the
    // client's non-blocking writes can't fully drain: exercises the partial-write
    // buffering (tx_pending_) and the flush_pending / would_block paths.
    RawRouter router(
        [](int fd) {
            if (!do_handshake(fd, 0xffff)) return;
            std::this_thread::sleep_for(std::chrono::milliseconds(800)); // deaf
        },
        /*rcvbuf=*/2048);
    auto sess = Session::open(endpoint(router.port()));
    CHECK(sess.has_value());
    if (!sess) {
        router.join();
        return;
    }
    std::vector<std::byte> payload(8192, std::byte{0xAB});
    bool would_block = false;
    for (int i = 0; i < 4096 && !would_block; ++i) {
        auto r = sess->try_put("demo/bp", payload);
        if (!r) {
            CHECK(r.error() == ZError::would_block);
            would_block = true;
        }
    }
    CHECK(would_block); // window fills -> try_put eventually reports backpressure
    sess->close();
    router.join();
}

// Integration tests for the publisher send path (zenoh.session): a PubRouter runs
// in-process, completes the handshake, then decodes everything a real Session sends
// in the data phase — DeclareKeyExpr/UndeclareKeyExpr, Interest/InterestFinal, and
// Push(Put|Del) — so `declare_publisher` is checked on the wire, byte for byte, and
// not merely through the API's return values.
//
// What a publisher is *for* is the key-expression id: after the declaration, a Push
// carries `scope = id, suffix = ""` instead of the key expression text. That
// substitution, the id's refcounted sharing and release, and the fallback to the
// literal key expression when no id can be bound are what these cases pin down.
import zenoh;       // Session, Publisher, ZError, CongestionControl
import zenoh.proto; // messages + ByteReader/ByteWriter + load_le/store_le

#include "ztest.hpp"

#include <array>
#include <cerrno>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
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

// One decoded data-phase message, flattened into the handful of fields these tests
// assert on. Keeping every message kind in one ordered vector is deliberate: the
// *order* of DeclareKeyExpr / Interest / Push / InterestFinal / UndeclareKeyExpr is
// itself part of what a publisher declaration promises.
struct Event {
    enum class Kind : std::uint8_t { declare_ke, undeclare_ke, interest, interest_final, put, del };

    Kind kind{};
    std::uint32_t id = 0;     ///< declare/undeclare keyexpr id, or the interest id
    std::uint16_t scope = 0;  ///< wire_expr scope of a Push or an Interest
    std::string key;          ///< declared key expression, or a Push's suffix
    std::string value;        ///< Put payload
    std::uint8_t options = 0; ///< Interest options bitfield
    std::uint8_t mode = 0;    ///< Interest mode
    std::uint8_t qos = 5;     ///< Push QoS byte (default: Data priority, drop)
    bool has_dest = false;    ///< Push carried a DestinationId (zid targeting)
};

// A minimal in-process Zenoh router that records the client's data-phase messages.
class PubRouter {
  public:
    explicit PubRouter(std::uint16_t batch_size = 8192) : batch_size_(batch_size) {
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
    ~PubRouter() {
        join();
        if (listen_fd_ >= 0) ::close(listen_fd_);
    }

    [[nodiscard]] auto port() const -> std::uint16_t { return port_; }
    auto join() -> void {
        if (thread_.joinable()) thread_.join();
    }
    /// The recorded messages. Call after `join()` (or after the session is closed).
    [[nodiscard]] auto events() -> std::vector<Event> {
        std::lock_guard const lock(mu_);
        return events_;
    }

  private:
    auto record(Event e) -> void {
        std::lock_guard const lock(mu_);
        events_.push_back(std::move(e));
    }

    // Decode one network message. Returns false on anything malformed, which fails
    // the connection loudly rather than letting a test silently observe nothing.
    auto decode_network(ByteReader& r) -> bool {
        auto pk = r.peek();
        if (!pk) return false;
        std::uint8_t const mid = std::to_integer<std::uint8_t>(*pk) & mid_mask;
        if (mid == Push::id) {
            auto push = Push::decode(r);
            if (!push) return false;
            Event e{};
            e.scope = push->wire_expr.scope;
            e.key = std::string(push->wire_expr.suffix);
            e.qos = push->qos.inner;
            e.has_dest = push->dest.has_value();
            if (auto const* p = std::get_if<Put>(&push->payload.body)) {
                e.kind = Event::Kind::put;
                e.value = std::string(reinterpret_cast<const char*>(p->payload.data()),
                                      p->payload.size());
            } else {
                e.kind = Event::Kind::del;
            }
            record(std::move(e));
            return true;
        }
        if (mid == Declare::mid) {
            auto d = Declare::decode(r);
            if (!d) return false;
            if (auto const* dk = std::get_if<DeclareKeyExpr>(&d->body.body)) {
                record({.kind = Event::Kind::declare_ke,
                        .id = dk->id,
                        .scope = dk->wire_expr.scope,
                        .key = std::string(dk->wire_expr.suffix)});
            } else if (auto const* uk = std::get_if<UndeclareKeyExpr>(&d->body.body)) {
                record({.kind = Event::Kind::undeclare_ke, .id = uk->id});
            }
            return true;
        }
        if (mid == Interest::mid) {
            // Interest and InterestFinal share the mid; MODE == 0 is the Final form.
            auto const mode_bits =
                static_cast<std::uint8_t>((std::to_integer<unsigned>(*pk) >> 5) & 0x3);
            if (mode_bits == 0) {
                auto fin = InterestFinal::decode(r);
                if (!fin) return false;
                record({.kind = Event::Kind::interest_final, .id = fin->id});
                return true;
            }
            auto in = Interest::decode(r);
            if (!in) return false;
            record(
                {.kind = Event::Kind::interest,
                 .id = in->id,
                 .scope = in->inner.wire_expr ? in->inner.wire_expr->scope
                                              : static_cast<std::uint16_t>(0),
                 .key =
                     in->inner.wire_expr ? std::string(in->inner.wire_expr->suffix) : std::string{},
                 .options = static_cast<std::uint8_t>(in->inner.options & InterestInner::opt_mask),
                 .mode = static_cast<std::uint8_t>(mode_bits)});
            return true;
        }
        return false; // not a network message this router expects
    }

    auto run() -> void {
        pollfd pfd{.fd = listen_fd_, .events = POLLIN, .revents = 0};
        if (::poll(&pfd, 1, 5000) <= 0) return;
        int const fd = ::accept(listen_fd_, nullptr, nullptr);
        if (fd < 0) return;
        timeval tv{.tv_sec = 6, .tv_usec = 0};
        ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

        if (!recv_batch(fd)) { // InitSyn
            ::close(fd);
            return;
        }
        InitAck ack{};
        ack.version = 9;
        ack.identifier.whatami = WhatAmI::router;
        ack.identifier.zid.len = 4;
        ack.identifier.zid.bytes = {std::byte{7}, std::byte{7}, std::byte{7}, std::byte{7}};
        ack.resolution.resolution = 0x0a;
        ack.resolution.batch_size = batch_size_;
        std::array<std::byte, 4> cookie{std::byte{4}, std::byte{3}, std::byte{2}, std::byte{1}};
        ack.cookie = cookie;
        if (!send_batch(fd, encode_body(ack))) {
            ::close(fd);
            return;
        }
        if (!recv_batch(fd)) { // OpenSyn
            ::close(fd);
            return;
        }
        OpenAck oack{};
        oack.lease = Duration::from_millis(10000);
        oack.sn = 0;
        if (!send_batch(fd, encode_body(oack))) {
            ::close(fd);
            return;
        }

        // --- data phase: decode every message of every batch ---
        for (;;) {
            auto batch = recv_batch(fd);
            if (!batch) break; // EOF / timeout
            ByteReader r{*batch};
            while (r.remaining() > 0) {
                auto pk = r.peek();
                if (!pk) break;
                std::uint8_t const mid = std::to_integer<std::uint8_t>(*pk) & mid_mask;
                if (mid == Close::id) goto done;
                if (mid == KeepAlive::id) {
                    if (!KeepAlive::decode(r)) goto done;
                    continue;
                }
                if (mid != FrameHeader::id) goto done;
                if (!FrameHeader::decode(r)) goto done;
                // A frame's body runs to the first non-network id (see the session's
                // own dispatch loop) — here, to the end of the batch or a KeepAlive.
                while (r.remaining() > 0) {
                    auto next = r.peek();
                    if (!next || !is_network_mid(std::to_integer<std::uint8_t>(*next) & mid_mask))
                        break;
                    if (!decode_network(r)) goto done;
                }
            }
        }
    done:
        ::close(fd);
    }

    std::uint16_t batch_size_;
    int listen_fd_ = -1;
    std::uint16_t port_ = 0;
    std::thread thread_;
    std::mutex mu_;
    std::vector<Event> events_;
};

auto endpoint(std::uint16_t port) -> std::string { return "tcp/127.0.0.1:" + std::to_string(port); }

/// Indices of the events of one kind, in arrival order.
auto of_kind(const std::vector<Event>& events, Event::Kind k) -> std::vector<Event> {
    std::vector<Event> out;
    for (auto const& e : events)
        if (e.kind == k) out.push_back(e);
    return out;
}

} // namespace

TEST("declare_publisher binds a keyexpr id, and put/del send the id instead of the key") {
    PubRouter router;
    auto sess = Session::open(endpoint(router.port()));
    CHECK(sess.has_value());
    if (!sess) {
        router.join();
        return;
    }

    auto pub = sess->declare_publisher("demo/example/some/long/key");
    CHECK(pub.has_value());
    if (!pub) {
        router.join();
        return;
    }
    CHECK(pub->key_expr() == "demo/example/some/long/key");
    CHECK(pub->keyexpr_id() != 0);
    CHECK(pub->congestion_control() == CongestionControl::drop);
    CHECK(!pub->target_zid().has_value());

    CHECK(pub->put(bytes("one")).has_value());
    CHECK(pub->try_put(bytes("two")).has_value());
    CHECK(pub->del().has_value());
    std::uint16_t const ke_id = pub->keyexpr_id();
    pub->undeclare();

    sess->close();
    router.join();
    auto const ev = router.events();

    // The declaration itself spells the key expression out (it is what binds the id),
    // and is followed by the publisher's Interest naming the *id*.
    CHECK(ev.size() == 7);
    if (ev.size() != 7) return;
    CHECK(ev[0].kind == Event::Kind::declare_ke);
    CHECK(ev[0].id == ke_id);
    CHECK(ev[0].scope == 0);
    CHECK(ev[0].key == "demo/example/some/long/key");

    CHECK(ev[1].kind == Event::Kind::interest);
    CHECK(ev[1].scope == ke_id);
    CHECK(ev[1].key.empty());
    // KEYEXPRS | SUBSCRIBERS, mode CurrentFuture — what a zenoh-rust publisher sends.
    CHECK(ev[1].options == 0x03);
    CHECK(ev[1].mode == static_cast<std::uint8_t>(InterestMode::current_future));

    // Every publication: the id, and not one byte of the key expression.
    CHECK(ev[2].kind == Event::Kind::put);
    CHECK(ev[2].scope == ke_id);
    CHECK(ev[2].key.empty());
    CHECK(ev[2].value == "one");
    CHECK(ev[3].kind == Event::Kind::put);
    CHECK(ev[3].scope == ke_id);
    CHECK(ev[3].value == "two");
    CHECK(ev[4].kind == Event::Kind::del);
    CHECK(ev[4].scope == ke_id);
    CHECK(ev[4].key.empty());
    CHECK(ev[4].value.empty());

    // undeclare() closes the interest, then releases the id — in that order, matching
    // the reference (which sends Interest{Final} from the publisher's own undeclare
    // and UndeclareKeyExpr when the declared key expression's last holder drops).
    CHECK(ev[5].kind == Event::Kind::interest_final);
    CHECK(ev[5].id == ev[1].id);
    CHECK(ev[6].kind == Event::Kind::undeclare_ke);
    CHECK(ev[6].id == ke_id);
}

TEST("undeclare releases the keyexpr id and is idempotent") {
    PubRouter router;
    auto sess = Session::open(endpoint(router.port()));
    CHECK(sess.has_value());
    if (!sess) {
        router.join();
        return;
    }
    auto pub = sess->declare_publisher("demo/a");
    CHECK(pub.has_value());
    if (!pub) {
        router.join();
        return;
    }
    std::uint16_t const ke_id = pub->keyexpr_id();

    pub->undeclare();
    pub->undeclare(); // idempotent: no second InterestFinal/UndeclareKeyExpr
    // Publishing through an undeclared handle is refused, not silently dropped.
    CHECK(pub->put(bytes("x")).error() == ZError::connection_closed);
    CHECK(pub->try_put(bytes("x")).error() == ZError::connection_closed);
    CHECK(pub->del().error() == ZError::connection_closed);

    sess->close();
    router.join();
    auto const ev = router.events();

    auto const fins = of_kind(ev, Event::Kind::interest_final);
    auto const undecls = of_kind(ev, Event::Kind::undeclare_ke);
    CHECK(fins.size() == 1);
    CHECK(undecls.size() == 1);
    if (undecls.size() == 1) CHECK(undecls[0].id == ke_id);
    CHECK(of_kind(ev, Event::Kind::put).empty());
}

TEST("two publishers on one key expression share a single declared id") {
    PubRouter router;
    auto sess = Session::open(endpoint(router.port()));
    CHECK(sess.has_value());
    if (!sess) {
        router.join();
        return;
    }

    auto a = sess->declare_publisher("demo/shared");
    auto b = sess->declare_publisher("demo/shared");
    auto c = sess->declare_publisher("demo/other");
    CHECK(a.has_value() && b.has_value() && c.has_value());
    if (!a || !b || !c) {
        router.join();
        return;
    }
    CHECK(a->keyexpr_id() == b->keyexpr_id());
    CHECK(c->keyexpr_id() != a->keyexpr_id());
    CHECK(a->keyexpr_id() != 0 && c->keyexpr_id() != 0);

    a->undeclare(); // the id is still held by `b`: no UndeclareKeyExpr yet
    CHECK(b->put(bytes("still here")).has_value());
    b->undeclare(); // last reference: now it goes
    c->undeclare(); // undeclare before close(), or the wire message never goes out

    sess->close();
    router.join();
    auto const ev = router.events();

    auto const decls = of_kind(ev, Event::Kind::declare_ke);
    CHECK(decls.size() == 2); // one per distinct key expression, not per publisher
    auto const undecls = of_kind(ev, Event::Kind::undeclare_ke);
    CHECK(undecls.size() == 2); // demo/shared (once, on b's undeclare) + demo/other
    CHECK(of_kind(ev, Event::Kind::interest).size() == 3);       // one per publisher
    CHECK(of_kind(ev, Event::Kind::interest_final).size() == 3); // ...and one per undeclare

    // The put between the two undeclares still used the shared id.
    auto const puts = of_kind(ev, Event::Kind::put);
    CHECK(puts.size() == 1);
    if (puts.size() == 1) {
        CHECK(puts[0].scope == decls[0].id);
        CHECK(puts[0].value == "still here");
    }
}

TEST("re-declaring a released key expression declares it again on the wire") {
    PubRouter router;
    auto sess = Session::open(endpoint(router.port()));
    CHECK(sess.has_value());
    if (!sess) {
        router.join();
        return;
    }

    std::uint16_t first = 0;
    {
        auto p = sess->declare_publisher("demo/transient");
        CHECK(p.has_value());
        if (!p) {
            router.join();
            return;
        }
        first = p->keyexpr_id();
        CHECK(first != 0);
    } // destructor undeclares, releasing the id

    // The router has been told to forget that id, so the second publisher on the same
    // key expression must bind it afresh rather than reuse the released registration.
    auto q = sess->declare_publisher("demo/transient");
    CHECK(q.has_value());
    if (!q) {
        router.join();
        return;
    }
    CHECK(q->keyexpr_id() != 0);
    CHECK(q->put(bytes("second life")).has_value());
    q->undeclare();

    sess->close();
    router.join();
    auto const ev = router.events();

    auto const decls = of_kind(ev, Event::Kind::declare_ke);
    CHECK(decls.size() == 2); // one per declaration, not one per key expression
    if (decls.size() == 2) {
        CHECK(decls[0].key == "demo/transient" && decls[1].key == "demo/transient");
        // Ids advance rather than being handed straight back, so a Push still in
        // flight under the old id can never be resolved against the new declaration.
        CHECK(decls[0].id != decls[1].id);
    }
    CHECK(of_kind(ev, Event::Kind::undeclare_ke).size() == 2);
    auto const puts = of_kind(ev, Event::Kind::put);
    CHECK(puts.size() == 1);
    if (puts.size() == 1 && decls.size() == 2) CHECK(puts[0].scope == decls[1].id);
}

TEST("PublisherOptions fix the congestion control and zid target of every publication") {
    PubRouter router;
    auto sess = Session::open(endpoint(router.port()));
    CHECK(sess.has_value());
    if (!sess) {
        router.join();
        return;
    }

    auto const self = sess->local_zid();
    auto pub = sess->declare_publisher(
        "cmd/arm", {.target_zid = self, .congestion = CongestionControl::block});
    CHECK(pub.has_value());
    if (!pub) {
        router.join();
        return;
    }
    CHECK(pub->congestion_control() == CongestionControl::block);
    CHECK(pub->target_zid().has_value());
    if (pub->target_zid()) CHECK(*pub->target_zid() == self);

    CHECK(pub->put(bytes("go")).has_value());
    CHECK(pub->try_put(bytes("go")).has_value());
    CHECK(pub->del().has_value());

    sess->close();
    router.join();
    auto const ev = router.events();

    // Bit 3 ("D") of the QoS byte is set on all three, and each carries the zid ext —
    // the point being that the settings are fixed once, not passed per publication.
    int checked = 0;
    for (auto const& e : ev) {
        if (e.kind != Event::Kind::put && e.kind != Event::Kind::del) continue;
        CHECK((e.qos & 0x08) != 0);
        CHECK(e.has_dest);
        ++checked;
    }
    CHECK(checked == 3);
}

TEST("a moved-from publisher undeclares nothing; move-assignment undeclares the target") {
    PubRouter router;
    auto sess = Session::open(endpoint(router.port()));
    CHECK(sess.has_value());
    if (!sess) {
        router.join();
        return;
    }

    auto a = sess->declare_publisher("demo/moved");
    CHECK(a.has_value());
    if (!a) {
        router.join();
        return;
    }
    std::uint16_t const a_id = a->keyexpr_id();

    {
        Publisher moved = std::move(*a); // `a` is now inert
        CHECK(moved.keyexpr_id() == a_id);
        CHECK(moved.key_expr() == "demo/moved");
        CHECK(moved.put(bytes("via move")).has_value());

        auto b = sess->declare_publisher("demo/target");
        CHECK(b.has_value());
        if (b) {
            // Assigning over `b` must undeclare what `b` held before taking `moved`.
            *b = std::move(moved);
            CHECK(b->keyexpr_id() == a_id);
            CHECK(b->put(bytes("after assign")).has_value());
        }
    } // `b`'s destructor undeclares demo/moved; `moved` is inert

    sess->close();
    router.join();
    auto const ev = router.events();

    auto const decls = of_kind(ev, Event::Kind::declare_ke);
    CHECK(decls.size() == 2); // demo/moved, demo/target
    auto const undecls = of_kind(ev, Event::Kind::undeclare_ke);
    CHECK(undecls.size() == 2); // demo/target (on move-assign), demo/moved (on scope exit)
    CHECK(of_kind(ev, Event::Kind::interest_final).size() == 2);
    auto const puts = of_kind(ev, Event::Kind::put);
    CHECK(puts.size() == 2);
    for (auto const& p : puts) CHECK(p.scope == a_id);
}

TEST("self-move-assignment leaves a publisher usable") {
    PubRouter router;
    auto sess = Session::open(endpoint(router.port()));
    CHECK(sess.has_value());
    if (!sess) {
        router.join();
        return;
    }
    auto pub = sess->declare_publisher("demo/self");
    CHECK(pub.has_value());
    if (!pub) {
        router.join();
        return;
    }
    auto& alias = *pub;
    alias = std::move(*pub); // NOLINT(clang-diagnostic-self-move) — the guarded path
    CHECK(pub->key_expr() == "demo/self");
    CHECK(pub->put(bytes("alive")).has_value());

    sess->close();
    router.join();
    CHECK(of_kind(router.events(), Event::Kind::put).size() == 1);
}

TEST("publishing after the session closes reports connection_closed") {
    PubRouter router;
    auto sess = Session::open(endpoint(router.port()));
    CHECK(sess.has_value());
    if (!sess) {
        router.join();
        return;
    }
    auto pub = sess->declare_publisher("demo/closed");
    CHECK(pub.has_value());
    if (!pub) {
        router.join();
        return;
    }
    sess->close();

    CHECK(pub->put(bytes("x")).error() == ZError::connection_closed);
    CHECK(pub->try_put(bytes("x")).error() == ZError::connection_closed);
    CHECK(pub->del().error() == ZError::connection_closed);
    // Declaring on a closed session fails the same way, before any wire traffic.
    auto late = sess->declare_publisher("demo/late");
    CHECK(!late.has_value());
    CHECK(late.error() == ZError::connection_closed);

    router.join();
}

TEST("a key expression too large for one batch fails the declaration and releases the id") {
    // batch_size 128 leaves no room for a 300-byte key expression, so the
    // DeclareKeyExpr cannot be encoded; the publisher then tries the Interest with
    // the key expression in full, which fails for the same reason.
    PubRouter router{128};
    auto sess = Session::open(endpoint(router.port()));
    CHECK(sess.has_value());
    if (!sess) {
        router.join();
        return;
    }

    std::string const huge(300, 'k');
    auto pub = sess->declare_publisher(huge);
    CHECK(!pub.has_value());
    if (!pub) CHECK(pub.error() == ZError::encode_error);

    // The session is still usable, and the failed declaration left nothing behind:
    // a publisher on a short key expression gets a fresh id and works.
    auto ok = sess->declare_publisher("demo/short");
    CHECK(ok.has_value());
    if (ok) {
        CHECK(ok->keyexpr_id() != 0);
        CHECK(ok->put(bytes("fine")).has_value());
    }

    sess->close();
    router.join();
    auto const ev = router.events();
    CHECK(of_kind(ev, Event::Kind::declare_ke).size() == 1); // only demo/short's
    auto const puts = of_kind(ev, Event::Kind::put);
    CHECK(puts.size() == 1);
    if (puts.size() == 1) CHECK(puts[0].value == "fine");
}

TEST("publisher and session puts interleave on one wire without disturbing each other") {
    PubRouter router;
    auto sess = Session::open(endpoint(router.port()));
    CHECK(sess.has_value());
    if (!sess) {
        router.join();
        return;
    }
    auto pub = sess->declare_publisher("demo/pub");
    CHECK(pub.has_value());
    if (!pub) {
        router.join();
        return;
    }

    CHECK(pub->put(bytes("p1")).has_value());
    CHECK(sess->put("demo/plain", bytes("s1")).has_value());
    CHECK(pub->try_put(bytes("p2")).has_value());
    CHECK(sess->try_put("demo/plain", bytes("s2")).has_value());

    sess->close();
    router.join();
    auto const puts = of_kind(router.events(), Event::Kind::put);
    CHECK(puts.size() == 4);
    if (puts.size() != 4) return;
    // Publisher puts carry the id and no text; session puts carry the text and no id.
    CHECK(puts[0].scope == pub->keyexpr_id() && puts[0].key.empty() && puts[0].value == "p1");
    CHECK(puts[1].scope == 0 && puts[1].key == "demo/plain" && puts[1].value == "s1");
    CHECK(puts[2].scope == pub->keyexpr_id() && puts[2].key.empty() && puts[2].value == "p2");
    CHECK(puts[3].scope == 0 && puts[3].key == "demo/plain" && puts[3].value == "s2");
}

TEST("past the declared-keyexpr cap a publisher falls back to the full key expression") {
    // The session declines to bind more ids than a router will remember (4096, the
    // same cap both this project's broker and the session's own receive path use).
    // The publisher past that point is still perfectly usable — it just publishes the
    // key expression in full, exactly as `Session::put` does.
    PubRouter router;
    auto sess = Session::open(endpoint(router.port()));
    CHECK(sess.has_value());
    if (!sess) {
        router.join();
        return;
    }

    constexpr std::size_t cap = 4096;
    std::vector<Publisher> pubs;
    pubs.reserve(cap);
    bool all_bound = true;
    for (std::size_t i = 0; i < cap; ++i) {
        auto p = sess->declare_publisher("demo/many/" + std::to_string(i));
        if (!p) {
            all_bound = false;
            break;
        }
        if (p->keyexpr_id() == 0) all_bound = false;
        pubs.push_back(std::move(*p));
    }
    CHECK(all_bound);
    CHECK(pubs.size() == cap);

    auto over = sess->declare_publisher("demo/over/the/cap");
    CHECK(over.has_value());
    if (!over) {
        router.join();
        return;
    }
    CHECK(over->keyexpr_id() == 0); // no id: the key expression travels in full
    CHECK(over->put(bytes("uncompressed")).has_value());
    CHECK(over->del().has_value());

    // Releasing one id makes room again.
    pubs.pop_back();
    auto again = sess->declare_publisher("demo/room/again");
    CHECK(again.has_value());
    if (again) CHECK(again->keyexpr_id() != 0);

    pubs.clear();
    sess->close();
    router.join();
    auto const ev = router.events();

    auto const puts = of_kind(ev, Event::Kind::put);
    CHECK(puts.size() == 1);
    if (puts.size() == 1) {
        CHECK(puts[0].scope == 0);
        CHECK(puts[0].key == "demo/over/the/cap");
        CHECK(puts[0].value == "uncompressed");
    }
    auto const dels = of_kind(ev, Event::Kind::del);
    CHECK(dels.size() == 1);
    if (dels.size() == 1) {
        CHECK(dels[0].scope == 0);
        CHECK(dels[0].key == "demo/over/the/cap");
    }
    // The capped publisher's Interest names the key expression instead of an id, and
    // its undeclare closes the interest without ever undeclaring a keyexpr id.
    auto const ints = of_kind(ev, Event::Kind::interest);
    bool found_text_interest = false;
    for (auto const& in : ints)
        if (in.scope == 0 && in.key == "demo/over/the/cap") found_text_interest = true;
    CHECK(found_text_interest);
    CHECK(of_kind(ev, Event::Kind::declare_ke).size() == cap + 1); // the cap + demo/room/again
}

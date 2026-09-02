module;

#include <algorithm>
#include <array>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <functional>
#include <memory>
#include <optional>
#include <random>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

module zenoh.session;

import zenoh.proto;
import zenoh.runtime.tcp;
import zenoh.runtime.strand;

// Implementation unit for zenoh.session: endpoint parsing, the 4-way transport
// handshake, and the put / try_put / get publish/query paths.
namespace zenoh {
namespace {

/// Default client lease (matches the reference): 10 s.
constexpr std::uint64_t default_lease_ms = 10'000;
/// Default `get()` reply-collection deadline when `GetOptions::timeout` is unset.
constexpr std::uint64_t default_get_timeout_ms = 10'000;
/// SN resolution mask for the default U32 frame-SN resolution (u32::MAX >> 4).
constexpr std::uint32_t sn_mask = 0x0fff'ffff;
/// Max bytes a single TCP batch can carry (the 2-byte length prefix is a u16).
constexpr std::size_t max_batch = 0xffff;
/// Conservative per-batch fixed overhead: the 2-byte length prefix + a reliable,
/// default-QoS FrameHeader (1 header byte + up to a 4-byte SN varint). Used to decide
/// when an API batch is full before the real header is encoded.
constexpr std::size_t frame_overhead = 8;
/// Upper bound on a resolved key expression (one TCP batch); rejects attacker-driven
/// oversized keys on the receive path.
constexpr std::size_t max_key_len = 0xffff;
/// Cap on distinct router-declared keyexpr ids we cache (bounds resmap memory).
constexpr std::size_t max_resmap_entries = 4096;

[[nodiscard]] auto io_to_zerr(IoError e) noexcept -> ZError {
    switch (e) {
    case IoError::would_block:
        return ZError::would_block;
    case IoError::closed:
        return ZError::connection_closed;
    default:
        return ZError::io_error;
    }
}

/// `PeerId` <-> `ZenohId` are the same 16-byte-array + length shape; the public
/// `zenoh` module just doesn't re-export `zenoh.proto.fields`, so this conversion
/// lives here at the boundary instead of being a public API.
[[nodiscard]] auto to_zenoh_id(const PeerId& p) noexcept -> ZenohId {
    ZenohId z{};
    z.len = p.len;
    z.bytes = p.bytes;
    return z;
}

[[nodiscard]] auto from_zenoh_id(const ZenohId& z) noexcept -> PeerId {
    PeerId p{};
    p.len = z.len;
    p.bytes = z.bytes;
    return p;
}

/// `GetConsolidation`/`GetTarget` <-> `zenoh.proto.fields`'s `ConsolidationMode`/
/// `QueryTarget`: same shape-mirroring reason as `to_zenoh_id`/`from_zenoh_id`.
[[nodiscard]] auto to_consolidation_mode(GetConsolidation c) noexcept -> ConsolidationMode {
    switch (c) {
    case GetConsolidation::none:
        return ConsolidationMode::none;
    case GetConsolidation::monotonic:
        return ConsolidationMode::monotonic;
    case GetConsolidation::latest:
        return ConsolidationMode::latest;
    case GetConsolidation::automatic:
        return ConsolidationMode::automatic;
    }
    return ConsolidationMode::automatic; // unreachable for a valid enumerator
}

[[nodiscard]] auto to_query_target(GetTarget t) noexcept -> QueryTarget {
    switch (t) {
    case GetTarget::all:
        return QueryTarget::all;
    case GetTarget::all_complete:
        return QueryTarget::all_complete;
    case GetTarget::best_matching:
        return QueryTarget::best_matching;
    }
    return QueryTarget::best_matching; // unreachable for a valid enumerator
}

/// The effective reply-collection deadline in milliseconds: `opts.timeout_ms` if set,
/// else `default_get_timeout_ms`.
[[nodiscard]] auto effective_timeout_ms(const GetOptions& opts) noexcept -> std::uint64_t {
    return opts.timeout_ms ? static_cast<std::uint64_t>(*opts.timeout_ms) : default_get_timeout_ms;
}

/// Parse "tcp/host:port", "host:port" (incl. "[ipv6]:port") into host + port.
[[nodiscard]] auto parse_endpoint(std::string_view ep, std::string& host, std::uint16_t& port)
    -> bool {
    if (ep.starts_with("tcp/")) ep.remove_prefix(4);
    if (ep.empty()) return false;

    std::size_t colon = std::string_view::npos;
    if (ep.front() == '[') { // [ipv6]:port
        auto const rb = ep.find(']');
        if (rb == std::string_view::npos || rb + 1 >= ep.size() || ep[rb + 1] != ':') return false;
        host = std::string(ep.substr(1, rb - 1));
        colon = rb + 1;
    } else {
        colon = ep.rfind(':');
        if (colon == std::string_view::npos) return false;
        host = std::string(ep.substr(0, colon));
    }
    auto const port_sv = ep.substr(colon + 1);
    if (host.empty() || port_sv.empty()) return false;

    std::uint16_t p = 0;
    auto const* first = port_sv.data();
    auto const* last = port_sv.data() + port_sv.size();
    auto const [ptr, ec] = std::from_chars(first, last, p);
    if (ec != std::errc{} || ptr != last) return false;
    port = p;
    return true;
}

/// Frame one transport message into `buf` with the 2-byte little-endian batch
/// length prefix; returns the total framed length (prefix + body).
template <class Msg>
[[nodiscard]] auto frame_message(std::vector<std::byte>& buf, const Msg& m)
    -> std::expected<std::size_t, ZError> {
    ByteWriter w{std::span(buf).subspan(2)};
    if (!m.encode(w)) return std::unexpected(ZError::encode_error);
    std::size_t const len = w.written();
    if (len > max_batch) return std::unexpected(ZError::encode_error);
    store_le<std::uint16_t>(buf.data(), static_cast<std::uint16_t>(len));
    return len + 2;
}

/// Read one TCP batch (2-byte LE length + body) into `buf`; returns body length.
[[nodiscard]] auto recv_batch(TcpLink& link, std::vector<std::byte>& buf)
    -> std::expected<std::size_t, ZError> {
    std::array<std::byte, 2> len_bytes{};
    if (auto r = link.read_exact(len_bytes); !r) return std::unexpected(io_to_zerr(r.error()));
    std::size_t const len = load_le<std::uint16_t>(len_bytes.data());
    if (buf.size() < len) buf.resize(len);
    if (auto r = link.read_exact(std::span(buf).first(len)); !r)
        return std::unexpected(io_to_zerr(r.error()));
    return len;
}

} // namespace

// The active subscriber registration (one per session, first cut). Holds the bounded
// strand and the optional callback; defined here so the non-movable Strand stays
// behind the session's `unique_ptr<SubReg>` and the Session itself remains movable.
struct SubReg {
    std::uint32_t id;
    std::string key;
    Strand<Sample> strand;
    SampleHandler handler; ///< empty for a pull-based (recv) subscriber
    SubReg(std::uint32_t i, std::string k, std::size_t cap, StrandMode m, SampleHandler h)
        : id(i), key(std::move(k)), strand(cap, m), handler(std::move(h)) {}
};

// Plain data popped from a queryable's strand; wrapped into an `IncomingQuery`
// (which needs a live `Session*`) only at delivery time — same reasoning as `Sample`
// being built fresh from a receive-buffer copy in `dispatch_cursor`.
struct PendingQuery {
    std::uint32_t rid = 0;
    std::string key;
    std::string params;
    std::vector<std::byte> payload; ///< Query::body's payload, if any (else empty)
};

// The active queryable registration (one per session, first cut). Mirrors `SubReg`.
struct QblReg {
    std::uint32_t id;
    std::string key;
    Strand<PendingQuery> strand;
    QueryHandler handler; ///< empty for a pull-based (recv) queryable
    QblReg(std::uint32_t i, std::string k, std::size_t cap, StrandMode m, QueryHandler h)
        : id(i), key(std::move(k)), strand(cap, m), handler(std::move(h)) {}
};

// One in-flight get()'s bookkeeping: the reply strand, an optional callback (empty
// for a pull-based `Getter`), whether the broker's `ResponseFinal` has been seen, and
// the client-enforced deadline (the broker does not enforce query timeouts in v1).
struct GetReg {
    Strand<GetReply> strand;
    GetReplyHandler handler;
    bool final = false;
    std::chrono::steady_clock::time_point deadline;
    GetReg(std::size_t cap, StrandMode mode, GetReplyHandler h,
           std::chrono::steady_clock::time_point dl)
        : strand(cap, mode), handler(std::move(h)), deadline(dl) {}
};

auto GetRegDeleter::operator()(GetReg* p) const noexcept -> void { delete p; }

Session::Session(Session&&) noexcept = default;
auto Session::operator=(Session&&) noexcept -> Session& = default;
Session::~Session() = default;

auto Session::open(std::string_view endpoint) -> std::expected<Session, ZError> {
    std::string host;
    std::uint16_t port = 0;
    if (!parse_endpoint(endpoint, host, port)) return std::unexpected(ZError::bad_endpoint);

    auto link = TcpLink::connect(host, port);
    if (!link) return std::unexpected(io_to_zerr(link.error()));

    Session s;
    s.link_ = std::move(*link);

    // A fresh, random 16-byte client Zenoh id.
    ZenohId zid{};
    zid.len = 16;
    std::random_device rd;
    for (std::size_t i = 0; i < zid.len; i += sizeof(unsigned)) {
        unsigned const v = rd();
        __builtin_memcpy(zid.bytes.data() + i, &v, sizeof(unsigned));
    }
    s.local_zid_ = zid;

    std::vector<std::byte> txbuf(4096);
    std::vector<std::byte> rxbuf(4096);

    // --- InitSyn -> InitAck ---
    InitSyn isyn{};
    isyn.version = 9;
    isyn.identifier.whatami = WhatAmI::client;
    isyn.identifier.zid = zid;
    if (auto framed = frame_message(txbuf, isyn); framed) {
        if (auto w = s.link_.write_all(std::span(txbuf).first(*framed)); !w)
            return std::unexpected(io_to_zerr(w.error()));
    } else {
        return std::unexpected(framed.error());
    }

    // The cookie is a view into rxbuf, which is not touched again until the OpenAck
    // recv_batch below — and OpenSyn (which copies the cookie bytes out) is framed
    // before that — so we can borrow it here instead of copying it into a vector.
    std::span<const std::byte> cookie;
    if (auto len = recv_batch(s.link_, rxbuf); len) {
        ByteReader r{std::span(rxbuf).first(*len)};
        auto ack = InitAck::decode(r);
        if (!ack || ack->version != 9) return std::unexpected(ZError::protocol_error);
        // Honor the router's batch size (it advertises an MTU below u16::MAX, e.g.
        // 65328): our batches, length prefix included, must not exceed it.
        s.batch_size_ = ack->resolution.batch_size;
        cookie = ack->cookie;
    } else {
        return std::unexpected(len.error());
    }

    // --- OpenSyn -> OpenAck ---
    // initial_sn is exchanged in the handshake, so we simply declare 0 and start our
    // frames there; the router adopts it as the expected baseline.
    OpenSyn osyn{};
    osyn.lease = Duration::from_millis(default_lease_ms);
    osyn.sn = 0;
    osyn.cookie = cookie;
    if (auto framed = frame_message(txbuf, osyn); framed) {
        if (auto w = s.link_.write_all(std::span(txbuf).first(*framed)); !w)
            return std::unexpected(io_to_zerr(w.error()));
    } else {
        return std::unexpected(framed.error());
    }

    if (auto len = recv_batch(s.link_, rxbuf); len) {
        ByteReader r{std::span(rxbuf).first(*len)};
        if (!OpenAck::decode(r)) return std::unexpected(ZError::protocol_error);
    } else {
        return std::unexpected(len.error());
    }

    // Data phase runs non-blocking so try_put can detect backpressure.
    if (auto r = s.link_.set_nonblocking(); !r) return std::unexpected(io_to_zerr(r.error()));
    s.frame_sn_ = 0;
    // Keepalive cadence = our declared lease / 4 (four keepalives per lease window).
    s.keepalive_ms_ = static_cast<std::int32_t>(default_lease_ms / 4);
    return s;
}

auto Session::local_zid() const noexcept -> PeerId { return from_zenoh_id(local_zid_); }

namespace {

/// Bit 3 ("D") of the QoS byte is the standard Zenoh congestion-control flag: set
/// means `Block` (this message must not be dropped), clear means `Drop`. Everything
/// else in the byte -- priority in bits 2:0, express in bit 4 -- is left at its
/// default, since this runtime implements neither.
[[nodiscard]] auto to_qos(CongestionControl cc) noexcept -> QoS {
    QoS qos{};
    if (cc == CongestionControl::block) qos.inner |= 0x08;
    return qos;
}

} // namespace

auto Session::encode_put(std::string_view key_expr, std::span<const std::byte> payload,
                         const PutOptions& opts) -> std::expected<void, ZError> {
    Push push{};
    push.wire_expr = WireExpr{.scope = 0, .mapping = Mapping::sender, .suffix = key_expr};
    push.qos = to_qos(opts.congestion);
    if (opts.target_zid) push.dest = DestinationId{.zid = to_zenoh_id(*opts.target_zid)};
    Put put{};
    put.payload = payload;
    push.payload = PushBody{.body = std::move(put)};

    FrameHeader fh{};
    fh.reliability = Reliability::reliable;
    fh.sn = frame_sn_;

    std::size_t const cap = 2 + key_expr.size() + payload.size() + 64;
    if (tx_scratch_.size() < cap) tx_scratch_.resize(cap);

    ByteWriter w{std::span(tx_scratch_).subspan(2)};
    if (!fh.encode(w) || !push.encode(w)) return std::unexpected(ZError::encode_error);
    std::size_t const len = w.written();
    if (2 + len > batch_size_) return std::unexpected(ZError::encode_error);
    store_le<std::uint16_t>(tx_scratch_.data(), static_cast<std::uint16_t>(len));
    // The framed batch occupies tx_scratch_[0 .. len + 2). SN is advanced by the
    // caller only once the frame is committed to the wire (or buffered).
    return {};
}

auto Session::encode_put_head(std::string_view key_expr, std::span<const std::byte> payload,
                              const PutOptions& opts) -> std::expected<std::size_t, ZError> {
    Push push{};
    push.wire_expr = WireExpr{.scope = 0, .mapping = Mapping::sender, .suffix = key_expr};
    push.qos = to_qos(opts.congestion);
    if (opts.target_zid) push.dest = DestinationId{.zid = to_zenoh_id(*opts.target_zid)};
    Put put{};
    put.payload = payload; // present only so the encoded length prefix is correct
    push.payload = PushBody{.body = std::move(put)};

    FrameHeader fh{};
    fh.reliability = Reliability::reliable;
    fh.sn = frame_sn_;

    // Header only — no room reserved for the payload, which is sent via writev.
    std::size_t const cap = 2 + key_expr.size() + 64;
    if (tx_scratch_.size() < cap) tx_scratch_.resize(cap);

    ByteWriter w{std::span(tx_scratch_).subspan(2)};
    if (!fh.encode(w) || !push.encode_head(w)) return std::unexpected(ZError::encode_error);
    std::size_t const head = w.written();
    std::size_t const content = head + payload.size();
    if (2 + content > batch_size_) return std::unexpected(ZError::encode_error);
    // The batch length prefix covers the header *and* the not-yet-written payload.
    store_le<std::uint16_t>(tx_scratch_.data(), static_cast<std::uint16_t>(content));
    return 2 + head;
}

auto Session::flush_pending() -> std::expected<void, ZError> {
    if (pending_off_ >= tx_pending_.size()) {
        tx_pending_.clear();
        pending_off_ = 0;
        return {};
    }
    auto const rest = std::span(tx_pending_).subspan(pending_off_);
    auto n = link_.write_some(rest);
    if (!n) return std::unexpected(io_to_zerr(n.error()));
    pending_off_ += *n;
    if (pending_off_ >= tx_pending_.size()) {
        tx_pending_.clear();
        pending_off_ = 0;
        return {};
    }
    return std::unexpected(ZError::would_block); // still partially buffered
}

auto Session::put(std::string_view key_expr, std::span<const std::byte> payload, PutOptions opts)
    -> std::expected<void, ZError> {
    // Drain any bytes a prior try_put left buffered (blocking).
    if (pending_off_ < tx_pending_.size()) {
        if (auto r = link_.write_all(std::span(tx_pending_).subspan(pending_off_)); !r)
            return std::unexpected(io_to_zerr(r.error()));
    }
    tx_pending_.clear();
    pending_off_ = 0;

    // Scatter-gather: encode just the header into tx_scratch_ and write it together
    // with the borrowed payload, so the payload is never copied into a staging buffer.
    auto const head = encode_put_head(key_expr, payload, opts);
    if (!head) return std::unexpected(head.error());

    if (auto r = link_.writev_all(std::span(tx_scratch_).first(*head), payload); !r)
        return std::unexpected(io_to_zerr(r.error()));
    frame_sn_ = (frame_sn_ + 1) & sn_mask;
    return {};
}

auto Session::try_put(std::string_view key_expr, std::span<const std::byte> payload,
                      PutOptions opts) -> std::expected<void, ZError> {
    // Don't interleave a new frame ahead of buffered bytes: flush first.
    if (pending_off_ < tx_pending_.size()) {
        if (auto f = flush_pending(); !f) return std::unexpected(f.error());
    }

    if (auto e = encode_put(key_expr, payload, opts); !e) return std::unexpected(e.error());
    std::size_t const framed =
        static_cast<std::size_t>(load_le<std::uint16_t>(tx_scratch_.data())) + 2;
    auto const batch = std::span(tx_scratch_).first(framed);

    auto n = link_.write_some(batch);
    if (!n) {
        // Nothing went out and the SN was not consumed: the frame is simply not sent.
        if (n.error() == IoError::would_block) return std::unexpected(ZError::would_block);
        return std::unexpected(io_to_zerr(n.error()));
    }

    frame_sn_ = (frame_sn_ + 1) & sn_mask; // frame committed (fully or partially)
    if (*n == framed) return {};

    // Partial write: buffer the tail so the stream stays intact, and report
    // backpressure to the caller.
    tx_pending_.assign(batch.begin() + static_cast<std::ptrdiff_t>(*n), batch.end());
    pending_off_ = 0;
    // In this case we wrote some and committed to write the rest, thus we should
    // return OK. In other terms we only return EWOULDBLOCK if we could not write
    // at all the message.
    return {};
}

auto Session::close() -> void {
    if (!link_.valid()) return;
    // Best-effort: flush anything buffered, then send a Close(session).
    if (pending_off_ < tx_pending_.size())
        (void)link_.write_all(std::span(tx_pending_).subspan(pending_off_));
    tx_pending_.clear();
    pending_off_ = 0;

    Close c{};
    c.reason = 0;
    c.behaviour = CloseBehaviour::session;
    std::vector<std::byte> buf(64);
    if (auto framed = frame_message(buf, c)) (void)link_.write_all(std::span(buf).first(*framed));

    link_ = TcpLink{}; // close the socket
}

auto Session::write_frame(std::span<const std::byte> push_bytes) -> std::expected<void, ZError> {
    if (push_bytes.empty()) return {};

    // Drain any try_put backlog first so frames stay in order on the wire.
    if (pending_off_ < tx_pending_.size()) {
        if (auto r = link_.write_all(std::span(tx_pending_).subspan(pending_off_)); !r)
            return std::unexpected(io_to_zerr(r.error()));
    }
    tx_pending_.clear();
    pending_off_ = 0;

    FrameHeader fh{};
    fh.reliability = Reliability::reliable;
    fh.sn = frame_sn_;

    std::size_t const cap = 2 + frame_overhead + push_bytes.size();
    if (tx_scratch_.size() < cap) tx_scratch_.resize(cap);

    ByteWriter w{std::span(tx_scratch_).subspan(2)};
    if (!fh.encode(w)) return std::unexpected(ZError::encode_error);
    std::size_t const hdr = w.written();
    std::size_t const content = hdr + push_bytes.size();
    if (2 + content > batch_size_) return std::unexpected(ZError::encode_error);

    __builtin_memcpy(tx_scratch_.data() + 2 + hdr, push_bytes.data(), push_bytes.size());
    store_le<std::uint16_t>(tx_scratch_.data(), static_cast<std::uint16_t>(content));

    if (auto r = link_.write_all(std::span(tx_scratch_).first(2 + content)); !r)
        return std::unexpected(io_to_zerr(r.error()));
    frame_sn_ = (frame_sn_ + 1) & sn_mask;
    return {};
}

auto Session::batch() -> Batch { return Batch(this); }

// --- Batch ---

Batch::Batch(Batch&& other) noexcept
    : session_(other.session_), buf_(std::move(other.buf_)), body_len_(other.body_len_),
      count_(other.count_) {
    other.session_ = nullptr;
    other.body_len_ = 0;
    other.count_ = 0;
}

auto Batch::operator=(Batch&& other) noexcept -> Batch& {
    if (this != &other) {
        // Flush our own buffered messages before adopting the other's state.
        if (session_ != nullptr && body_len_ != 0)
            (void)session_->write_frame(std::span(buf_).first(body_len_));
        session_ = other.session_;
        buf_ = std::move(other.buf_);
        body_len_ = other.body_len_;
        count_ = other.count_;
        other.session_ = nullptr;
        other.body_len_ = 0;
        other.count_ = 0;
    }
    return *this;
}

Batch::~Batch() {
    if (session_ != nullptr && body_len_ != 0)
        (void)session_->write_frame(std::span(buf_).first(body_len_));
}

auto Batch::put(std::string_view key_expr, std::span<const std::byte> payload, PutOptions opts)
    -> std::expected<void, ZError> {
    if (session_ == nullptr) return std::unexpected(ZError::connection_closed);

    // Encode the Push(Put) in place at the end of the current body.
    std::size_t const msg_max = 64 + key_expr.size() + payload.size();
    if (buf_.size() < body_len_ + msg_max) buf_.resize(body_len_ + msg_max);

    Push push{};
    push.wire_expr = WireExpr{.scope = 0, .mapping = Mapping::sender, .suffix = key_expr};
    push.qos = to_qos(opts.congestion);
    if (opts.target_zid) push.dest = DestinationId{.zid = to_zenoh_id(*opts.target_zid)};
    Put put{};
    put.payload = payload;
    push.payload = PushBody{.body = std::move(put)};

    ByteWriter w{std::span(buf_).subspan(body_len_)};
    if (!push.encode(w)) return std::unexpected(ZError::encode_error);
    std::size_t const msg_len = w.written();

    // A single Put must fit in a frame on its own.
    if (frame_overhead + msg_len > session_->batch_size_)
        return std::unexpected(ZError::encode_error);

    if (body_len_ != 0 && frame_overhead + body_len_ + msg_len > session_->batch_size_) {
        // Adding this message would overflow the batch: send what we have, then keep
        // this message (already encoded just past the old body) as the next frame's
        // first message. write_frame does not touch buf_, so the bytes survive.
        std::size_t const prev = body_len_;
        if (auto r = session_->write_frame(std::span(buf_).first(prev)); !r)
            return std::unexpected(r.error());
        __builtin_memmove(buf_.data(), buf_.data() + prev, msg_len);
        body_len_ = msg_len;
        count_ = 1;
    } else {
        body_len_ += msg_len;
        ++count_;
    }
    return {};
}

auto Batch::flush() -> std::expected<void, ZError> {
    if (session_ == nullptr) return std::unexpected(ZError::connection_closed);
    if (body_len_ == 0) return {};
    auto r = session_->write_frame(std::span(buf_).first(body_len_));
    body_len_ = 0;
    count_ = 0;
    return r;
}

// --- Subscriber declaration / teardown ---

auto Session::write_declare_subscriber(std::uint32_t id, std::string_view key)
    -> std::expected<void, ZError> {
    Declare d{};
    DeclareSubscriber ds{};
    ds.id = id;
    ds.wire_expr = WireExpr{.scope = 0, .mapping = Mapping::sender, .suffix = key};
    d.body = DeclareBody{.body = ds};

    std::vector<std::byte> tmp(key.size() + 64);
    ByteWriter w{tmp};
    if (!d.encode(w)) return std::unexpected(ZError::encode_error);
    return write_frame(std::span(tmp).first(w.written())); // wraps in a FrameHeader, advances SN
}

auto Session::write_undeclare_subscriber(std::uint32_t id) -> void {
    if (!link_.valid()) return;
    Declare d{};
    UndeclareSubscriber us{};
    us.id = id;
    d.body = DeclareBody{.body = us};

    std::vector<std::byte> tmp(64);
    ByteWriter w{tmp};
    if (!d.encode(w)) return;
    (void)write_frame(std::span(tmp).first(w.written())); // best-effort
}

auto Session::declare_subscriber(std::string_view key_expr, SubscriberOptions opts)
    -> std::expected<Subscriber, ZError> {
    return declare_subscriber(key_expr, SampleHandler{}, opts);
}

auto Session::declare_subscriber(std::string_view key_expr, SampleHandler on_sample,
                                 SubscriberOptions opts) -> std::expected<Subscriber, ZError> {
    if (sub_) return std::unexpected(ZError::already_subscribed);
    if (!link_.valid()) return std::unexpected(ZError::connection_closed);
    std::uint32_t const id = next_entity_id_++;
    if (auto r = write_declare_subscriber(id, key_expr); !r) return std::unexpected(r.error());
    sub_ = std::make_unique<SubReg>(id, std::string(key_expr), opts.capacity, opts.mode,
                                    std::move(on_sample));
    return Subscriber{this};
}

auto Session::sub_drop() -> void {
    if (sub_) {
        write_undeclare_subscriber(sub_->id);
        sub_.reset();
    }
}

// --- Queryable declaration / teardown ---

auto Session::write_declare_queryable(std::uint32_t id, std::string_view key, QueryableInfo qinfo)
    -> std::expected<void, ZError> {
    Declare d{};
    DeclareQueryable dq{};
    dq.id = id;
    dq.wire_expr = WireExpr{.scope = 0, .mapping = Mapping::sender, .suffix = key};
    dq.qinfo = qinfo;
    d.body = DeclareBody{.body = dq};

    std::vector<std::byte> tmp(key.size() + 64);
    ByteWriter w{tmp};
    if (!d.encode(w)) return std::unexpected(ZError::encode_error);
    return write_frame(std::span(tmp).first(w.written()));
}

auto Session::write_undeclare_queryable(std::uint32_t id) -> void {
    if (!link_.valid()) return;
    Declare d{};
    UndeclareQueryable uq{};
    uq.id = id;
    d.body = DeclareBody{.body = uq};

    std::vector<std::byte> tmp(64);
    ByteWriter w{tmp};
    if (!d.encode(w)) return;
    (void)write_frame(std::span(tmp).first(w.written())); // best-effort
}

auto Session::declare_queryable(std::string_view key_expr, QueryableOptions opts)
    -> std::expected<Queryable, ZError> {
    return declare_queryable(key_expr, QueryHandler{}, opts);
}

auto Session::declare_queryable(std::string_view key_expr, QueryHandler on_query,
                                QueryableOptions opts) -> std::expected<Queryable, ZError> {
    if (qbl_) return std::unexpected(ZError::already_queryable);
    if (!link_.valid()) return std::unexpected(ZError::connection_closed);
    std::uint32_t const id = next_entity_id_++;
    QueryableInfo const qinfo{.complete = opts.complete, .distance = opts.distance};
    if (auto r = write_declare_queryable(id, key_expr, qinfo); !r)
        return std::unexpected(r.error());
    qbl_ = std::make_unique<QblReg>(id, std::string(key_expr), opts.capacity, opts.mode,
                                    std::move(on_query));
    return Queryable{this};
}

auto Session::qbl_drop() -> void {
    if (qbl_) {
        write_undeclare_queryable(qbl_->id);
        qbl_.reset();
    }
}

auto Session::send_response(std::uint32_t rid, std::string_view key_expr,
                            std::span<const std::byte> payload, bool is_err)
    -> std::expected<void, ZError> {
    Response rsp{};
    rsp.rid = rid;
    rsp.wire_expr = WireExpr{.scope = 0, .mapping = Mapping::sender, .suffix = key_expr};
    if (is_err) {
        Err e{};
        e.payload = payload;
        rsp.payload.body = e;
    } else {
        Put put{};
        put.payload = payload;
        Reply rep{};
        rep.payload = PushBody{.body = put};
        rsp.payload.body = rep;
    }

    std::vector<std::byte> tmp(64 + key_expr.size() + payload.size());
    ByteWriter w{tmp};
    if (!rsp.encode(w)) return std::unexpected(ZError::encode_error);
    return write_frame(std::span(tmp).first(w.written()));
}

auto Session::send_response_final(std::uint32_t rid) -> void {
    if (!link_.valid()) return;
    ResponseFinal rf{};
    rf.rid = rid;
    std::vector<std::byte> tmp(32);
    ByteWriter w{tmp};
    if (!rf.encode(w)) return;
    (void)write_frame(std::span(tmp).first(w.written())); // best-effort
}

// --- get() / Getter ---

auto Session::write_request(std::uint32_t rid, std::string_view key_expr,
                            std::string_view parameters, const GetOptions& opts)
    -> std::expected<void, ZError> {
    Request req{};
    req.id = rid;
    req.wire_expr = WireExpr{.scope = 0, .mapping = Mapping::sender, .suffix = key_expr};
    req.target = to_query_target(opts.target);
    req.qos = to_qos(opts.congestion);
    req.timeout = Duration::from_millis(effective_timeout_ms(opts));
    if (opts.target_zid) req.dest = DestinationId{.zid = to_zenoh_id(*opts.target_zid)};

    Query q{};
    q.consolidation = to_consolidation_mode(opts.consolidation);
    q.parameters = parameters;
    req.payload = RequestBody{.query = q};

    std::vector<std::byte> tmp(64 + key_expr.size() + parameters.size());
    ByteWriter w{tmp};
    if (!req.encode(w)) return std::unexpected(ZError::encode_error);
    return write_frame(std::span(tmp).first(w.written()));
}

auto Session::start_get(std::string_view key_expr, std::string_view parameters,
                        GetReplyHandler handler, const GetOptions& opts)
    -> std::expected<std::uint32_t, ZError> {
    if (!link_.valid()) return std::unexpected(ZError::connection_closed);
    std::uint32_t const rid = next_request_id_++;
    if (auto r = write_request(rid, key_expr, parameters, opts); !r)
        return std::unexpected(r.error());
    auto const deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(effective_timeout_ms(opts));
    pending_gets_[rid] = std::unique_ptr<GetReg, GetRegDeleter>(
        new GetReg(256, StrandMode::ordered, std::move(handler), deadline));
    return rid;
}

auto Session::get(std::string_view key_expr, std::string_view parameters, GetOptions opts)
    -> std::expected<Getter, ZError> {
    auto rid = start_get(key_expr, parameters, GetReplyHandler{}, opts);
    if (!rid) return std::unexpected(rid.error());
    return Getter{this, *rid};
}

auto Session::get(std::string_view key_expr, std::string_view parameters, GetReplyHandler on_reply,
                  GetOptions opts) -> std::expected<void, ZError> {
    auto rid = start_get(key_expr, parameters, std::move(on_reply), opts);
    if (!rid) return std::unexpected(rid.error());
    return {};
}

auto Session::get_recv(std::uint32_t rid) -> std::expected<std::optional<GetReply>, ZError> {
    for (;;) {
        auto it = pending_gets_.find(rid);
        if (it == pending_gets_.end())
            return std::optional<GetReply>{std::nullopt}; // already cleaned up: treat as done
        if (auto r = it->second->strand.pop()) return std::move(r);
        if (it->second->final) {
            pending_gets_.erase(it);
            return std::optional<GetReply>{std::nullopt};
        }
        auto const now = std::chrono::steady_clock::now();
        if (now >= it->second->deadline) {
            pending_gets_.erase(it);
            return std::unexpected(ZError::query_timeout);
        }
        // Bound this pump_step's wait to what's left of the deadline, so a short
        // GetOptions::timeout is noticed as soon as it elapses rather than only after
        // pump_step's normal (much longer) keepalive-cadence wait returns.
        auto const remaining_ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(it->second->deadline - now)
                .count();
        if (auto p = pump_step(static_cast<std::int32_t>(remaining_ms)); !p)
            return std::unexpected(p.error());
    }
}

auto Session::get_drop(std::uint32_t rid) -> void { pending_gets_.erase(rid); }

// --- Receive pump ---

auto Session::send_keepalive() -> std::expected<void, ZError> {
    // Flush any try_put backlog first so the keepalive doesn't jump the byte stream.
    if (pending_off_ < tx_pending_.size()) {
        if (auto r = link_.write_all(std::span(tx_pending_).subspan(pending_off_)); !r)
            return std::unexpected(io_to_zerr(r.error()));
        tx_pending_.clear();
        pending_off_ = 0;
    }
    KeepAlive ka{};
    std::vector<std::byte> buf(16);
    auto framed = frame_message(buf, ka); // SN-less transport message, its own batch
    if (!framed) return std::unexpected(framed.error());
    if (auto r = link_.write_all(std::span(buf).first(*framed)); !r)
        return std::unexpected(io_to_zerr(r.error()));
    return {};
}

auto Session::resolve_key(const WireExpr& we) -> std::expected<std::string, ZError> {
    if (we.scope == 0) { // literal suffix — the common case (router pushes scope 0)
        if (we.suffix.size() > max_key_len) return std::unexpected(ZError::protocol_error);
        return std::string(we.suffix);
    }
    // Numeric keyexpr id bound by a router DeclareKeyExpr; never operator[] (no insert).
    auto it = resmap_.find(we.scope);
    if (it == resmap_.end()) return std::unexpected(ZError::protocol_error);
    if (it->second.size() + we.suffix.size() > max_key_len)
        return std::unexpected(ZError::protocol_error);
    std::string out = it->second;
    out.append(we.suffix);
    return out;
}

auto Session::dispatch_cursor() -> std::expected<void, ZError> {
    while (rx_pos_ < rx_end_) {
        std::span<const std::byte> const body{rx_buf_.data() + rx_pos_, rx_end_ - rx_pos_};
        ByteReader r{body};
        auto pk = r.peek();
        if (!pk) {
            fault_ = ZError::protocol_error;
            return std::unexpected(*fault_);
        }
        std::uint8_t const mid = std::to_integer<std::uint8_t>(*pk) & mid_mask;

        if (mid == Push::id) {
            auto push = Push::decode(r);
            if (!push) {
                fault_ = ZError::protocol_error;
                return std::unexpected(*fault_);
            }
            auto key = resolve_key(push->wire_expr);
            if (!key) {
                fault_ = key.error();
                return std::unexpected(*fault_);
            }
            SampleKind kind = SampleKind::del;
            std::vector<std::byte> payload;
            if (auto const* put = std::get_if<Put>(&push->payload.body)) {
                kind = SampleKind::put;
                payload.assign(put->payload.begin(), put->payload.end()); // copy out of rx_buf_
            }
            if (sub_) {
                // `*key` is a stable local; the strand may copy it, so keep it alive
                // independently of the moved-in Sample.
                Sample sample{*key, std::move(payload), kind};
                if (sub_->strand.post(*key, std::move(sample)) == PostResult::full) {
                    // Strand full: leave the cursor; the consumer drains, then we retry.
                    return {};
                }
            }
            rx_pos_ = rx_end_ - r.remaining(); // committed: advance past this message
        } else if (mid == Declare::mid) {
            auto dec = Declare::decode(r);
            if (!dec) {
                fault_ = ZError::protocol_error;
                return std::unexpected(*fault_);
            }
            // Maintain the resmap from router keyexpr (un)declarations; ignore the rest.
            if (auto const* dk = std::get_if<DeclareKeyExpr>(&dec->body.body)) {
                if (auto k = resolve_key(dk->wire_expr); k && k->size() <= max_key_len) {
                    if (resmap_.size() < max_resmap_entries || resmap_.contains(dk->id))
                        resmap_[dk->id] = std::move(*k);
                }
            } else if (auto const* uk = std::get_if<UndeclareKeyExpr>(&dec->body.body)) {
                resmap_.erase(uk->id);
            }
            rx_pos_ = rx_end_ - r.remaining();
        } else if (mid == Request::mid) {
            auto req = Request::decode(r);
            if (!req) {
                fault_ = ZError::protocol_error;
                return std::unexpected(*fault_);
            }
            // `req->dest` (zid-targeting) is not re-filtered here: the broker already
            // restricts fan-out to the matching peer before a Request ever reaches
            // us, so by the time it's here it's already known to be for us.
            if (qbl_) {
                auto key = resolve_key(req->wire_expr);
                if (!key) {
                    fault_ = key.error();
                    return std::unexpected(*fault_);
                }
                std::vector<std::byte> payload;
                if (auto const& body = req->payload.query.body) {
                    payload.assign(body->payload.begin(), body->payload.end());
                }
                PendingQuery pq{.rid = req->id,
                                .key = *key,
                                .params = std::string(req->payload.query.parameters),
                                .payload = std::move(payload)};
                if (qbl_->strand.post(*key, std::move(pq)) == PostResult::full) {
                    return {}; // pause; retry next pump
                }
            } else {
                // No queryable declared on this session: nobody will ever construct an
                // IncomingQuery to finalize this request, so finalize it ourselves —
                // otherwise the broker's fan-in counter for this rid waits forever.
                send_response_final(req->id);
            }
            rx_pos_ = rx_end_ - r.remaining();
        } else if (mid == Response::id) {
            auto rsp = Response::decode(r);
            if (!rsp) {
                fault_ = ZError::protocol_error;
                return std::unexpected(*fault_);
            }
            if (auto it = pending_gets_.find(rsp->rid); it != pending_gets_.end()) {
                GetReply gr{};
                PostResult post_result = PostResult::posted;
                if (auto const* reply = std::get_if<Reply>(&rsp->payload.body)) {
                    auto key = resolve_key(rsp->wire_expr);
                    if (!key) {
                        fault_ = key.error();
                        return std::unexpected(*fault_);
                    }
                    gr.ok_ = true;
                    SampleKind kind = SampleKind::del;
                    std::vector<std::byte> payload;
                    if (auto const* put = std::get_if<Put>(&reply->payload.body)) {
                        kind = SampleKind::put;
                        payload.assign(put->payload.begin(), put->payload.end());
                    }
                    gr.sample_ = Sample{*key, std::move(payload), kind};
                    post_result = it->second->strand.post(*key, std::move(gr));
                } else if (auto const* err = std::get_if<Err>(&rsp->payload.body)) {
                    gr.ok_ = false;
                    gr.err_payload_.assign(err->payload.begin(), err->payload.end());
                    post_result = it->second->strand.post(std::string_view{}, std::move(gr));
                }
                if (post_result == PostResult::full) return {}; // pause; retry next pump
            }
            // else: unknown/already-cleaned-up rid (e.g. client-side timeout already
            // fired) — silently ignore, matching resolve_key's "tolerate the unknown"
            // spirit for anything that isn't a stream-desync signal.
            rx_pos_ = rx_end_ - r.remaining();
        } else if (mid == ResponseFinal::id) {
            auto rf = ResponseFinal::decode(r);
            if (!rf) {
                fault_ = ZError::protocol_error;
                return std::unexpected(*fault_);
            }
            if (auto it = pending_gets_.find(rf->rid); it != pending_gets_.end())
                it->second->final = true;
            rx_pos_ = rx_end_ - r.remaining();
        } else if (mid == FrameHeader::id) {
            // The next transport message in this batch: a new frame. Its body simply
            // continues this loop -- nothing in the dispatch below depends on frame
            // context, and the SN is the transport's business, not the dispatcher's.
            if (!FrameHeader::decode(r)) {
                fault_ = ZError::protocol_error;
                return std::unexpected(*fault_);
            }
            rx_pos_ = rx_end_ - r.remaining();
        } else if (mid == KeepAlive::id) {
            if (!KeepAlive::decode(r)) {
                fault_ = ZError::protocol_error;
                return std::unexpected(*fault_);
            }
            rx_pos_ = rx_end_ - r.remaining();
        } else if (mid == Close::id) {
            fault_ = ZError::connection_closed; // router closed the session
            return std::unexpected(*fault_);
        } else {
            // Anything else -- an unknown transport message, or a network message a
            // client never routes (OAM, Request) -- carries no length, so the cursor
            // cannot be advanced past it and the rest of the batch is unreadable.
            // Sticky fault, as before: guessing is how a decoder starts inventing
            // messages out of payload bytes. (zenoh-rust ends the link here too --
            // `read_messages` fails once a byte decodes as neither kind.)
            fault_ = ZError::protocol_error;
            return std::unexpected(*fault_);
        }
    }
    rx_pos_ = rx_end_ = 0; // batch fully consumed
    return {};
}

auto Session::pump_step(std::optional<std::int32_t> max_wait_ms) -> std::expected<void, ZError> {
    if (fault_) return std::unexpected(*fault_);     // sticky terminal fault — never resync
    if (rx_pos_ < rx_end_) return dispatch_cursor(); // resume an in-progress frame

    // A caller-supplied max_wait_ms only ever shortens this call's poll (e.g.
    // get_recv bounding it to a get()'s own deadline so a short GetOptions::timeout
    // is noticed promptly); it never lengthens it past the normal keepalive cadence.
    std::int32_t const wait = max_wait_ms ? std::min(*max_wait_ms, keepalive_ms_) : keepalive_ms_;
    auto ready = link_.poll_readable(wait);
    if (!ready) {
        fault_ = io_to_zerr(ready.error());
        return std::unexpected(*fault_);
    }
    if (!*ready) {
        // Only a *real* keepalive-interval-long idle period sends a keepalive — a
        // shortened wait timing out just means "no data yet, let the caller re-check
        // its own deadline", not "the link has been idle for a full keepalive_ms_".
        if (wait >= keepalive_ms_) {
            if (auto r = send_keepalive(); !r) {
                fault_ = r.error();
                return std::unexpected(*fault_);
            }
        }
        return {};
    }

    auto len = recv_batch(link_, rx_buf_);
    if (!len) {
        fault_ = len.error();
        return std::unexpected(*fault_);
    }
    if (*len == 0) return {};

    // A batch is a *sequence* of transport messages, not one message: the reference
    // packs a KeepAlive (or a Close, or a second Frame) into whatever batch is
    // currently staging -- see zenoh-rust's `TransmissionPipeline::push_transport_
    // message`. Handing the whole batch to one cursor loop is what keeps
    // `[Frame][Push][KeepAlive]` from looking like a desync and `[KeepAlive][Frame]`
    // from silently discarding the frame behind it.
    rx_pos_ = 0;
    rx_end_ = *len;
    return dispatch_cursor();
}

auto Session::sub_recv() -> std::expected<Sample, ZError> {
    for (;;) {
        if (!sub_) return std::unexpected(ZError::connection_closed);
        if (auto s = sub_->strand.pop()) return std::move(*s);
        // A pump that ends in an error may still have posted messages from the same
        // batch before hitting it -- a graceful Close, for instance, is routinely
        // packed in behind a frame. Hand those to the caller first; the error is
        // sticky, so it is still waiting once the queue drains.
        if (auto r = pump_step(); !r && sub_->strand.empty()) return std::unexpected(r.error());
    }
}

auto Session::qbl_recv() -> std::expected<IncomingQuery, ZError> {
    for (;;) {
        if (!qbl_) return std::unexpected(ZError::connection_closed);
        if (auto pq = qbl_->strand.pop())
            return IncomingQuery(this, pq->rid, std::move(pq->key), std::move(pq->params),
                                 std::move(pq->payload));
        // A pump that ends in an error may still have posted messages from the same
        // batch before hitting it -- a graceful Close, for instance, is routinely
        // packed in behind a frame. Hand those to the caller first; the error is
        // sticky, so it is still waiting once the queue drains.
        if (auto r = pump_step(); !r && qbl_->strand.empty()) return std::unexpected(r.error());
    }
}

auto Session::run_once() -> std::expected<void, ZError> {
    if (auto r = pump_step(); !r) return std::unexpected(r.error());
    if (sub_ && sub_->handler) {
        while (auto s = sub_->strand.pop()) sub_->handler(*s); // deliver to the callback
    }
    if (qbl_ && qbl_->handler) {
        while (auto pq = qbl_->strand.pop())
            qbl_->handler(IncomingQuery(this, pq->rid, std::move(pq->key), std::move(pq->params),
                                        std::move(pq->payload)));
    }
    // Drain callback-style get()s. Pull-style (handler-empty) entries are owned
    // entirely by their Getter (recv()'s own pump loop and ~Getter()'s cleanup), so
    // they're deliberately left untouched here to avoid two owners racing to erase
    // the same map entry.
    for (auto it = pending_gets_.begin(); it != pending_gets_.end();) {
        auto& reg = *it->second;
        if (!reg.handler) {
            ++it;
            continue;
        }
        while (auto r = reg.strand.pop()) reg.handler(*r);
        if ((reg.final && reg.strand.empty()) || std::chrono::steady_clock::now() >= reg.deadline)
            it = pending_gets_.erase(it);
        else
            ++it;
    }
    return {};
}

auto Session::run() -> std::expected<void, ZError> {
    for (;;) {
        if (auto r = run_once(); !r) return std::unexpected(r.error());
    }
}

// --- Subscriber handle ---

Subscriber::Subscriber(Subscriber&& other) noexcept : session_(other.session_) {
    other.session_ = nullptr;
}

auto Subscriber::operator=(Subscriber&& other) noexcept -> Subscriber& {
    if (this != &other) {
        if (session_ != nullptr) session_->sub_drop(); // undeclare our own first
        session_ = other.session_;
        other.session_ = nullptr;
    }
    return *this;
}

Subscriber::~Subscriber() {
    if (session_ != nullptr) session_->sub_drop(); // best-effort undeclare
}

auto Subscriber::recv() -> std::expected<Sample, ZError> {
    if (session_ == nullptr) return std::unexpected(ZError::connection_closed);
    return session_->sub_recv();
}

auto Subscriber::undeclare() -> void {
    if (session_ != nullptr) {
        session_->sub_drop();
        session_ = nullptr;
    }
}

auto Subscriber::key_expr() const noexcept -> std::string_view {
    if (session_ != nullptr && session_->sub_) return session_->sub_->key;
    return {};
}

// --- Queryable handle ---

Queryable::Queryable(Queryable&& other) noexcept : session_(other.session_) {
    other.session_ = nullptr;
}

auto Queryable::operator=(Queryable&& other) noexcept -> Queryable& {
    if (this != &other) {
        if (session_ != nullptr) session_->qbl_drop(); // undeclare our own first
        session_ = other.session_;
        other.session_ = nullptr;
    }
    return *this;
}

Queryable::~Queryable() {
    if (session_ != nullptr) session_->qbl_drop(); // best-effort undeclare
}

auto Queryable::recv() -> std::expected<IncomingQuery, ZError> {
    if (session_ == nullptr) return std::unexpected(ZError::connection_closed);
    return session_->qbl_recv();
}

auto Queryable::undeclare() -> void {
    if (session_ != nullptr) {
        session_->qbl_drop();
        session_ = nullptr;
    }
}

auto Queryable::key_expr() const noexcept -> std::string_view {
    if (session_ != nullptr && session_->qbl_) return session_->qbl_->key;
    return {};
}

// --- IncomingQuery handle ---

IncomingQuery::IncomingQuery(IncomingQuery&& other) noexcept
    : session_(other.session_), rid_(other.rid_), key_(std::move(other.key_)),
      params_(std::move(other.params_)), payload_(std::move(other.payload_)) {
    other.session_ = nullptr;
}

auto IncomingQuery::operator=(IncomingQuery&& other) noexcept -> IncomingQuery& {
    if (this != &other) {
        if (session_ != nullptr) session_->send_response_final(rid_); // finalize our own first
        session_ = other.session_;
        rid_ = other.rid_;
        key_ = std::move(other.key_);
        params_ = std::move(other.params_);
        payload_ = std::move(other.payload_);
        other.session_ = nullptr;
    }
    return *this;
}

IncomingQuery::~IncomingQuery() {
    if (session_ != nullptr) session_->send_response_final(rid_); // best-effort
}

auto IncomingQuery::reply(std::string_view key_expr, std::span<const std::byte> payload)
    -> std::expected<void, ZError> {
    if (session_ == nullptr) return std::unexpected(ZError::connection_closed);
    return session_->send_response(rid_, key_expr, payload, /*is_err=*/false);
}

auto IncomingQuery::reply_err(std::span<const std::byte> payload) -> std::expected<void, ZError> {
    if (session_ == nullptr) return std::unexpected(ZError::connection_closed);
    return session_->send_response(rid_, std::string_view{}, payload, /*is_err=*/true);
}

// --- Getter handle ---

Getter::Getter(Getter&& other) noexcept : session_(other.session_), rid_(other.rid_) {
    other.session_ = nullptr;
}

auto Getter::operator=(Getter&& other) noexcept -> Getter& {
    if (this != &other) {
        if (session_ != nullptr) session_->get_drop(rid_);
        session_ = other.session_;
        rid_ = other.rid_;
        other.session_ = nullptr;
    }
    return *this;
}

Getter::~Getter() {
    if (session_ != nullptr) session_->get_drop(rid_);
}

auto Getter::recv() -> std::expected<std::optional<GetReply>, ZError> {
    if (session_ == nullptr) return std::unexpected(ZError::connection_closed);
    return session_->get_recv(rid_);
}

} // namespace zenoh

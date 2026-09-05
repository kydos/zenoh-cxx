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

import zenoh.ke;
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

/// The reserved namespace every Computation lives in on the wire, and the whole of
/// the Query/Eval isolation mechanism (session.cppm's Evaluation section): the
/// logical computation key `robot/r1/reset` is declared, queried and matched as
/// `@eval/robot/r1/reset`.
///
/// Prefixing preserves key-expression matching exactly (`a/*/b` matches `a/x/b` iff
/// `@eval/a/*/b` matches `@eval/a/x/b`), so nothing about the user's key expressions
/// changes; what it buys is that an ordinary `get`'s key expression can never reach
/// a Computation and an eval's can never reach a `Queryable`. `@eval` is a *verbatim*
/// chunk (`zenoh.ke`), so that holds even for `get("**")`, and it sits in Zenoh's
/// reserved non-alphabetic-leading key space next to the reference's own `@adv` —
/// deliberately not inside the `@/...` admin space, which the broker refuses to route
/// at all (`zenoh.broker.membership`'s `is_internal_key`).
constexpr std::string_view eval_prefix = "@eval/";
/// The namespace's own chunk, derived from the prefix so the name is spelled exactly
/// once ("centralized in one internal helper/constant"). `constexpr`, so this costs
/// nothing and runs no static initializer.
constexpr std::string_view eval_ns = eval_prefix.substr(0, eval_prefix.size() - 1);

/// Selector parameter asking the router to accept replies whose key expression does
/// not intersect the query's own (the reference's `ReplyKeyExpr::Any`, which it
/// likewise carries as a parameter rather than a wire field). An eval needs it
/// because its replies are keyed by the *logical* computation key while the request
/// travels under `eval_prefix`.
constexpr std::string_view eval_parameters = "_anyke";

/// Whether `key` names the reserved Evaluation namespace, i.e. its first chunk is
/// literally `@eval`. A literal is the only thing that can name it: `@eval` is a
/// verbatim chunk, so no wildcard on either side matches it (`zenoh.ke`, and the
/// reference's `MayHaveVerbatim` rule this mirrors).
///
/// This is what makes the namespace *reserved* rather than merely obscure. The
/// key-expression matching keeps evals and ordinary queries apart for every key
/// expression an application would naturally write, but a caller who types the prefix
/// verbatim would otherwise match a Computation's own wire declaration exactly — so
/// the ordinary query API (`get`, `declare_queryable`) refuses the namespace outright,
/// and the Evaluation API refuses to nest it inside itself. Only the query surface
/// needs this: `put`/`declare_subscriber` route to subscribers, and no Computation is
/// ever one, so there is nothing there to reach.
[[nodiscard]] auto names_eval_namespace(std::string_view key) noexcept -> bool {
    return key.substr(0, key.find('/')) == eval_ns;
}

/// `robot/r1/reset` -> `@eval/robot/r1/reset`. Private, by design: no key this
/// returns is ever handed back to an application.
[[nodiscard]] auto to_eval_wire_key(std::string_view key) -> std::string {
    std::string out;
    out.reserve(eval_prefix.size() + key.size());
    out.append(eval_prefix);
    out.append(key);
    return out;
}

/// The inverse, for the receive path: `@eval/robot/r1/reset` -> `robot/r1/reset`,
/// or `nullopt` when `key` is an ordinary (non-evaluation) key expression.
[[nodiscard]] auto from_eval_wire_key(std::string_view key) noexcept
    -> std::optional<std::string_view> {
    if (!key.starts_with(eval_prefix)) return std::nullopt;
    return key.substr(eval_prefix.size());
}

/// Whether `key` is a concrete, canonical key expression — what a Computation must
/// be declared on. Canonicity is `zenoh.ke`'s business; concreteness is then a plain
/// scan, because in a canonical key expression the only legal use of '*' is a whole
/// `*`/`**` chunk (v1 has no `$*` sub-chunk globbing — see ke.cppm).
[[nodiscard]] auto is_concrete_key(std::string_view key) noexcept -> bool {
    return ke::is_canon(key) && key.find('*') == std::string_view::npos;
}

/// Whether `key_expr` is usable as the key expression of an eval: any canonical key
/// expression that does not name the reserved namespace. Canonicity is required here
/// though `get` does not require it, because prefixing has to preserve the caller's
/// key expression — `""` or `a//b` would otherwise be spliced into a malformed wire
/// key rather than rejected.
[[nodiscard]] auto is_eval_key_expr(std::string_view key_expr) noexcept -> bool {
    return ke::is_canon(key_expr) && !names_eval_namespace(key_expr);
}

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

// Plain data popped from a computation's strand; wrapped into an `Eval` (which needs
// a live `Session*`) only at delivery time, exactly as `PendingQuery` is.
struct PendingEval {
    std::uint32_t rid = 0;
    std::string key;             ///< the evaluator's key expression (logical)
    std::string computation_key; ///< the concrete key of the computation it reached
    std::vector<std::byte> argument;
};

// One declared computation. Mirrors `QblReg`, minus the delivery mode: a computation
// strand is always `ordered` (see `ComputationOptions`).
struct CompReg {
    std::uint32_t id;
    std::string key; ///< the logical, concrete key (no `eval_prefix`)
    Strand<PendingEval> strand;
    EvalHandler handler; ///< empty for a pull-based (recv) computation
    CompReg(std::uint32_t i, std::string k, std::size_t cap, EvalHandler h)
        : id(i), key(std::move(k)), strand(cap, StrandMode::ordered), handler(std::move(h)) {}
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
auto CompRegDeleter::operator()(CompReg* p) const noexcept -> void { delete p; }

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

auto Session::encode_put(std::uint16_t scope, std::string_view suffix,
                         std::span<const std::byte> payload, const PutOptions& opts)
    -> std::expected<void, ZError> {
    Push push{};
    push.wire_expr = WireExpr{.scope = scope, .mapping = Mapping::sender, .suffix = suffix};
    push.qos = to_qos(opts.congestion);
    if (opts.target_zid) push.dest = DestinationId{.zid = to_zenoh_id(*opts.target_zid)};
    Put put{};
    put.payload = payload;
    push.payload = PushBody{.body = std::move(put)};

    FrameHeader fh{};
    fh.reliability = Reliability::reliable;
    fh.sn = frame_sn_;

    std::size_t const cap = 2 + suffix.size() + payload.size() + 64;
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

auto Session::encode_put_head(std::uint16_t scope, std::string_view suffix,
                              std::span<const std::byte> payload, const PutOptions& opts)
    -> std::expected<std::size_t, ZError> {
    Push push{};
    push.wire_expr = WireExpr{.scope = scope, .mapping = Mapping::sender, .suffix = suffix};
    push.qos = to_qos(opts.congestion);
    if (opts.target_zid) push.dest = DestinationId{.zid = to_zenoh_id(*opts.target_zid)};
    Put put{};
    put.payload = payload; // present only so the encoded length prefix is correct
    push.payload = PushBody{.body = std::move(put)};

    FrameHeader fh{};
    fh.reliability = Reliability::reliable;
    fh.sn = frame_sn_;

    // Header only — no room reserved for the payload, which is sent via writev.
    std::size_t const cap = 2 + suffix.size() + 64;
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
    return put_wire(0, key_expr, payload, opts); // no declared id: the key travels in full
}

auto Session::try_put(std::string_view key_expr, std::span<const std::byte> payload,
                      PutOptions opts) -> std::expected<void, ZError> {
    return try_put_wire(0, key_expr, payload, opts);
}

auto Session::put_wire(std::uint16_t scope, std::string_view suffix,
                       std::span<const std::byte> payload, const PutOptions& opts)
    -> std::expected<void, ZError> {
    // A closed session says so, rather than letting the write fail on a dead
    // descriptor and surfacing as a generic io_error -- which is what
    // declare_subscriber/declare_queryable/get already do.
    if (!link_.valid()) return std::unexpected(ZError::connection_closed);
    // Drain any bytes a prior try_put left buffered (blocking).
    if (pending_off_ < tx_pending_.size()) {
        if (auto r = link_.write_all(std::span(tx_pending_).subspan(pending_off_)); !r)
            return std::unexpected(io_to_zerr(r.error()));
    }
    tx_pending_.clear();
    pending_off_ = 0;

    // Scatter-gather: encode just the header into tx_scratch_ and write it together
    // with the borrowed payload, so the payload is never copied into a staging buffer.
    auto const head = encode_put_head(scope, suffix, payload, opts);
    if (!head) return std::unexpected(head.error());

    if (auto r = link_.writev_all(std::span(tx_scratch_).first(*head), payload); !r)
        return std::unexpected(io_to_zerr(r.error()));
    frame_sn_ = (frame_sn_ + 1) & sn_mask;
    return {};
}

auto Session::try_put_wire(std::uint16_t scope, std::string_view suffix,
                           std::span<const std::byte> payload, const PutOptions& opts)
    -> std::expected<void, ZError> {
    if (!link_.valid()) return std::unexpected(ZError::connection_closed); // see put()
    // Don't interleave a new frame ahead of buffered bytes: flush first.
    if (pending_off_ < tx_pending_.size()) {
        if (auto f = flush_pending(); !f) return std::unexpected(f.error());
    }

    if (auto e = encode_put(scope, suffix, payload, opts); !e) return std::unexpected(e.error());
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

// --- Publisher declaration / teardown ---

auto Session::write_declare_keyexpr(std::uint16_t id, std::string_view key)
    -> std::expected<void, ZError> {
    Declare d{};
    DeclareKeyExpr dk{};
    dk.id = id;
    // The declaration itself always spells the key expression out: it is what binds
    // the id, so it cannot be expressed in terms of one.
    dk.wire_expr = WireExpr{.scope = 0, .mapping = Mapping::sender, .suffix = key};
    d.body = DeclareBody{.body = dk};

    std::vector<std::byte> tmp(key.size() + 64);
    ByteWriter w{tmp};
    if (!d.encode(w)) return std::unexpected(ZError::encode_error);
    return write_frame(std::span(tmp).first(w.written()));
}

auto Session::write_undeclare_keyexpr(std::uint16_t id) -> void {
    if (!link_.valid()) return;
    Declare d{};
    d.body = DeclareBody{.body = UndeclareKeyExpr{.id = id}};

    std::vector<std::byte> tmp(64);
    ByteWriter w{tmp};
    if (!d.encode(w)) return;
    (void)write_frame(std::span(tmp).first(w.written())); // best-effort
}

auto Session::write_interest(std::uint32_t id, std::uint16_t scope, std::string_view suffix)
    -> std::expected<void, ZError> {
    Interest in{};
    in.id = id;
    // What a `zenoh-rust` publisher announces on declaration: "tell me, now and as it
    // changes, about the key expressions and subscribers matching this" -- the query
    // behind its matching_status()/matching_listener(). Bit 0 is KEYEXPRS, bit 1 is
    // SUBSCRIBERS (`InterestInner`'s K and S).
    in.mode = InterestMode::current_future;
    in.inner.options = 0x01 | 0x02;
    in.inner.wire_expr = WireExpr{.scope = scope, .mapping = Mapping::sender, .suffix = suffix};

    std::vector<std::byte> tmp(suffix.size() + 64);
    ByteWriter w{tmp};
    if (!in.encode(w)) return std::unexpected(ZError::encode_error);
    return write_frame(std::span(tmp).first(w.written()));
}

auto Session::write_interest_final(std::uint32_t id) -> void {
    if (!link_.valid()) return;
    InterestFinal fin{};
    fin.id = id;

    std::vector<std::byte> tmp(64);
    ByteWriter w{tmp};
    if (!fin.encode(w)) return;
    (void)write_frame(std::span(tmp).first(w.written())); // best-effort
}

auto Session::declare_ke(std::string_view key) -> std::uint16_t {
    if (auto it = ke_by_key_.find(std::string(key)); it != ke_by_key_.end()) {
        ++it->second.refs; // already declared on this link: share the id
        return it->second.id;
    }
    // Stay under what a router will actually remember: both this project's broker and
    // a `Session`'s own receive path cap their id->key maps at `max_resmap_entries`
    // and silently ignore declarations past it. An id the peer dropped would make
    // every later Push unresolvable, so declining to allocate (and publishing the key
    // in full) is the safe answer rather than the lossy one.
    if (ke_by_id_.size() >= max_resmap_entries) return 0;

    // Ids are u16 and are released on undeclare, so a long-lived session that churns
    // publishers wraps: walk from the cursor to the first free id, skipping 0 (which
    // means "no id" on the wire). The map size check above guarantees one exists.
    std::uint16_t id = next_ke_id_;
    while (id == 0 || ke_by_id_.contains(id)) ++id;
    next_ke_id_ = static_cast<std::uint16_t>(id + 1);

    if (!write_declare_keyexpr(id, key)) return 0; // publish the key in full instead
    ke_by_key_.emplace(std::string(key), KeReg{.id = id, .refs = 1});
    ke_by_id_.emplace(id, std::string(key));
    return id;
}

auto Session::undeclare_ke(std::uint16_t ke_id) -> void {
    if (ke_id == 0) return;
    auto by_id = ke_by_id_.find(ke_id);
    if (by_id == ke_by_id_.end()) return;
    auto by_key = ke_by_key_.find(by_id->second);
    if (by_key == ke_by_key_.end()) return;
    if (--by_key->second.refs > 0) return; // another publisher still holds it
    write_undeclare_keyexpr(ke_id);
    ke_by_key_.erase(by_key);
    ke_by_id_.erase(by_id);
}

auto Session::declare_publisher(std::string_view key_expr, PublisherOptions opts)
    -> std::expected<Publisher, ZError> {
    if (!link_.valid()) return std::unexpected(ZError::connection_closed);
    std::uint32_t const id = next_entity_id_++;
    std::uint16_t const ke_id = declare_ke(key_expr);
    // With an id bound, the publisher's key expression is that id and nothing else;
    // without one it is the text, exactly as a bare `put` sends it.
    std::string_view const suffix = ke_id == 0 ? key_expr : std::string_view{};
    if (auto r = write_interest(id, ke_id, suffix); !r) {
        undeclare_ke(ke_id);
        return std::unexpected(r.error());
    }
    return Publisher{this, id, ke_id, std::string(key_expr),
                     PutOptions{.target_zid = opts.target_zid, .congestion = opts.congestion}};
}

auto Session::del_wire(std::uint16_t scope, std::string_view suffix, const PutOptions& opts)
    -> std::expected<void, ZError> {
    if (!link_.valid()) return std::unexpected(ZError::connection_closed);
    Push push{};
    push.wire_expr = WireExpr{.scope = scope, .mapping = Mapping::sender, .suffix = suffix};
    push.qos = to_qos(opts.congestion);
    if (opts.target_zid) push.dest = DestinationId{.zid = to_zenoh_id(*opts.target_zid)};
    push.payload = PushBody{.body = Del{}};

    std::vector<std::byte> tmp(suffix.size() + 64);
    ByteWriter w{tmp};
    if (!push.encode(w)) return std::unexpected(ZError::encode_error);
    return write_frame(std::span(tmp).first(w.written()));
}

auto Session::pub_drop(std::uint32_t id, std::uint16_t ke_id) -> void {
    write_interest_final(id); // mirrors the reference's Interest{Final} on undeclare
    undeclare_ke(ke_id);
}

// --- Publisher ---

Publisher::Publisher(Publisher&& other) noexcept
    : session_(other.session_), id_(other.id_), ke_id_(other.ke_id_), key_(std::move(other.key_)),
      opts_(std::move(other.opts_)) {
    other.session_ = nullptr;
}

auto Publisher::operator=(Publisher&& other) noexcept -> Publisher& {
    if (this != &other) {
        if (session_ != nullptr) session_->pub_drop(id_, ke_id_); // undeclare our own first
        session_ = other.session_;
        id_ = other.id_;
        ke_id_ = other.ke_id_;
        key_ = std::move(other.key_);
        opts_ = std::move(other.opts_);
        other.session_ = nullptr;
    }
    return *this;
}

Publisher::~Publisher() {
    if (session_ != nullptr) session_->pub_drop(id_, ke_id_); // best-effort undeclare
}

auto Publisher::undeclare() -> void {
    if (session_ != nullptr) {
        session_->pub_drop(id_, ke_id_);
        session_ = nullptr; // idempotent: the destructor must not undeclare twice
    }
}

auto Publisher::put(std::span<const std::byte> payload) -> std::expected<void, ZError> {
    if (session_ == nullptr) return std::unexpected(ZError::connection_closed);
    return session_->put_wire(ke_id_, wire_suffix(), payload, opts_);
}

auto Publisher::try_put(std::span<const std::byte> payload) -> std::expected<void, ZError> {
    if (session_ == nullptr) return std::unexpected(ZError::connection_closed);
    return session_->try_put_wire(ke_id_, wire_suffix(), payload, opts_);
}

auto Publisher::del() -> std::expected<void, ZError> {
    if (session_ == nullptr) return std::unexpected(ZError::connection_closed);
    return session_->del_wire(ke_id_, wire_suffix(), opts_);
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
    // The other half of the reservation (see `names_eval_namespace`): an ordinary
    // queryable must not be able to register itself where evals are routed, or an eval
    // would reach a `Queryable` -- the isolation guarantee inverted.
    if (names_eval_namespace(key_expr)) return std::unexpected(ZError::invalid_key_expr);
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

auto Session::write_request(std::uint32_t rid, std::uint16_t scope, std::string_view suffix,
                            std::string_view parameters, std::span<const std::byte> payload,
                            const GetOptions& opts) -> std::expected<void, ZError> {
    Request req{};
    req.id = rid;
    req.wire_expr = WireExpr{.scope = scope, .mapping = Mapping::sender, .suffix = suffix};
    req.target = to_query_target(opts.target);
    req.qos = to_qos(opts.congestion);
    req.timeout = Duration::from_millis(effective_timeout_ms(opts));
    if (opts.target_zid) req.dest = DestinationId{.zid = to_zenoh_id(*opts.target_zid)};

    Query q{};
    q.consolidation = to_consolidation_mode(opts.consolidation);
    q.parameters = parameters;
    // The request payload (the eval argument) rides the `Query`'s value extension —
    // the same field the reference's `get().payload(..)` uses. Left unset for a plain
    // `get`, which has no payload in this runtime's API.
    if (!payload.empty()) q.body = Value{.encoding = Encoding{}, .payload = payload};
    req.payload = RequestBody{.query = q};

    std::vector<std::byte> tmp(64 + suffix.size() + parameters.size() + payload.size());
    ByteWriter w{tmp};
    if (!req.encode(w)) return std::unexpected(ZError::encode_error);
    return write_frame(std::span(tmp).first(w.written()));
}

auto Session::start_get(std::uint16_t scope, std::string_view suffix, std::string_view parameters,
                        std::span<const std::byte> payload, GetReplyHandler handler,
                        const GetOptions& opts) -> std::expected<std::uint32_t, ZError> {
    if (!link_.valid()) return std::unexpected(ZError::connection_closed);
    std::uint32_t const rid = next_request_id_++;
    if (auto r = write_request(rid, scope, suffix, parameters, payload, opts); !r)
        return std::unexpected(r.error());
    auto const deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(effective_timeout_ms(opts));
    pending_gets_[rid] = std::unique_ptr<GetReg, GetRegDeleter>(
        new GetReg(256, StrandMode::ordered, std::move(handler), deadline));
    return rid;
}

auto Session::get(std::string_view key_expr, std::string_view parameters, GetOptions opts)
    -> std::expected<Getter, ZError> {
    if (names_eval_namespace(key_expr)) return std::unexpected(ZError::invalid_key_expr);
    auto rid = start_get(0, key_expr, parameters, {}, GetReplyHandler{}, opts);
    if (!rid) return std::unexpected(rid.error());
    return Getter{this, *rid};
}

auto Session::get(std::string_view key_expr, std::string_view parameters, GetReplyHandler on_reply,
                  GetOptions opts) -> std::expected<void, ZError> {
    if (names_eval_namespace(key_expr)) return std::unexpected(ZError::invalid_key_expr);
    auto rid = start_get(0, key_expr, parameters, {}, std::move(on_reply), opts);
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
        // Clamped, not just narrowed: `remaining_ms` is an int64, and a timeout beyond
        // ~24.8 days (well inside the uint32 milliseconds GetOptions accepts) wrapped
        // negative here. std::min then kept the negative, and poll() reads a negative
        // timeout as "wait forever" -- so the longest timeouts became infinite ones,
        // with no keepalive going out either, and the router dropped the session on
        // lease expiry. Nothing above the keepalive cadence is useful anyway.
        auto const wait =
            static_cast<std::int32_t>(std::clamp<std::int64_t>(remaining_ms, 0, keepalive_ms_));
        if (auto p = pump_step(wait); !p) return std::unexpected(p.error());
    }
}

auto Session::get_drop(std::uint32_t rid) -> void { pending_gets_.erase(rid); }

// --- Evaluation: Computation declaration / Evaluator / eval() ---

auto Session::declare_computation(std::string_view key, ComputationOptions opts)
    -> std::expected<Computation, ZError> {
    return declare_computation(key, EvalHandler{}, opts);
}

auto Session::declare_computation(std::string_view key, EvalHandler on_eval,
                                  ComputationOptions opts) -> std::expected<Computation, ZError> {
    // A Computation is a computation registered at *one* key: a wild (or merely
    // non-canonical) key expression is rejected rather than quietly registered on
    // something that can never be one concrete computation.
    // ... and the reserved namespace is refused so it cannot be nested inside itself
    // (see `names_eval_namespace`).
    if (!is_concrete_key(key) || names_eval_namespace(key))
        return std::unexpected(ZError::invalid_key_expr);
    if (!link_.valid()) return std::unexpected(ZError::connection_closed);
    if (auto r = declare_comp_key(key); !r) return std::unexpected(r.error());
    std::uint32_t const id = next_entity_id_++;
    comps_.push_back(std::unique_ptr<CompReg, CompRegDeleter>(
        new CompReg(id, std::string(key), opts.capacity, std::move(on_eval))));
    return Computation{this, id};
}

auto Session::declare_comp_key(std::string_view key) -> std::expected<void, ZError> {
    // One wire declaration per *key*, refcounted, not one per registration -- the same
    // shape `declare_ke` uses for publishers, and for a sharper reason here. Two
    // Computations may share a key (both must run), and an `UndeclareQueryable` names a
    // key expression, not a registration: a router that keys declarations by key alone
    // -- `zenohb` does, via `ResourceTable`'s per-(key, face) flag -- would let the
    // first undeclare silently stop routing to the survivors. Declaring once and
    // releasing on the last drop makes the guarantee independent of that choice, and
    // puts N-1 fewer declarations on the wire.
    auto it = comp_decls_.find(key);
    if (it != comp_decls_.end()) {
        ++it->second.refs;
        return {};
    }
    std::uint32_t const decl_id = next_entity_id_++;
    // Declared to the router as an ordinary queryable on the internally-namespaced
    // key, which is what keeps ordinary queries and evals from reaching each other.
    // Always incomplete: `complete` is a data-query notion (see ComputationOptions).
    QueryableInfo const qinfo{.complete = false, .distance = 0};
    if (auto r = write_declare_queryable(decl_id, to_eval_wire_key(key), qinfo); !r)
        return std::unexpected(r.error());
    comp_decls_.emplace(std::string(key), CompDecl{.id = decl_id, .refs = 1});
    return {};
}

auto Session::undeclare_comp_key(std::string_view key) -> void {
    auto it = comp_decls_.find(key);
    if (it == comp_decls_.end()) return;
    if (--it->second.refs != 0) return; // another Computation still holds this key
    write_undeclare_queryable(it->second.id);
    comp_decls_.erase(it);
}

auto Session::find_comp(std::uint32_t id) noexcept -> CompReg* {
    for (auto const& c : comps_) {
        if (c->id == id) return c.get();
    }
    return nullptr;
}

auto Session::comp_drop(std::uint32_t id) -> void {
    auto it =
        std::find_if(comps_.begin(), comps_.end(), [id](auto const& c) { return c->id == id; });
    if (it == comps_.end()) return;
    undeclare_comp_key((*it)->key); // a wire message only when the last one on that key goes
    // Evals still queued for this computation will never be delivered, so release
    // their share of their request now -- otherwise the evaluator waits out its whole
    // timeout for a `ResponseFinal` that nobody is left to trigger.
    while (auto pe = (*it)->strand.pop()) eval_finish(pe->rid);
    comps_.erase(it);
}

auto Session::deliver_eval(std::uint32_t rid, std::string_view key,
                           std::span<const std::byte> argument) -> bool {
    // Which computations does this evaluation reach? The router matched our
    // declarations *as a face* (one Request per session, however many of its
    // declarations matched), so the per-registration fan-out is ours to do -- and it
    // is a match, not a lookup: the evaluator's key expression may be wild, and two
    // computations may share one key, in which case both run.
    std::size_t matched = 0;
    for (auto const& c : comps_) {
        if (!ke::intersects(key, c->key)) continue;
        // Room is checked for *every* matching computation before anything is posted:
        // a half-delivered request that the caller replays would run the computations
        // that had room a second time, and a computation may not be replay-safe.
        if (c->strand.size() >= c->strand.capacity()) return false;
        ++matched;
    }
    if (matched == 0) {
        // Nothing matched after all (e.g. the computation was undeclared while this
        // request was in flight): terminate it rather than leave the evaluator
        // waiting on a reply stream that will never close.
        send_response_final(rid);
        return true;
    }
    eval_pending_[rid] += matched;
    for (auto const& c : comps_) {
        if (!ke::intersects(key, c->key)) continue;
        PendingEval pe{.rid = rid,
                       .key = std::string(key),
                       .computation_key = c->key,
                       .argument = {argument.begin(), argument.end()}};
        // Room was pre-checked above, so this always `posted` -- never `full` (which
        // would lose an eval already counted in `eval_pending_`) and never `conflated`
        // (a computation strand is `ordered`, so it does not conflate at all).
        (void)c->strand.post(c->key, std::move(pe));
    }
    return true;
}

auto Session::eval_finish(std::uint32_t rid) -> void {
    auto it = eval_pending_.find(rid);
    if (it == eval_pending_.end()) return;
    if (--it->second != 0) return; // sibling computations are still working
    eval_pending_.erase(it);
    send_response_final(rid);
}

auto Session::comp_recv(std::uint32_t id) -> std::expected<Eval, ZError> {
    for (;;) {
        auto* comp = find_comp(id);
        if (comp == nullptr) return std::unexpected(ZError::connection_closed);
        if (auto pe = comp->strand.pop())
            return Eval(this, pe->rid, std::move(pe->key), std::move(pe->computation_key),
                        std::move(pe->argument));
        // As in `qbl_recv`: a pump that ends in an error may still have posted
        // messages from the same batch first, so deliver those before reporting it.
        if (auto r = pump_step(); !r) {
            auto* still = find_comp(id);
            if (still == nullptr || still->strand.empty()) return std::unexpected(r.error());
        }
    }
}

auto Session::declare_evaluator(std::string_view key_expr, EvalOptions opts)
    -> std::expected<Evaluator, ZError> {
    if (!is_eval_key_expr(key_expr)) return std::unexpected(ZError::invalid_key_expr);
    if (!link_.valid()) return std::unexpected(ZError::connection_closed);
    std::string wire_key = to_eval_wire_key(key_expr);
    // Same declared-keyexpr-id mechanism a `Publisher` uses, for the same reason: an
    // evaluator sends its key expression on every eval, so binding it to an id once
    // turns a long key expression into a couple of bytes per request. Id 0 (none
    // available) is a valid outcome -- the text is then sent in full.
    std::uint16_t const ke_id = declare_ke(wire_key);
    return Evaluator{this, ke_id, std::string(key_expr), std::move(wire_key), std::move(opts)};
}

auto Session::start_eval(std::uint16_t scope, std::string_view suffix,
                         std::span<const std::byte> argument, GetReplyHandler handler,
                         const EvalOptions& opts) -> std::expected<std::uint32_t, ZError> {
    // The Evaluation contract, applied where the underlying query is built rather
    // than exposed as options (session.cppm's `EvalOptions`): every matching
    // Computation registration runs (`all`), nothing is consolidated (`none`), and
    // replies keyed by the logical computation key are accepted even though the
    // request travels under `eval_prefix` (`_anyke`).
    GetOptions gopts{};
    gopts.consolidation = GetConsolidation::none;
    gopts.target = GetTarget::all;
    gopts.timeout_ms = opts.timeout_ms;
    gopts.target_zid = opts.target_zid;
    gopts.congestion = opts.congestion;
    return start_get(scope, suffix, eval_parameters, argument, std::move(handler), gopts);
}

auto Session::eval(std::string_view key_expr, std::span<const std::byte> argument, EvalOptions opts)
    -> std::expected<Getter, ZError> {
    if (!is_eval_key_expr(key_expr)) return std::unexpected(ZError::invalid_key_expr);
    auto rid = start_eval(0, to_eval_wire_key(key_expr), argument, GetReplyHandler{}, opts);
    if (!rid) return std::unexpected(rid.error());
    return Getter{this, *rid};
}

auto Session::eval(std::string_view key_expr, std::span<const std::byte> argument,
                   GetReplyHandler on_reply, EvalOptions opts) -> std::expected<void, ZError> {
    if (!is_eval_key_expr(key_expr)) return std::unexpected(ZError::invalid_key_expr);
    auto rid = start_eval(0, to_eval_wire_key(key_expr), argument, std::move(on_reply), opts);
    if (!rid) return std::unexpected(rid.error());
    return {};
}

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

auto Session::dispatch_cursor() -> std::expected<bool, ZError> {
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
                    return false;
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
            if (qbl_ || !comps_.empty()) {
                auto key = resolve_key(req->wire_expr);
                if (!key) {
                    fault_ = key.error();
                    return std::unexpected(*fault_);
                }
                // Bound to a reference before dereferencing rather than read through a
                // conditional expression: the latter is what `bugprone-unchecked-
                // optional-access` cannot see through (it reads the `->payload` as
                // unguarded), and this is the shape the rest of this file already uses.
                std::span<const std::byte> arg{};
                if (auto const& body = req->payload.query.body) arg = body->payload;
                // An evaluation and an ordinary query are told apart by the internal
                // namespace alone, and each is routed only to its own abstraction:
                // this is the client half of the Query/Eval isolation the `@eval`
                // prefix buys (see `eval_prefix`). The router already enforces the
                // other half by matching key expressions, verbatim chunk included.
                if (auto logical = from_eval_wire_key(*key)) {
                    if (!deliver_eval(req->id, *logical, arg)) {
                        return false; // pause; retry next pump
                    }
                } else if (qbl_) {
                    PendingQuery pq{.rid = req->id,
                                    .key = *key,
                                    .params = std::string(req->payload.query.parameters),
                                    .payload = {arg.begin(), arg.end()}};
                    if (qbl_->strand.post(*key, std::move(pq)) == PostResult::full) {
                        return false; // pause; retry next pump
                    }
                } else {
                    send_response_final(req->id); // computations only, and none matched
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
                if (post_result == PostResult::full) return false; // pause; retry next pump
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
    return true;
}

auto Session::recv_batch_step() -> std::expected<std::optional<std::size_t>, ZError> {
    // The 2-byte little-endian length prefix, possibly across several reads.
    while (rx_hdr_fill_ < rx_hdr_.size()) {
        auto n = link_.read_some(std::span(rx_hdr_).subspan(rx_hdr_fill_));
        if (!n) {
            if (n.error() == IoError::would_block) return std::nullopt;
            return std::unexpected(io_to_zerr(n.error()));
        }
        rx_hdr_fill_ += *n;
    }
    if (rx_need_ == 0 && rx_fill_ == 0) {
        rx_need_ = load_le<std::uint16_t>(rx_hdr_.data());
        if (rx_buf_.size() < rx_need_) rx_buf_.resize(rx_need_);
    }

    while (rx_fill_ < rx_need_) {
        auto n = link_.read_some(std::span(rx_buf_).subspan(rx_fill_, rx_need_ - rx_fill_));
        if (!n) {
            if (n.error() == IoError::would_block) return std::nullopt;
            return std::unexpected(io_to_zerr(n.error()));
        }
        rx_fill_ += *n;
    }

    auto const len = rx_need_;
    rx_hdr_fill_ = 0; // ready for the next batch
    rx_need_ = 0;
    rx_fill_ = 0;
    return len;
}

// Deliver to callback-style registrations. Cheap and always safe to call: a handler
// may undeclare its own registration, so re-test the owner each iteration and invoke
// through a copy (see run_once).
auto Session::drain_handlers() -> void {
    while (sub_ && sub_->handler) {
        auto s = sub_->strand.pop();
        if (!s) break;
        auto handler = sub_->handler;
        handler(*s);
    }
    while (qbl_ && qbl_->handler) {
        auto pq = qbl_->strand.pop();
        if (!pq) break;
        auto handler = qbl_->handler;
        handler(IncomingQuery(this, pq->rid, std::move(pq->key), std::move(pq->params),
                              std::move(pq->payload)));
    }
    // Computations are drained in two phases, unlike the single registrations above:
    // a handler may undeclare *any* computation (its own included), which would
    // invalidate an iteration over `comps_` mid-flight. Phase one runs no user code
    // and only moves queued evals out; phase two invokes the handlers, skipping any
    // computation undeclared in the meantime -- so an undeclare still stops delivery,
    // exactly as re-testing `qbl_` does above.
    std::vector<std::pair<std::uint32_t, PendingEval>> due;
    for (auto const& c : comps_) {
        if (!c->handler) continue;
        while (auto pe = c->strand.pop()) due.emplace_back(c->id, std::move(*pe));
    }
    for (auto& [id, pe] : due) {
        auto* comp = find_comp(id);
        if (comp == nullptr) { // undeclared by an earlier handler in this batch
            eval_finish(pe.rid);
            continue;
        }
        auto handler = comp->handler;
        handler(Eval(this, pe.rid, std::move(pe.key), std::move(pe.computation_key),
                     std::move(pe.argument)));
    }
    // Callback-style get()s and evals, over a snapshot of the request ids rather than
    // over the map itself. A reply handler that starts the *next* request -- "on each
    // reply, eval the next step", which the Evaluation API invites -- inserts into
    // `pending_gets_` and can rehash it, which would invalidate a live iterator here.
    // Re-finding by id each time round is what makes that safe; the entry may also be
    // gone (a handler dropping its own `Getter`), hence the lookup rather than a
    // cached pointer.
    for (std::uint32_t const rid : callback_get_rids()) {
        for (;;) {
            auto it = pending_gets_.find(rid);
            if (it == pending_gets_.end()) break;
            auto r = it->second->strand.pop();
            if (!r) break;
            auto handler = it->second->handler; // copy: the handler may erase the entry
            handler(*r);
        }
    }
}

auto Session::callback_get_rids() -> std::vector<std::uint32_t> {
    std::vector<std::uint32_t> rids;
    if (pending_gets_.empty()) return rids; // the common case: no allocation at all
    rids.reserve(pending_gets_.size());
    for (auto const& [rid, reg] : pending_gets_) {
        if (reg->handler) rids.push_back(rid);
    }
    return rids;
}

auto Session::pump_step(std::optional<std::int32_t> max_wait_ms) -> std::expected<void, ZError> {
    if (fault_) return std::unexpected(*fault_); // sticky terminal fault — never resync
    if (rx_pos_ < rx_end_) {                     // resume an in-progress batch
        auto progressed = dispatch_cursor();
        if (!progressed) return std::unexpected(progressed.error());
        if (*progressed) return {};
        // Stalled on a full strand. Anything with a callback can be drained right
        // here, whoever is pumping -- otherwise a callback subscriber that nobody is
        // calling run_once() for head-of-line blocks the shared cursor, and a get()
        // pumping for its reply spins re-decoding the same undeliverable sample.
        drain_handlers();
        auto retried = dispatch_cursor();
        if (!retried) return std::unexpected(retried.error());
        if (*retried) return {};
        // Still stalled: the blocked strand is pull-style and its owner is not the
        // caller pumping here (e.g. get_recv() while an undrained subscriber queue is
        // full). Nothing this call can do, but returning immediately would spin, so
        // pace it against the socket instead of burning the CPU. The application
        // draining that Subscriber is what actually unblocks the cursor.
        (void)link_.poll_readable(std::min<std::int32_t>(max_wait_ms.value_or(1), 1));
        return {};
    }

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

    auto step = recv_batch_step();
    if (!step) {
        fault_ = step.error();
        return std::unexpected(*fault_);
    }
    auto const& arrived = *step;
    // Still arriving: the progress is held on the session, so returning here is safe
    // and lets the caller re-check its own deadline (and us send a keepalive on the
    // next idle wait) instead of blocking inside a half-read batch.
    if (!arrived.has_value()) return {};
    std::size_t const len = *arrived;
    if (len == 0) return {};

    // A batch is a *sequence* of transport messages, not one message: the reference
    // packs a KeepAlive (or a Close, or a second Frame) into whatever batch is
    // currently staging -- see zenoh-rust's `TransmissionPipeline::push_transport_
    // message`. Handing the whole batch to one cursor loop is what keeps
    // `[Frame][Push][KeepAlive]` from looking like a desync and `[KeepAlive][Frame]`
    // from silently discarding the frame behind it.
    rx_pos_ = 0;
    rx_end_ = len;
    if (auto progressed = dispatch_cursor(); !progressed) {
        return std::unexpected(progressed.error());
    }
    return {};
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
    // The pump's error is reported *after* delivering, not instead of it: a pump that
    // ends in EOF (or any other terminal fault) may have posted messages from the same
    // batch first, and dropping them on the floor loses data the peer did send. The
    // fault is sticky, so it is still there on the next call.
    auto pumped = pump_step();
    // A handler may undeclare (or destroy) its own Subscriber/Queryable -- "stop after
    // N samples" is the obvious case -- which frees the registration the loop is
    // standing on; `drain_handlers` re-tests the owner each iteration and invokes
    // through a copy of the std::function for exactly that reason.
    drain_handlers();
    // Retire finished callback-style get()s and evals. Pull-style (handler-empty)
    // entries are owned entirely by their Getter (recv()'s own pump loop and
    // ~Getter()'s cleanup), so they're deliberately left untouched here to avoid two
    // owners racing to erase the same map entry. `drain_handlers` above already
    // delivered everything queued; this pass only decides what to erase, over a
    // snapshot of the ids for the same reentrancy reason it uses.
    for (std::uint32_t const rid : callback_get_rids()) {
        auto it = pending_gets_.find(rid);
        if (it == pending_gets_.end()) continue; // erased by a handler
        auto& reg = *it->second;
        if ((reg.final && reg.strand.empty()) || std::chrono::steady_clock::now() >= reg.deadline)
            pending_gets_.erase(it);
    }
    if (!pumped) return std::unexpected(pumped.error());
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

// --- Computation handle ---

Computation::Computation(Computation&& other) noexcept : session_(other.session_), id_(other.id_) {
    other.session_ = nullptr;
}

auto Computation::operator=(Computation&& other) noexcept -> Computation& {
    if (this != &other) {
        if (session_ != nullptr) session_->comp_drop(id_); // undeclare our own first
        session_ = other.session_;
        id_ = other.id_;
        other.session_ = nullptr;
    }
    return *this;
}

Computation::~Computation() {
    if (session_ != nullptr) session_->comp_drop(id_); // best-effort undeclare
}

auto Computation::recv() -> std::expected<Eval, ZError> {
    if (session_ == nullptr) return std::unexpected(ZError::connection_closed);
    return session_->comp_recv(id_);
}

auto Computation::undeclare() -> void {
    if (session_ != nullptr) {
        session_->comp_drop(id_);
        session_ = nullptr;
    }
}

auto Computation::key() const noexcept -> std::string_view {
    if (session_ == nullptr) return {};
    auto const* comp = session_->find_comp(id_);
    return comp == nullptr ? std::string_view{} : std::string_view{comp->key};
}

// --- Eval handle ---

Eval::Eval(Eval&& other) noexcept
    : session_(other.session_), rid_(other.rid_), key_(std::move(other.key_)),
      computation_key_(std::move(other.computation_key_)), argument_(std::move(other.argument_)) {
    other.session_ = nullptr;
}

auto Eval::operator=(Eval&& other) noexcept -> Eval& {
    if (this != &other) {
        if (session_ != nullptr) session_->eval_finish(rid_); // release our own first
        session_ = other.session_;
        rid_ = other.rid_;
        key_ = std::move(other.key_);
        computation_key_ = std::move(other.computation_key_);
        argument_ = std::move(other.argument_);
        other.session_ = nullptr;
    }
    return *this;
}

Eval::~Eval() {
    // Not `send_response_final` directly (as `~IncomingQuery` does): one request may
    // have reached several of this session's computations, and it is finalized only
    // once the last of them is done -- see `Session::eval_finish`.
    if (session_ != nullptr) session_->eval_finish(rid_);
}

auto Eval::reply(std::span<const std::byte> value) -> std::expected<void, ZError> {
    if (session_ == nullptr) return std::unexpected(ZError::connection_closed);
    // The reply carries the *logical* computation key, never the internal wire one:
    // that is what identifies which computation produced this result, and it is
    // why the request asked for `_anyke` (the two do not intersect).
    return session_->send_response(rid_, computation_key_, value, /*is_err=*/false);
}

auto Eval::reply_err(std::span<const std::byte> error) -> std::expected<void, ZError> {
    if (session_ == nullptr) return std::unexpected(ZError::connection_closed);
    return session_->send_response(rid_, std::string_view{}, error, /*is_err=*/true);
}

// --- Evaluator handle ---

Evaluator::Evaluator(Evaluator&& other) noexcept
    : session_(other.session_), ke_id_(other.ke_id_), key_(std::move(other.key_)),
      wire_key_(std::move(other.wire_key_)), opts_(std::move(other.opts_)) {
    other.session_ = nullptr;
}

auto Evaluator::operator=(Evaluator&& other) noexcept -> Evaluator& {
    if (this != &other) {
        if (session_ != nullptr) session_->undeclare_ke(ke_id_); // release our own first
        session_ = other.session_;
        ke_id_ = other.ke_id_;
        key_ = std::move(other.key_);
        wire_key_ = std::move(other.wire_key_);
        opts_ = std::move(other.opts_);
        other.session_ = nullptr;
    }
    return *this;
}

Evaluator::~Evaluator() {
    if (session_ != nullptr) session_->undeclare_ke(ke_id_); // best-effort
}

auto Evaluator::eval(std::span<const std::byte> argument) -> std::expected<Getter, ZError> {
    if (session_ == nullptr) return std::unexpected(ZError::connection_closed);
    auto rid = session_->start_eval(ke_id_, wire_suffix(), argument, GetReplyHandler{}, opts_);
    if (!rid) return std::unexpected(rid.error());
    return Getter{session_, *rid};
}

auto Evaluator::eval(std::span<const std::byte> argument, GetReplyHandler on_reply)
    -> std::expected<void, ZError> {
    if (session_ == nullptr) return std::unexpected(ZError::connection_closed);
    auto rid = session_->start_eval(ke_id_, wire_suffix(), argument, std::move(on_reply), opts_);
    if (!rid) return std::unexpected(rid.error());
    return {};
}

auto Evaluator::undeclare() -> void {
    if (session_ != nullptr) {
        session_->undeclare_ke(ke_id_);
        session_ = nullptr;
    }
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

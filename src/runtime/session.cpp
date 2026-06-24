module;

#include <array>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <random>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

module zenoh.session;

import zenoh.proto;
import zenoh.runtime.tcp;

// Implementation unit for zenoh.session: endpoint parsing, the 4-way transport
// handshake, and the put / try_put publish paths.
namespace zenoh {
namespace {

/// Default client lease (matches the reference): 10 s.
constexpr std::uint64_t default_lease_ms = 10'000;
/// SN resolution mask for the default U32 frame-SN resolution (u32::MAX >> 4).
constexpr std::uint32_t sn_mask = 0x0fff'ffff;
/// Max bytes a single TCP batch can carry (the 2-byte length prefix is a u16).
constexpr std::size_t max_batch = 0xffff;
/// Conservative per-batch fixed overhead: the 2-byte length prefix + a reliable,
/// default-QoS FrameHeader (1 header byte + up to a 4-byte SN varint). Used to decide
/// when an API batch is full before the real header is encoded.
constexpr std::size_t frame_overhead = 8;

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
    return s;
}

auto Session::encode_put(std::string_view key_expr, std::span<const std::byte> payload)
    -> std::expected<void, ZError> {
    Push push{};
    push.wire_expr = WireExpr{.scope = 0, .mapping = Mapping::sender, .suffix = key_expr};
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

auto Session::encode_put_head(std::string_view key_expr, std::span<const std::byte> payload)
    -> std::expected<std::size_t, ZError> {
    Push push{};
    push.wire_expr = WireExpr{.scope = 0, .mapping = Mapping::sender, .suffix = key_expr};
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

auto Session::put(std::string_view key_expr, std::span<const std::byte> payload)
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
    auto const head = encode_put_head(key_expr, payload);
    if (!head) return std::unexpected(head.error());

    if (auto r = link_.writev_all(std::span(tx_scratch_).first(*head), payload); !r)
        return std::unexpected(io_to_zerr(r.error()));
    frame_sn_ = (frame_sn_ + 1) & sn_mask;
    return {};
}

auto Session::try_put(std::string_view key_expr, std::span<const std::byte> payload)
    -> std::expected<void, ZError> {
    // Don't interleave a new frame ahead of buffered bytes: flush first.
    if (pending_off_ < tx_pending_.size()) {
        if (auto f = flush_pending(); !f) return std::unexpected(f.error());
    }

    if (auto e = encode_put(key_expr, payload); !e) return std::unexpected(e.error());
    std::size_t const framed = static_cast<std::size_t>(load_le<std::uint16_t>(tx_scratch_.data())) + 2;
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
    if (auto framed = frame_message(buf, c); framed)
        (void)link_.write_all(std::span(buf).first(*framed));

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

auto Batch::put(std::string_view key_expr, std::span<const std::byte> payload)
    -> std::expected<void, ZError> {
    if (session_ == nullptr) return std::unexpected(ZError::connection_closed);

    // Encode the Push(Put) in place at the end of the current body.
    std::size_t const msg_max = 64 + key_expr.size() + payload.size();
    if (buf_.size() < body_len_ + msg_max) buf_.resize(body_len_ + msg_max);

    Push push{};
    push.wire_expr = WireExpr{.scope = 0, .mapping = Mapping::sender, .suffix = key_expr};
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

} // namespace zenoh

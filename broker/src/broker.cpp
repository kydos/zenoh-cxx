module;

#include <asio/as_tuple.hpp>
#include <asio/awaitable.hpp>
#include <asio/bind_allocator.hpp>
#include <asio/bind_executor.hpp>
#include <asio/buffer.hpp>
#include <asio/co_spawn.hpp>
#include <asio/connect.hpp>
#include <asio/detached.hpp>
#include <asio/io_context.hpp>
#include <asio/ip/tcp.hpp>
#include <asio/post.hpp>
#include <asio/recycling_allocator.hpp>
#include <asio/steady_timer.hpp>
#include <asio/strand.hpp>
#include <asio/this_coro.hpp>
#include <asio/use_awaitable.hpp>
#include <asio/write.hpp>

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <expected>
#include <memory>
#include <optional>
#include <random>
#include <set>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

module zenoh.broker;

import zenoh.proto;
import zenoh.broker.tables;

// Implementation unit for zenoh.broker: bind/listen setup, the thread-pool run loop,
// the accept loop, and (in the anonymous namespace below) the per-connection `Face`
// and its listener-side handshake + read loop. All of this lives in ONE translation
// unit, and none of it is declared in broker.cppm: `asio::awaitable<T>` never
// appears in any `.cppm` interface here. This isn't just style -- a `.cppm`
// declaration (even a private, non-exported one) using `asio::awaitable<T>` was
// found to poison this module's BMI for *any* importer that also textually includes
// a handful of ordinary standard headers (`<string>` alone is enough), triggering a
// clang/libc++ "cannot add 'abi_tag' attribute in a redeclaration" error -- the same
// class of named-modules/libc++ fragility this project's docs already flag
// elsewhere, just via a different trigger (confirmed empirically: identical headers
// compile cleanly outside of a module). Keeping every coroutine-returning
// declaration entirely inside this one `.cpp` file (as local lambdas / anonymous-
// namespace functions, never named in the interface unit) is the workaround.
namespace zenoh::broker {

namespace {

constexpr std::uint64_t default_lease_ms = 10'000;
// Congestion watermarks, in *bytes* of outbound data queued for a Face (hysteresis:
// cross `high` to start dropping new deliveries to this face, drain back below `low`
// to resume). Replaces an earlier "close the face on overflow" policy: that turned
// any transient production/drain mismatch -- not just a genuinely stuck peer --
// into an outright disconnect (confirmed: a single fast publisher could trip it
// within ~1s of connecting). Dropping keeps the slow consumer connected and lets
// it catch back up, at the cost of reliable delivery to it while congested (v1
// policy -- see docs/BROKER.md). Bytes rather than a message count so the bound is
// on actual memory (and so on how far behind -- i.e. how stale -- a slow consumer's
// queue is allowed to get) regardless of payload size.
constexpr std::size_t congested_high_watermark = 1U << 20; // 1 MiB queued
constexpr std::size_t congested_low_watermark = 1U << 18;  // 256 KiB queued
// A clique link is not one subscriber: it aggregates every client behind the broker
// on the other end, so the budget tuned for a single slow consumer is far too tight
// for it -- crossing it would start discarding traffic for a whole broker's worth of
// subscribers over a transient blip.
constexpr std::size_t router_congested_high_watermark = 16U << 20; // 16 MiB queued
constexpr std::size_t router_congested_low_watermark = 4U << 20;   // 4 MiB queued
// The hard ceilings, far above the watermarks. `CongestionControl::Block` traffic is
// queued *past* the high watermark by design, and read-throttling the producers is
// what normally keeps that bounded; these bounds are what happens when that fails --
// a peer that has stopped draining entirely, e.g. a dead TCP connection the kernel
// has not timed out yet. Reaching one closes the face, turning unbounded memory
// growth into a detectable failure.
//
// Note the deliberate contrast with the close-on-overflow policy this broker used to
// have (docs/BROKER.md's history section): that fired at 1024 queued *frames*, so
// any transient production/drain mismatch became a disconnect. These sit two orders
// of magnitude higher and are only reachable after backpressure has already failed.
constexpr std::size_t hard_ceiling_bytes = 64U << 20;         // 64 MiB, client face
constexpr std::size_t router_hard_ceiling_bytes = 256U << 20; // 256 MiB, clique link
// Initial size of a Face's reusable receive buffer. Sized to hold many small
// batches per `read_some` syscall (the point of buffering at all) without
// committing 64 KiB per connection up front; `next_batch` grows it on demand for a
// peer that actually sends batches larger than this, up to the 64 KiB the 2-byte
// length prefix can express.
constexpr std::size_t rx_buffer_initial = std::size_t{16} * 1024;
// Bound on how many bytes of re-encoded Pushes one Face accumulates into a single
// shared block before posting it to the routing strand (see Face::flush_push_batch).
// Caps both the memory a single inbound frame can make the broker hold and how long
// a routed batch can get before the first of its messages starts moving.
constexpr std::size_t max_push_block_bytes = std::size_t{256} * 1024;
constexpr std::size_t max_key_len = 0xffff;      ///< mirrors Session's resolve_key bound
constexpr std::size_t max_resmap_entries = 4096; ///< mirrors Session's resmap_ bound
constexpr std::size_t max_decl_ids = 4096;       ///< cap on sub_ids_/qbl_ids_, same rationale
/// Cap on simultaneously-dialled peer endpoints.
///
/// Membership allows 1024 members x 8 endpoints, and every endpoint learned by gossip
/// spawns a connector that retries forever -- so an untrusted (or merely confused)
/// gossip source could otherwise leave this broker permanently dialling thousands of
/// attacker-chosen host:port pairs, and `Membership` has no removal by design. A real
/// clique dials one endpoint per peer; this leaves two orders of magnitude of room.
constexpr std::size_t max_dialing = 256;
// Bound on Tables::pending_routing_jobs() before a Face pauses its own reads --
// see Face::throttle_if_backlogged for the rationale (defensive hardening against
// asio::post's own lack of a queue bound, not a fix for a confirmed leak: a single
// fast publisher was measured to never actually backlog the routing strand).
constexpr std::size_t max_pending_routing_jobs = 4096;
// Backoff bounds for re-dialling a configured peer broker. A peer that isn't up yet
// is an ordinary startup condition in a clique (whichever broker starts first has
// nobody to talk to), not an error, so the connector retries forever -- but it backs
// off to `max` so a permanently-absent peer costs one connect attempt every 30s
// rather than a spin. The `min` delay also applies after a *successful* link drops,
// which keeps a peer that accepts and immediately closes from becoming a tight loop.
constexpr std::uint32_t peer_reconnect_min_ms = 250;
constexpr std::uint32_t peer_reconnect_max_ms = 30'000;
// How often a connector parked by a duplicate-link collapse re-checks whether the
// link that displaced it is still up. Cheap (one timer per suppressed peer) and only
// ever reached once a working link to that peer exists.
constexpr std::uint32_t peer_suppressed_poll_ms = 500;
// How often the broker re-checks whether every known peer is reachable. Only a
// changed answer is reported, so this is a polling interval, not a log rate.
constexpr std::uint32_t partition_report_interval_ms = 5'000;

// Which side of the transport handshake a Face performs. The listener side waits for
// an InitSyn and answers InitAck/OpenAck (every client connection, and an inbound
// peer-broker link); the dialer side drives InitSyn/OpenSyn itself, mirroring the
// client `Session::open` sequence in src/runtime/session.cpp -- the broker only ever
// dials another broker, never a client.
enum class FaceRole : std::uint8_t { listener, dialer };

[[nodiscard]] auto next_face_id() noexcept -> FaceId {
    static std::atomic<FaceId> next{1};
    return next.fetch_add(1, std::memory_order_relaxed);
}

// One accepted connection. Two-tier strand model (see the broker plan): everything
// in this class -- the socket, rx cursor, resmap_, tx queue, per-face frame SN -- is
// touched only on `strand_` (Tier 1). Cross-face delivery/routing goes through
// `Tables` (Tier 2, its own routing_strand_) via the `FaceHandle::deliver` callback
// this class builds, never by any other object reaching into a `Face` directly.
class Face : public std::enable_shared_from_this<Face> {
  public:
    Face(asio::ip::tcp::socket sock, asio::strand<asio::any_io_executor> strand, FaceId id,
         Tables* tables, FaceRole role, bool accept_router_faces = false,
         std::shared_ptr<std::atomic<bool>> dial_suppress = nullptr)
        : sock_(std::move(sock)), strand_(std::move(strand)), id_(id), tables_(tables), role_(role),
          accept_router_faces_(accept_router_faces), dial_suppress_(std::move(dial_suppress)) {
        // A dialled link is a peer broker by construction; an accepted one is a
        // client until its InitSyn says otherwise (see handshake_listener).
        if (role_ == FaceRole::dialer) kind_ = FaceKind::router;
    }

    [[nodiscard]] auto run() -> asio::awaitable<void> {
        if (!co_await handshake()) co_return;

        // Register before the read loop can post anything -- asio::strand preserves
        // FIFO order of *posted* handlers, so this is guaranteed to be processed by
        // Tables before any routing work this face's own reads produce below.
        post_to_tables([handle = build_handle()](Tables& tables) mutable {
            tables.add_face(std::move(handle));
        });

        // A clique link is the one connection whose silent death actually matters:
        // it carries every client behind the peer, and the partition detection below
        // depends on noticing it. Client faces keep the previous behaviour (liveness
        // purely by TCP error/EOF), so nothing about them changes.
        if (kind_ == FaceKind::router) {
            asio::co_spawn(
                strand_,
                [self = shared_from_this()]() -> asio::awaitable<void> {
                    co_await self->keepalive_loop();
                },
                asio::detached);
        }

        co_await read_loop();

        // Drop out of the congested-links count before disappearing: leaving it
        // incremented would read-throttle every client face for the rest of the
        // broker's life. `torn_down_` makes that release final -- see `set_pressure`.
        torn_down_ = true;
        set_pressure(FacePressure::ok);
        post_to_tables([id = id_](Tables& tables) { tables.remove_face(id); });
    }

    /// Frame `msgs` (unframed, already-encoded network messages, in order) into this
    /// face's outbound byte stream and (re)start the async-write chain if idle. Must
    /// run on `strand_` -- the only caller is the closure `build_handle()` posts
    /// through. Taking a whole run at a time is what lets consecutive messages share
    /// one frame and one `write` syscall (see `append_msg`/`pump_tx`).
    auto enqueue_and_pump(const SharedBuf& block, const std::vector<MsgSlice>& slices) -> void {
        assert(strand_.running_in_this_thread());
        if (torn_down_) return; // a delivery that lost the race with teardown
        auto const bytes = block.bytes();
        for (auto const& slice : slices) {
            if (slice.offset + slice.length <= bytes.size()) {
                append_msg(bytes.subspan(slice.offset, slice.length), slice.block);
            }
        }
        pump_tx();
    }

  private:
    /// Bytes queued for this face but not yet written to the socket.
    [[nodiscard]] auto queued_bytes() const noexcept -> std::size_t {
        return tx_len_ + tx_inflight_len_;
    }

    [[nodiscard]] auto high_watermark() const noexcept -> std::size_t {
        return kind_ == FaceKind::router ? router_congested_high_watermark
                                         : congested_high_watermark;
    }
    [[nodiscard]] auto low_watermark() const noexcept -> std::size_t {
        return kind_ == FaceKind::router ? router_congested_low_watermark : congested_low_watermark;
    }
    [[nodiscard]] auto hard_ceiling() const noexcept -> std::size_t {
        return kind_ == FaceKind::router ? router_hard_ceiling_bytes : hard_ceiling_bytes;
    }

    /// Publish this face's backpressure level, keeping `Tables`'s count of congested
    /// clique links in step with it. Only ever called from this face's own strand,
    /// so the exchange has a single writer; the atomic is for the readers (the
    /// routing strand, and every client face's read loop).
    auto set_pressure(FacePressure p) -> void {
        assert(strand_.running_in_this_thread());
        // Once the face is finished, only the release to `ok` is honoured. A delivery
        // posted from the routing strand *before* it processed `remove_face` can still
        // run on this strand after `run()` has returned (that is exactly why `deliver`
        // captures `shared_from_this()`), and re-raising the level there would leave
        // `congested_router_faces_` incremented with no coroutine left alive to ever
        // decrement it -- read-throttling every client face for the broker's lifetime.
        if (torn_down_ && p != FacePressure::ok) return;
        auto const previous = pressure_->exchange(p, std::memory_order_relaxed);
        if (previous == p || kind_ != FaceKind::router) return;
        bool const was_behind = previous != FacePressure::ok;
        bool const now_behind = p != FacePressure::ok;
        if (!was_behind && now_behind) {
            tables_->congested_router_faces().fetch_add(1, std::memory_order_relaxed);
        } else if (was_behind && !now_behind) {
            tables_->congested_router_faces().fetch_sub(1, std::memory_order_relaxed);
        }
    }

    /// Make room for `need` bytes in the accumulation buffer, doubling so growth is
    /// amortized. `tx_accum_` is used as raw storage -- `tx_len_`, not the vector's
    /// own size, says how much of it is live -- so that appending is a plain memcpy
    /// with no per-append bookkeeping and no re-zeroing of storage the encoder is
    /// about to overwrite (`std::vector::insert` at the end showed up as ~10% of
    /// broker CPU when this path used it).
    auto ensure_tx_capacity(std::size_t need) -> void {
        if (tx_accum_.size() >= need) return;
        std::size_t cap = std::max<std::size_t>(tx_accum_.size() * 2, 4096);
        while (cap < need) cap *= 2;
        tx_accum_.resize(cap);
    }

    /// Append one encoded network message to the outbound byte stream, extending the
    /// currently-open batch/frame if it still fits and starting a new one otherwise.
    ///
    /// Batching (rather than one framed batch per message, as this used to do) is
    /// what makes the write path cheap in the only two places it costs anything: the
    /// wire (one 2-byte length prefix + one `FrameHeader` amortized over every
    /// message that fits alongside it, which the peer then demuxes in a single
    /// decode loop) and the syscall count (`pump_tx` hands the accumulated bytes to
    /// exactly one `write`, however many messages that is -- a scatter-gather write
    /// over per-message buffers instead caps out at ASIO's 64-iovec limit).
    auto append_msg(std::span<const std::byte> body, bool block) -> void {
        std::size_t const queued = queued_bytes();
        if (queued >= hard_ceiling()) {
            // Backpressure has already failed to help: this peer is not draining at
            // all. Stop accumulating and close, rather than let one stuck link
            // consume memory without bound (see hard_ceiling_bytes).
            set_pressure(FacePressure::saturated);
            close_now();
            return;
        }
        if (queued >= high_watermark()) {
            // Past the watermark, droppable traffic is discarded for this face --
            // it stays connected and catches up, and neither the producer nor any
            // faster consumer is stalled. Traffic marked Block is queued anyway;
            // what bounds *that* is the read-throttling this pressure level turns
            // on for the faces feeding this one (see Tables::congested_router_faces)
            // and, failing that, the hard ceiling above.
            set_pressure(FacePressure::congested);
            if (!block) return;
        }
        // Budget for one batch's content, i.e. everything after its 2-byte length
        // prefix -- the peer's advertised batch size covers the prefix too (the same
        // accounting the client's own `Session` tx path uses).
        std::size_t const cap = peer_batch_size_ > 2 ? std::size_t{peer_batch_size_} - 2 : 0;

        if (open_batch_ != no_open_batch) {
            std::size_t const content = tx_len_ - open_batch_ - 2;
            if (content + body.size() <= cap) { // fits alongside what's already framed
                ensure_tx_capacity(tx_len_ + body.size());
                __builtin_memcpy(tx_accum_.data() + tx_len_, body.data(), body.size());
                tx_len_ += body.size();
                store_le<std::uint16_t>(tx_accum_.data() + open_batch_,
                                        static_cast<std::uint16_t>(content + body.size()));
                if (queued_bytes() >= high_watermark()) set_pressure(FacePressure::congested);
                return;
            }
            open_batch_ = no_open_batch; // full: this message starts a fresh batch
        }

        std::array<std::byte, 16> hdr{};
        ByteWriter w{hdr};
        FrameHeader fh{};
        fh.reliability = Reliability::reliable;
        fh.sn = frame_sn_;
        if (!fh.encode(w)) return;
        std::size_t const hdr_len = w.written();
        if (hdr_len + body.size() > cap) return; // can never fit a batch; drop silently

        ensure_tx_capacity(tx_len_ + 2 + hdr_len + body.size());
        std::byte* const at = tx_accum_.data() + tx_len_;
        store_le<std::uint16_t>(at, static_cast<std::uint16_t>(hdr_len + body.size()));
        __builtin_memcpy(at + 2, hdr.data(), hdr_len);
        __builtin_memcpy(at + 2 + hdr_len, body.data(), body.size());
        open_batch_ = tx_len_;
        tx_len_ += 2 + hdr_len + body.size();
        frame_sn_ = (frame_sn_ + 1) & 0x0fff'ffff;
        if (queued_bytes() >= high_watermark()) set_pressure(FacePressure::congested);
    }

    // Posts `fn` (any callable taking `Tables&`) onto `tables_->strand()`, tracking
    // it in `Tables::pending_routing_jobs()` so `throttle_if_backlogged` can detect
    // and bound Tier-2 backlog. Every message-triggered post to `tables_->strand()`
    // must go through this, not a bare `asio::post`, or the backlog gauge undercounts
    // and the throttle below never engages. Binds `asio::recycling_allocator` --
    // every post here has the same shape (same captured-lambda size class)
    // repeated at message rate, exactly the pattern that allocator is for: a
    // per-thread free list instead of a fresh `::operator new`/`delete` per post
    // (ASIO's own strand/coroutine-frame machinery already uses the same
    // allocator internally for this reason).
    template <class Fn> auto post_to_tables(Fn fn) -> void {
        tables_->pending_routing_jobs().fetch_add(1, std::memory_order_relaxed);
        asio::post(tables_->strand(),
                   asio::bind_allocator(asio::recycling_allocator<void>{},
                                        [tables = tables_, fn = std::move(fn)]() mutable {
                                            tables->pending_routing_jobs().fetch_sub(
                                                1, std::memory_order_relaxed);
                                            fn(*tables);
                                        }));
    }

    auto build_handle() -> FaceHandle {
        return FaceHandle{
            .id = id_,
            .zid = zid_,
            .kind = kind_,
            .dialed = role_ == FaceRole::dialer,
            .close =
                [self = shared_from_this()] {
                    // Always via this face's own strand: `Tables` calls this from the
                    // routing strand, and the socket is Tier-1 state.
                    asio::post(self->strand_, [self] { self->close_now(); });
                },
            .dial_suppress = dial_suppress_,
            .deliver =
                [self = shared_from_this()](SharedBuf block, std::vector<MsgSlice> slices) {
                    asio::post(self->strand_,
                               asio::bind_allocator(asio::recycling_allocator<void>{},
                                                    [self, block = std::move(block),
                                                     slices = std::move(slices)]() mutable {
                                                        self->enqueue_and_pump(block, slices);
                                                    }));
                },
            .pressure = pressure_};
    }

    // --- handshake ---

    [[nodiscard]] auto handshake() -> asio::awaitable<bool> {
        if (role_ == FaceRole::dialer) co_return co_await handshake_dialer();
        co_return co_await handshake_listener();
    }

    // Dialer side: the mirror of `Session::open`'s client handshake
    // (src/runtime/session.cpp), differing only in announcing `WhatAmI::router`, so
    // the broker on the other end classifies this face as a peer rather than a
    // client. Used exclusively for clique links.
    [[nodiscard]] auto handshake_dialer() -> asio::awaitable<bool> {
        assert(strand_.running_in_this_thread());
        sock_.set_option(asio::ip::tcp::no_delay(true));

        InitSyn isyn{};
        isyn.version = 9;
        isyn.identifier.whatami = WhatAmI::router;
        isyn.identifier.zid = tables_->router_zid();
        isyn.resolution.resolution = 0x0a;
        isyn.resolution.batch_size = 0xffff;
        if (!co_await send_now(encode_one(isyn))) co_return false;

        auto ack_bytes = co_await next_batch();
        if (!ack_bytes) co_return false;
        ByteReader ack_r{*ack_bytes};
        auto ack = InitAck::decode(ack_r);
        if (!ack || ack->version != 9) co_return false;
        // Refuse to treat a non-router answer as a clique link: dialling something
        // that turns out to be a client (or a foreign peer) and then routing to it
        // under split-horizon rules would silently mis-route. Better to drop the
        // link and let the connector retry.
        if (ack->identifier.whatami != WhatAmI::router) co_return false;
        zid_ = ack->identifier.zid;
        peer_batch_size_ = ack->resolution.batch_size;

        // `ack->cookie` borrows `rx_buf_` (see next_batch's contract). `encode_one`
        // copies it into the outbound buffer here, before any further read can
        // invalidate that borrow -- the same ordering `Session::open` relies on.
        OpenSyn osyn{};
        osyn.lease = Duration::from_millis(default_lease_ms);
        osyn.sn = 0;
        osyn.cookie = ack->cookie;
        if (!co_await send_now(encode_one(osyn))) co_return false;

        auto oack_bytes = co_await next_batch();
        if (!oack_bytes) co_return false;
        ByteReader oack_r{*oack_bytes};
        if (!OpenAck::decode(oack_r)) co_return false;
        co_return true;
    }

    // Listener side (mirror of Session::open's client side).
    [[nodiscard]] auto handshake_listener() -> asio::awaitable<bool> {
        assert(strand_.running_in_this_thread());
        sock_.set_option(asio::ip::tcp::no_delay(true));

        auto isyn_bytes = co_await next_batch();
        if (!isyn_bytes) co_return false;
        ByteReader isyn_r{*isyn_bytes};
        auto isyn = InitSyn::decode(isyn_r);
        if (!isyn || isyn->version != 9) co_return false;
        zid_ = isyn->identifier.zid;
        peer_batch_size_ = isyn->resolution.batch_size;
        // Only an explicit `router` makes this a clique link. `peer` is deliberately
        // treated as a client: a real zenoh-rust peer-mode session expects
        // scouting/link-state machinery this broker does not implement (see
        // docs/BROKER.md's interop note), so it must never be handed router-face
        // semantics.
        // `whatami` is the peer's own unauthenticated claim, so an inbound face is a
        // clique link only where the operator has said inbound peers are expected
        // (BrokerConfig::accept_router_faces). Without that, a client could announce
        // `router` and be handed gossip ingestion, a replay of this broker's
        // declarations, split-horizon treatment and the router congestion budgets.
        // Outbound links are unaffected: this broker chose to dial those.
        if (isyn->identifier.whatami == WhatAmI::router && !accept_router_faces_) {
            // Loud, because the failure mode otherwise is a clique that silently does
            // not federate: the peer looks connected but is routed to as a client.
            std::fprintf(stderr, "zenohb: inbound peer announced whatami=router; treating it as a "
                                 "client (pass --accept-router-faces to allow clique links in)\n");
        }
        kind_ = (isyn->identifier.whatami == WhatAmI::router && accept_router_faces_)
                    ? FaceKind::router
                    : FaceKind::client;

        std::random_device rd;
        std::array<std::byte, 8> cookie{};
        for (auto& b : cookie) b = static_cast<std::byte>(rd() & 0xff);

        InitAck ack{};
        ack.version = 9;
        ack.identifier.whatami = WhatAmI::router;
        ack.identifier.zid = tables_->router_zid();
        ack.resolution.resolution = 0x0a;
        ack.resolution.batch_size = 0xffff;
        ack.cookie = cookie;
        if (!co_await send_now(encode_one(ack))) co_return false;

        auto osyn_bytes = co_await next_batch();
        if (!osyn_bytes) co_return false;
        ByteReader osyn_r{*osyn_bytes};
        auto osyn = OpenSyn::decode(osyn_r);
        if (!osyn) co_return false;

        OpenAck oack{};
        oack.lease = Duration::from_millis(default_lease_ms);
        oack.sn = 0;
        if (!co_await send_now(encode_one(oack))) co_return false;

        co_return true;
    }

    // Sends a KeepAlive on an otherwise idle clique link, and drops the link if the
    // peer has gone quiet for a whole lease. Without this, a peer whose host
    // disappears (as opposed to closing cleanly) is only noticed whenever TCP
    // eventually gives up -- minutes, typically -- during which this broker keeps
    // routing into a black hole and reports no partition at all.
    //
    // Runs concurrently with the read loop on the same strand, so it never races the
    // socket or tx state it touches.
    [[nodiscard]] auto keepalive_loop() -> asio::awaitable<void> {
        auto token = asio::as_tuple(asio::use_awaitable);
        auto const period = std::chrono::milliseconds(default_lease_ms / 4);
        auto const lease = std::chrono::milliseconds(default_lease_ms);
        for (;;) {
            asio::steady_timer timer{strand_, period};
            co_await timer.async_wait(token);
            if (!sock_.is_open()) co_return;

            auto const now = std::chrono::steady_clock::now();
            if (now - last_rx_ > lease) {
                close_now(); // lease expired: the read loop unwinds and cleans up
                co_return;
            }
            // Only when this side is otherwise idle -- ordinary traffic is proof of
            // liveness on its own, and a busy link should not pay for extra frames.
            if (now - last_tx_ >= period) {
                append_keepalive();
                pump_tx();
            }
        }
    }

    /// Append a bare `KeepAlive` as its own batch. Unlike a network message it is a
    /// *transport* message, so it carries no `FrameHeader` and cannot share a frame
    /// with anything else -- which is why it closes whatever batch was open.
    auto append_keepalive() -> void {
        assert(strand_.running_in_this_thread());
        std::array<std::byte, 8> tmp{};
        ByteWriter w{tmp};
        if (!KeepAlive{}.encode(w)) return;
        std::size_t const n = w.written();
        ensure_tx_capacity(tx_len_ + 2 + n);
        std::byte* const at = tx_accum_.data() + tx_len_;
        store_le<std::uint16_t>(at, static_cast<std::uint16_t>(n));
        __builtin_memcpy(at + 2, tmp.data(), n);
        tx_len_ += 2 + n;
        open_batch_ = no_open_batch;
    }

    // --- read loop ---

    [[nodiscard]] auto read_loop() -> asio::awaitable<void> {
        for (;;) {
            co_await throttle_if_backlogged();
            auto batch = co_await next_batch();
            if (!batch) co_return;
            if (!process_batch(*batch)) co_return;
        }
    }

    // Pauses this Face's own reads while Tier 2 (the single global routing strand)
    // is backlogged beyond `max_pending_routing_jobs`. `asio::post` itself has no
    // queue bound, so nothing otherwise stops a fast Face from posting faster than
    // the single routing strand can drain -- this is defensive hardening against
    // that (documented) two-tier-design gap, not a fix for a confirmed leak: a
    // single publisher was measured (via `Tables::pending_routing_jobs()`, and via
    // release-build RSS staying flat across 3M+ messages) to never actually
    // backlog the routing strand at all, since matching/routing one face's traffic
    // is cheap relative to socket I/O. This exists for the many-simultaneous-fast-
    // publishers case the broker plan already flags as an unproven, "sharded
    // Tables if profiling ever shows otherwise" risk -- pausing reads here lets
    // ordinary TCP flow control push backpressure back to such a peer instead of
    // an unbounded queue growing, if that scenario is ever actually hit.
    [[nodiscard]] auto throttle_if_backlogged() -> asio::awaitable<void> {
        while (tables_->pending_routing_jobs().load(std::memory_order_relaxed) >
                   max_pending_routing_jobs ||
               (kind_ == FaceKind::client &&
                tables_->congested_router_faces().load(std::memory_order_relaxed) != 0)) {
            asio::steady_timer timer{co_await asio::this_coro::executor,
                                     std::chrono::milliseconds(1)};
            co_await timer.async_wait(asio::as_tuple(asio::use_awaitable));
        }
    }

    // Next length-prefixed TCP batch (2-byte LE length + body); nullopt on EOF/error.
    //
    // Reads into one reusable per-face buffer via `read_some` and hands back a
    // *borrow* into it, rather than doing two `async_read`s (length, then body) into
    // a freshly-allocated vector per batch: one `read` syscall typically yields
    // several batches from a pipelining peer, and none of them allocates. The
    // returned span is valid only until the next `next_batch()` call (which may
    // compact or grow the buffer) -- every caller fully consumes a batch before
    // asking for the next, and nothing decoded out of it escapes as a view (a Push
    // is re-encoded into its own owned buffer immediately, see `on_push`).
    [[nodiscard]] auto next_batch() -> asio::awaitable<std::optional<std::span<const std::byte>>> {
        auto token = asio::as_tuple(asio::use_awaitable);
        for (;;) {
            std::size_t const avail = rx_end_ - rx_start_;
            if (avail >= 2) {
                std::uint16_t const len = load_le<std::uint16_t>(rx_buf_.data() + rx_start_);
                std::size_t const need = 2 + std::size_t{len};
                if (avail >= need) { // a whole batch is already buffered
                    std::span<const std::byte> const batch{rx_buf_.data() + rx_start_ + 2, len};
                    rx_start_ += need;
                    co_return batch;
                }
                compact_rx(); // partial: make room (and capacity) for the rest of it
                if (rx_buf_.size() < need) rx_buf_.resize(need);
            } else {
                compact_rx();
            }
            auto [ec, n] = co_await sock_.async_read_some(
                asio::buffer(rx_buf_.data() + rx_end_, rx_buf_.size() - rx_end_), token);
            if (ec || n == 0) co_return std::nullopt;
            last_rx_ = std::chrono::steady_clock::now();
            rx_end_ += n;
        }
    }

    // Slide the unconsumed tail back to the front of the receive buffer so the space
    // already-consumed batches occupied becomes writable again.
    auto compact_rx() -> void {
        if (rx_start_ == 0) return;
        std::size_t const avail = rx_end_ - rx_start_;
        if (avail != 0) __builtin_memmove(rx_buf_.data(), rx_buf_.data() + rx_start_, avail);
        rx_start_ = 0;
        rx_end_ = avail;
    }

    [[nodiscard]] auto send_now(std::vector<std::byte> framed) -> asio::awaitable<bool> {
        auto token = asio::as_tuple(asio::use_awaitable);
        auto [ec, n] = co_await asio::async_write(sock_, asio::buffer(framed), token);
        co_return !ec && n == framed.size();
    }

    // Wraps one already-encoded transport message with the 2-byte length prefix
    // (handshake messages are their own batch, no FrameHeader -- mirrors
    // Session::open's frame_message helper).
    template <class Msg>
    [[nodiscard]] static auto encode_one(const Msg& m) -> std::vector<std::byte> {
        std::vector<std::byte> buf(4096);
        ByteWriter w{std::span(buf).subspan(2)};
        if (!m.encode(w)) return {};
        std::size_t const len = w.written();
        store_le<std::uint16_t>(buf.data(), static_cast<std::uint16_t>(len));
        buf.resize(len + 2);
        return buf;
    }

    // --- data-phase batch/frame decode + dispatch ---

    // Returns false to stop the read loop (EOF-equivalent fault); true to keep going.
    //
    // A batch holds a *sequence* of transport messages, not one: the reference packs a
    // KeepAlive (or Close, or a second Frame) into whichever batch is currently
    // staging -- zenoh-rust's `TransmissionPipeline::push_transport_message` appends
    // to the batch already carrying a frame. Walking the whole batch is what keeps
    // `[Frame][Push][KeepAlive]` from being read as a desync (which used to drop the
    // connection) and `[KeepAlive][Frame][Declare]` from silently discarding the
    // declaration behind the keepalive.
    [[nodiscard]] auto process_batch(std::span<const std::byte> batch) -> bool {
        ByteReader r{batch};
        while (r.remaining() > 0) {
            auto pk = r.peek();
            if (!pk) return false;
            std::uint8_t const mid = std::to_integer<std::uint8_t>(*pk) & mid_mask;

            if (mid == FrameHeader::id) {
                if (!FrameHeader::decode(r)) return false;
                if (!dispatch_frame_body(r, batch)) return false;
            } else if (mid == KeepAlive::id) {
                if (!KeepAlive::decode(r)) return false;
            } else if (mid == Close::id) {
                return false; // peer closed the session
            } else {
                // Unknown *transport* message: no length we can trust, so the rest of
                // this batch is unreadable. Tolerated for forward-compat -- the byte
                // stream stays in sync because batches are length-prefixed.
                return true;
            }
        }
        return true;
    }

    // `batch` is the whole enclosing batch `r` reads from, passed through so a Push
    // can be located byte-exactly within it (see `on_push`'s forward-as-received
    // path).
    [[nodiscard]] auto dispatch_frame_body(ByteReader& r, std::span<const std::byte> batch)
        -> bool {
        // Every Push decoded from this one frame accumulates in `push_batch_`
        // (re-encoded back to back into `push_block_`) instead of being posted to
        // Tables individually -- amortizes the Face->Tables asio::post hop (a
        // heap-allocated handler node plus a cross-strand wakeup) over the whole run
        // of consecutive Pushes instead of paying it per message; this was measured
        // as a meaningful share of the per-message routing cost at high throughput
        // (see docs/BROKER.md's "Performance testing" section). Flushed before any
        // *other* message type in the same frame is handled, so relative ordering
        // against interleaved Declare/Request/Response traffic within one frame is
        // preserved exactly -- only consecutive Pushes are ever batched together.
        auto flush_pushes = [this] { flush_push_batch(); };

        while (r.remaining() > 0) {
            auto pk = r.peek();
            if (!pk) {
                flush_pushes(); // don't lose already-decoded Pushes on a fatal desync
                return false;
            }
            std::uint8_t const mid = std::to_integer<std::uint8_t>(*pk) & mid_mask;
            // The frame's body ends at the first id that is not a network message:
            // that byte is the next *transport* message in the batch. Leave `r` on it
            // and let `process_batch` resume there -- this mirrors zenoh-rust's
            // `Frame::read`, which rewinds its reader when a network-message decode
            // fails, and is exact because the two id spaces are disjoint by design.
            if (!is_network_mid(mid)) break;

            if (mid == Push::id) {
                if (!on_push(r, batch)) {
                    flush_pushes(); // ditto: earlier Pushes in this run are still valid
                    return false;
                }
            } else if (mid == Declare::mid) {
                flush_pushes();
                if (!on_declare(r)) return false;
            } else if (mid == Request::mid) {
                flush_pushes();
                if (!on_request(r)) return false;
            } else if (mid == Response::id) {
                flush_pushes();
                if (!on_response(r)) return false;
            } else if (mid == ResponseFinal::id) {
                flush_pushes();
                if (!on_response_final(r)) return false;
            } else if (mid == Interest::mid) {
                if (!on_interest(r)) {
                    flush_pushes(); // decode-only itself, but earlier Pushes still need it
                    return false;
                }
            } else {
                // A network message this broker does not route (OAM): no length to
                // skip, so the cursor cannot get past it. Fault the face rather than
                // guess, exactly as before this loop understood transport messages.
                flush_pushes();
                return false;
            }
        }
        flush_pushes();
        return true;
    }

    // Resolve `we` into `out` via this face's resmap_ (mirrors Session::resolve_key
    // exactly -- same shape, same bounds). Writing into a caller-owned string lets
    // the hot path reuse one scratch buffer's capacity instead of allocating a fresh
    // key per message.
    [[nodiscard]] auto resolve_key_into(const WireExpr& we, std::string& out) -> bool {
        if (we.scope == 0) {
            if (we.suffix.size() > max_key_len) return false;
            out.assign(we.suffix);
            return true;
        }
        auto it = resmap_.find(we.scope);
        if (it == resmap_.end()) return false;
        if (it->second.size() + we.suffix.size() > max_key_len) return false;
        out.assign(it->second);
        out.append(we.suffix);
        return true;
    }

    // Same, returning an owned string -- for the non-hot paths (declare/query).
    [[nodiscard]] auto resolve_key(const WireExpr& we) -> std::optional<std::string> {
        std::string out;
        if (!resolve_key_into(we, out)) return std::nullopt;
        return out;
    }

    /// Handle a Push on a reserved (`@/`) key arriving from a peer broker. Only
    /// gossip exists today; an unrecognized internal key is ignored rather than
    /// faulting the link, so a newer peer can add one without breaking this one.
    auto on_internal_push(const Push& push) -> void {
        assert(strand_.running_in_this_thread());
        if (key_scratch_ != gossip_key) return;
        auto const* put = std::get_if<Put>(&push.payload.body);
        if (put == nullptr) return;
        // The decoded payload borrows the receive buffer, which the very next read
        // reuses -- so it must be copied before it crosses to the routing strand
        // (the same rule every other cross-tier message here obeys).
        std::vector<std::byte> payload(put->payload.begin(), put->payload.end());
        post_to_tables([id = id_, payload = std::move(payload)](Tables& tables) {
            tables.on_gossip(id, payload);
        });
    }

    /// Decodes one Push into this frame's shared outbound block and records it in
    /// `push_batch_` (see `dispatch_frame_body`) -- does not post to Tables itself.
    ///
    /// Composing the outbound bytes here, on this face's own (Tier-1, per-connection,
    /// genuinely parallel) strand rather than later on the single global routing
    /// strand, is deliberate: they are identical for every destination face, so Tier
    /// 2 is left with nothing but matching and handing out slices of one refcounted
    /// block -- and the payload is never copied into an intermediate buffer, going
    /// straight into the outbound bytes.
    ///
    /// A Push that arrives with `scope == 0` already carries the full key, i.e. it is
    /// byte-for-byte what every subscriber should receive, so it is forwarded exactly
    /// as received (one memcpy, no re-encode -- and, as a bonus, every extension it
    /// carries survives verbatim). Only a resmap-compressed one (`scope != 0`) has to
    /// be re-encoded, with its key expanded and everything else carried over.
    [[nodiscard]] auto on_push(ByteReader& r, std::span<const std::byte> batch) -> bool {
        assert(strand_.running_in_this_thread());
        std::size_t const before = r.remaining();
        auto push = Push::decode(r);
        if (!push) return false;
        if (!resolve_key_into(push->wire_expr, key_scratch_)) return false;

        // Broker-internal control traffic (clique gossip) travels as an ordinary
        // Push on a reserved key so it can reuse this face's framing and batching.
        // It is consumed here and never reaches `Tables`, which is what guarantees a
        // client can neither observe it by subscribing under the prefix nor inject it
        // by publishing there -- a Push on such a key from a *client* is simply
        // dropped.
        if (is_internal_key(key_scratch_)) {
            if (kind_ == FaceKind::router) on_internal_push(*push);
            return true;
        }

        // Keep one frame's worth of composed Pushes bounded: a peer can pack a great
        // many small Pushes into one frame, and each may expand (a short resmap-
        // compressed key becomes the full key on the way out). Past the bound, post
        // what's accumulated and start a fresh block rather than growing one without
        // limit.
        if (push_used_ >= max_push_block_bytes) flush_push_batch();
        reset_push_block_if_shared();

        std::optional<MsgSlice> slice;
        if (push->wire_expr.scope == 0) {
            // Exactly the bytes `Push::decode` just consumed out of `batch`.
            auto const raw = batch.subspan(batch.size() - before, before - r.remaining());
            slice = copy_msg_into(push_block_, push_used_, raw);
        } else {
            push->wire_expr =
                WireExpr{.scope = 0, .mapping = Mapping::sender, .suffix = key_scratch_};
            slice = encode_push_into(push_block_, push_used_, *push);
        }
        if (!slice) return true; // pathological encode failure: skip it, stay in sync
        push_used_ = slice->offset + slice->length;

        RoutedPush msg{};
        msg.slice = *slice;
        // Lifted out here, once, so neither the routing strand nor the transmit path
        // has to re-decode the message to learn whether it may be dropped.
        msg.slice.block = (push->qos.inner & 0x08) != 0;
        msg.key.assign(key_scratch_);
        if (auto const& dest = push->dest) msg.dest = dest->zid;
        push_batch_.push_back(std::move(msg));
        return true;
    }

    /// Post whatever Pushes have accumulated for this frame to the routing strand,
    /// and start a fresh block for the next run.
    ///
    /// The block is reused rather than reallocated whenever the previous one has
    /// already been consumed by every face it was delivered to (`unique()`), which in
    /// steady state it has -- so a sustained publish stream allocates nothing per
    /// message *and* nothing per frame. A block still referenced by a lagging face is
    /// simply left to that face and a new one allocated here.
    auto flush_push_batch() -> void {
        if (push_batch_.empty()) {
            push_used_ = 0;
            return;
        }
        push_block_.finish(push_used_);
        std::size_t const count = push_batch_.size();
        post_to_tables([id = id_, block = push_block_, batch = std::move(push_batch_)](
                           Tables& tables) { tables.on_push_batch(id, block, batch); });
        // The batch vector went with the post, so this one starts empty: size it for
        // what the last frame needed, rather than letting it regrow from nothing (a
        // handful of reallocations) on every single frame.
        push_batch_.clear();
        push_batch_.reserve(count);
        push_used_ = 0;
        // NOT tested here: the post above holds a reference by construction, and
        // `asio::post` never runs its handler inline, so `unique()` is false on every
        // single flush -- the block was dropped and reallocated every frame, which is the
        // opposite of the intended reuse (and contradicted docs/BROKER.md's "allocates
        // nothing at all in steady state"). The check belongs at the point of *use*,
        // where the routing strand has usually finished with the previous batch: see
        // `reset_push_block_if_shared`, called before the next frame's first Push.
    }

    /// Start the next frame on a block nothing else still holds.
    ///
    /// Called when a frame's composition begins, not when the previous one is handed
    /// off: by then the routing strand has typically consumed the earlier batch, so the
    /// refcount is back to one and the buffer is genuinely reusable. A block that has
    /// grown oversized is also dropped here rather than kept for the life of the face.
    auto reset_push_block_if_shared() -> void {
        assert(strand_.running_in_this_thread());
        if (push_used_ != 0) return; // mid-frame: the block is in use
        if (!push_block_.unique() || push_block_.capacity() > max_push_block_bytes) {
            push_block_ = SharedBuf{};
        }
    }

    [[nodiscard]] auto on_request(ByteReader& r) -> bool {
        assert(strand_.running_in_this_thread());
        auto req = Request::decode(r);
        if (!req) return false;
        auto key = resolve_key(req->wire_expr);
        if (!key) return false;

        RoutedRequest msg{};
        msg.origin_rid = req->id;
        msg.qos = req->qos;
        msg.key = *key;
        msg.parameters = std::string(req->payload.query.parameters);
        msg.target = req->target;
        if (auto const& body = req->payload.query.body) {
            msg.payload = std::vector<std::byte>(body->payload.begin(), body->payload.end());
        }
        if (auto const& dest = req->dest) msg.dest = dest->zid;
        post_to_tables([id = id_, msg = std::move(msg)](Tables& tables) mutable {
            tables.on_request(id, std::move(msg));
        });
        return true;
    }

    [[nodiscard]] auto on_response(ByteReader& r) -> bool {
        assert(strand_.running_in_this_thread());
        auto rsp = Response::decode(r);
        if (!rsp) return false;

        RoutedResponse msg{};
        msg.local_rid = rsp->rid;
        msg.qos = rsp->qos;
        if (auto const* reply = std::get_if<Reply>(&rsp->payload.body)) {
            auto key = resolve_key(rsp->wire_expr);
            if (!key) return false;
            msg.ok = true;
            msg.key = *key;
            if (auto const* put = std::get_if<Put>(&reply->payload.body))
                msg.payload.assign(put->payload.begin(), put->payload.end());
        } else if (auto const* err = std::get_if<Err>(&rsp->payload.body)) {
            msg.ok = false;
            msg.payload.assign(err->payload.begin(), err->payload.end());
        }
        post_to_tables([id = id_, msg = std::move(msg)](Tables& tables) mutable {
            tables.on_response(id, std::move(msg));
        });
        return true;
    }

    [[nodiscard]] auto on_response_final(ByteReader& r) -> bool {
        assert(strand_.running_in_this_thread());
        auto rf = ResponseFinal::decode(r);
        if (!rf) return false;
        post_to_tables(
            [id = id_, rid = rf->rid](Tables& tables) { tables.on_response_final(id, rid); });
        return true;
    }

    // `Interest`/`InterestFinal` share mid 0x19, disambiguated by the 2-bit MODE
    // field in the header byte (0 = InterestFinal). A real zenoh-rust peer sends
    // this unconditionally -- e.g. a Publisher's writer-side matching-status
    // tracking queries "who currently has a matching Subscriber declared" -- even
    // though v1 doesn't implement declare-replay/interest-based sync at all (see
    // docs/BROKER.md). Just decode-and-tolerate it (matching this codebase's
    // existing "unknown-but-well-formed -> tolerate" idiom, e.g. `on_declare`'s
    // DeclareToken/UndeclareToken/DeclareFinal handling) rather than replying:
    // replying with an `InterestFinal` was tried and tripped a genuine
    // `unreachable code` panic inside the reference client's own routing internals
    // (`south-bound client hat`) -- a real zenoh-rust client-mode session does not
    // expect a router to reply to this particular Interest shape at all. Silently
    // dropping the query (as an unknown mid-frame message otherwise would) is
    // still wrong -- it would fault the whole face -- so this decodes it correctly
    // (to stay in sync with the byte stream) without ever answering it.
    [[nodiscard]] auto on_interest(ByteReader& r) -> bool {
        assert(strand_.running_in_this_thread());
        auto pk = r.peek();
        if (!pk) return false;
        auto const mode_bits =
            static_cast<std::uint8_t>((std::to_integer<unsigned>(*pk) >> 5) & 0x3);
        if (mode_bits == 0) return InterestFinal::decode(r).has_value();
        return Interest::decode(r).has_value();
    }

    // What an Undeclare{Subscriber,Queryable} is withdrawing. Two encodings coexist,
    // and both are legitimate:
    //
    //  * a client identifies the declaration by its entity id, which this face
    //    recorded when the matching Declare arrived (`ids`);
    //  * a peer broker sends id 0 and puts the key expression in the `wire_expr`
    //    extension, because its declarations are per-key aggregates with no entity
    //    id to refer to.
    //
    // The extension wins whenever it is present: it is strictly more specific than
    // an id lookup, and a client that chooses to send it is only ever able to affect
    // its own declarations. Erasing the id entry regardless keeps `ids` from
    // accumulating stale keys on that path.
    /// Remember `key` as the current binding of declaration id `decl_id`, undeclaring
    /// whatever that id was bound to before.
    ///
    /// The id->key map is capped at `max_decl_ids` *entries*, which silently assumed
    /// the binding was one-to-one. It is not: re-declaring the same id with a
    /// different key overwrote the entry here while `ResourceTable` gained a whole new
    /// resource, leaving the old key with no id any undeclare could name. One client,
    /// one id and N keys was therefore an unbounded resource-table leak -- and worse
    /// than memory, since every declare invalidates the match memo and each publish
    /// then rescans every resource on the single routing strand.
    auto rebind_decl_id(std::unordered_map<std::uint32_t, std::string>& ids, std::uint32_t decl_id,
                        const std::string& key, void (Tables::*undeclare)(FaceId, std::string_view))
        -> void {
        auto const prev = ids.find(decl_id);
        if (prev != ids.end() && prev->second != key) {
            post_to_tables([id = id_, old = prev->second, undeclare](Tables& tables) {
                (tables.*undeclare)(id, old);
            });
        }
        ids[decl_id] = key;
    }

    [[nodiscard]] auto undeclared_key(const std::optional<WireExpr>& we,
                                      std::unordered_map<std::uint32_t, std::string>& ids,
                                      std::uint32_t decl_id) -> std::optional<std::string> {
        if (we) {
            ids.erase(decl_id);
            return resolve_key(*we);
        }
        auto it = ids.find(decl_id);
        if (it == ids.end()) return std::nullopt;
        std::string key = std::move(it->second);
        ids.erase(it);
        return key;
    }

    [[nodiscard]] auto on_declare(ByteReader& r) -> bool {
        assert(strand_.running_in_this_thread());
        auto d = Declare::decode(r);
        if (!d) return false;

        if (auto const* dk = std::get_if<DeclareKeyExpr>(&d->body.body)) {
            if (auto k = resolve_key(dk->wire_expr); k && k->size() <= max_key_len) {
                if (resmap_.size() < max_resmap_entries || resmap_.contains(dk->id))
                    resmap_[dk->id] = std::move(*k);
            }
        } else if (auto const* uk = std::get_if<UndeclareKeyExpr>(&d->body.body)) {
            resmap_.erase(uk->id);
        } else if (auto const* ds = std::get_if<DeclareSubscriber>(&d->body.body)) {
            auto key = resolve_key(ds->wire_expr);
            if (key && (sub_ids_.size() < max_decl_ids || sub_ids_.contains(ds->id))) {
                // A peer broker's declarations are aggregates keyed by key
                // expression, always carrying id 0 (see Tables' declaration
                // encoders), so remembering them by id would be meaningless -- and
                // would let every one of them collide on the single slot 0. Their
                // undeclares carry the key expression instead; see below.
                if (kind_ == FaceKind::client)
                    rebind_decl_id(sub_ids_, ds->id, *key, &Tables::on_undeclare_subscriber);
                post_to_tables([id = id_, key = *key](Tables& tables) {
                    tables.on_declare_subscriber(id, key);
                });
            }
        } else if (auto const* us = std::get_if<UndeclareSubscriber>(&d->body.body)) {
            if (auto key = undeclared_key(us->wire_expr, sub_ids_, us->id)) {
                post_to_tables([id = id_, key = std::move(*key)](Tables& tables) mutable {
                    tables.on_undeclare_subscriber(id, key);
                });
            }
        } else if (auto const* dq = std::get_if<DeclareQueryable>(&d->body.body)) {
            auto key = resolve_key(dq->wire_expr);
            if (key && (qbl_ids_.size() < max_decl_ids || qbl_ids_.contains(dq->id))) {
                if (kind_ == FaceKind::client)
                    rebind_decl_id(qbl_ids_, dq->id, *key, &Tables::on_undeclare_queryable);
                QueryableInfo const qinfo = dq->qinfo;
                post_to_tables([id = id_, key = *key, qinfo](Tables& tables) {
                    tables.on_declare_queryable(id, key, qinfo);
                });
            }
        } else if (auto const* uq = std::get_if<UndeclareQueryable>(&d->body.body)) {
            if (auto key = undeclared_key(uq->wire_expr, qbl_ids_, uq->id)) {
                post_to_tables([id = id_, key = std::move(*key)](Tables& tables) mutable {
                    tables.on_undeclare_queryable(id, key);
                });
            }
        }
        // DeclareToken/UndeclareToken/DeclareFinal: liveliness tokens are out of v1
        // scope (see docs/BROKER.md); tolerate and ignore, matching this codebase's
        // general "unknown-but-well-formed -> forward-compat tolerate" idiom.
        return true;
    }

    // Hand everything accumulated so far to one `async_write`, if a write isn't
    // already in flight. Two buffers ping-pong: whatever arrives while a write is
    // outstanding accumulates in `tx_accum_` and goes out in the *next* write, so a
    // burst costs one syscall rather than one per message (and, unlike a
    // scatter-gather write over per-message buffers, isn't capped by ASIO's 64-iovec
    // limit -- a full 64 KiB batch goes out whole).
    //
    // The write is initiated inline, not via `co_spawn` as it once was: ASIO's
    // reactive socket service attempts the send immediately on an idle socket, so
    // for an otherwise-idle face the message reaches the wire within this same
    // handler instead of after another trip through the event loop -- which is a
    // direct, per-message latency saving on top of the syscall saving above.
    auto pump_tx() -> void {
        assert(strand_.running_in_this_thread());
        if (writing_ || tx_len_ == 0) return;
        tx_accum_.swap(tx_inflight_); // tx_accum_ takes over the (spent) other buffer
        tx_inflight_len_ = std::exchange(tx_len_, 0);
        last_tx_ = std::chrono::steady_clock::now();
        open_batch_ = no_open_batch; // the batch we just handed off is closed now
        writing_ = true;
        asio::async_write(
            sock_, asio::buffer(tx_inflight_.data(), tx_inflight_len_),
            asio::bind_executor(
                strand_, asio::bind_allocator(asio::recycling_allocator<void>{},
                                              [this, self = shared_from_this()](
                                                  const asio::error_code& ec, std::size_t) {
                                                  writing_ = false;
                                                  if (ec) {
                                                      // Nothing will ever drain these
                                                      // bytes now. Left as-is they keep
                                                      // `queued_bytes()` permanently
                                                      // above the watermark, pinning the
                                                      // face (and, for a router face,
                                                      // the global congested count) at
                                                      // congested forever.
                                                      tx_inflight_len_ = 0;
                                                      tx_len_ = 0;
                                                      this->set_pressure(FacePressure::ok);
                                                      close_now();
                                                      return;
                                                  }
                                                  // async_write transfers everything or fails -- no
                                                  // partial write to carry over, unlike
                                                  // async_write_some.
                                                  tx_inflight_len_ =
                                                      0; // storage is kept for the next swap
                                                  if (queued_bytes() <= this->low_watermark()) {
                                                      this->set_pressure(FacePressure::ok);
                                                  }
                                                  pump_tx();
                                              })));
    }

    auto close_now() -> void {
        assert(strand_.running_in_this_thread());
        asio::error_code ec;
        sock_.close(ec);
    }

    asio::ip::tcp::socket sock_;
    asio::strand<asio::any_io_executor> strand_;
    FaceId id_;
    Tables* tables_; ///< not owned; Tables outlives every Face during normal operation
    FaceRole role_;  ///< which side of the handshake this face performs
    /// Client or peer broker. Fixed at construction for a dialled link, resolved
    /// from the peer's InitSyn for an accepted one, and read by `build_handle` --
    /// so it is always final before this face is ever registered with `Tables`.
    FaceKind kind_ = FaceKind::client;
    ZenohId zid_{};
    std::uint16_t peer_batch_size_ = 0xffff;
    std::uint32_t frame_sn_ = 0;
    /// When this face last read from, and last wrote to, its socket. Drive the
    /// keepalive/lease logic above; touched only on this face's own strand.
    std::chrono::steady_clock::time_point last_rx_ = std::chrono::steady_clock::now();
    std::chrono::steady_clock::time_point last_tx_ = std::chrono::steady_clock::now();

    /// Reusable receive buffer and the window of it holding read-but-unconsumed
    /// bytes (`[rx_start_, rx_end_)`) -- see `next_batch`/`compact_rx`.
    std::vector<std::byte> rx_buf_ = std::vector<std::byte>(rx_buffer_initial);
    std::size_t rx_start_ = 0;
    std::size_t rx_end_ = 0;
    /// Scratch for the resolved key of the message being decoded (`resolve_key_into`).
    std::string key_scratch_;
    /// The Pushes decoded from the frame currently being dispatched, and the shared
    /// block their re-encoded bytes are written into (`push_used_` bytes of it are
    /// live). Both are members purely to keep their allocations across frames -- see
    /// `flush_push_batch`.
    std::vector<RoutedPush> push_batch_;
    SharedBuf push_block_;
    std::size_t push_used_ = 0;

    /// Outbound byte stream, framed and ready to write: `tx_accum_[0, tx_len_)` is
    /// being filled, `tx_inflight_[0, tx_inflight_len_)` is what the outstanding
    /// `async_write` (if `writing_`) is sending. The two swap on each `pump_tx`, so
    /// neither reallocates in steady state. Both vectors are raw storage: their
    /// `size()` is capacity, and the `*_len_` counters are the live extents.
    std::vector<std::byte> tx_accum_;
    std::size_t tx_len_ = 0;
    std::vector<std::byte> tx_inflight_;
    std::size_t tx_inflight_len_ = 0;
    /// Offset in `tx_accum_` of the still-extendable batch's length prefix, or
    /// `no_open_batch` when the next message must start a new batch.
    static constexpr std::size_t no_open_batch = static_cast<std::size_t>(-1);
    std::size_t open_batch_ = no_open_batch;
    bool writing_ = false;
    /// Set once the read loop has unwound and this face has released its share of
    /// `congested_router_faces_`. Deliveries and pressure raises are ignored from then
    /// on: handlers posted before `remove_face` ran can still land on this strand
    /// afterwards, and honouring them would leak the count (see `set_pressure`).
    /// Face-strand state, like everything else here.
    bool torn_down_ = false;
    /// Whether an inbound `whatami = router` is honoured (BrokerConfig). Immutable
    /// after construction; always true for a dialled link, which this broker chose.
    bool accept_router_faces_ = false;
    /// Shared with `Tables` via `FaceHandle::congested` (see its doc comment).
    /// Always non-null: allocated once at construction, never reassigned.
    std::shared_ptr<std::atomic<FacePressure>> pressure_ =
        std::make_shared<std::atomic<FacePressure>>(FacePressure::ok);
    /// For a dialled clique link, the flag its connector polls before re-dialling.
    /// `Tables` sets it when this link loses a duplicate collapse, so the connector
    /// parks instead of reconnecting into another collapse (see `FaceHandle`).
    std::shared_ptr<std::atomic<bool>> dial_suppress_;
    std::unordered_map<std::uint16_t, std::string> resmap_;
    std::unordered_map<std::uint32_t, std::string> sub_ids_;
    std::unordered_map<std::uint32_t, std::string> qbl_ids_;
};

// Runs one accepted connection end-to-end: handshake, register with `tables` on
// success, then pump the read loop until EOF/fault. Deliberately a file-local
// (anonymous-namespace) function, never declared in broker.cppm -- see this file's
// header comment for why `asio::awaitable<T>` must never appear in a `.cppm`.
//
// MUST be co_spawn'd directly onto `strand` (never onto the bare `ioc`/executor a
// socket happens to report): a coroutine's *associated executor* -- fixed at the
// co_spawn call that starts it -- is what every co_await inside it (including
// nested awaitables like Face::run()/read_loop()/handshake()) resumes on after an
// async op completes on a possibly-different io_context thread. Spawning this on
// `ioc` instead of `strand` would silently let the whole read path (handshake,
// decode, on_push/on_declare/on_request/...) run un-serialized with the
// strand-posted write path (enqueue_and_pump) -- exactly the Tier-1 race the
// per-Face strand exists to prevent. `strand` must be constructed by the caller
// (the accept loop) and passed in, not created here, precisely so the co_spawn
// call below is the one that binds it.
[[nodiscard]] auto accept_face(asio::ip::tcp::socket sock,
                               asio::strand<asio::any_io_executor> strand, Tables& tables,
                               bool accept_router_faces) -> asio::awaitable<void> {
    auto face = std::make_shared<Face>(std::move(sock), strand, next_face_id(), &tables,
                                       FaceRole::listener, accept_router_faces);
    co_await face->run();
}

// The dialled counterpart of `accept_face`: same lifecycle, dialer-side handshake.
// Same anonymous-namespace / pass-the-strand-in discipline applies, and for the same
// reasons -- see `accept_face`'s comment.
[[nodiscard]] auto connect_face(asio::ip::tcp::socket sock,
                                asio::strand<asio::any_io_executor> strand, Tables& tables,
                                std::shared_ptr<std::atomic<bool>> dial_suppress)
    -> asio::awaitable<void> {
    auto face =
        std::make_shared<Face>(std::move(sock), strand, next_face_id(), &tables, FaceRole::dialer,
                               /*accept_router_faces=*/true, std::move(dial_suppress));
    co_await face->run();
}

// Split `tcp/host:port`, `host:port`, or `[v6::addr]:port` into host and port. The
// `tcp/` scheme prefix is optional (this broker speaks only TCP); anything else is
// rejected rather than guessed at. Mirrors `Session`'s own `parse_endpoint`
// (src/runtime/session.cpp), which lives in the client runtime library the broker
// deliberately does not link.
// Drives one outbound clique link forever: dial, run the face to EOF, back off,
// dial again. Anonymous-namespace, like `accept_face`, so `asio::awaitable<T>` never
// reaches a `.cppm`.
//
// The `suppress` flag is how a link that lost a duplicate collapse stops being
// re-dialled: without it this loop would reconnect immediately, be collapsed again,
// and flap indefinitely against a peer it already has a perfectly good link to.
// `Tables` clears the flag when that surviving link goes away.
[[nodiscard]] auto peer_connector(asio::strand<asio::any_io_executor> strand, Tables& tables,
                                  std::string endpoint, std::shared_ptr<std::atomic<bool>> suppress)
    -> asio::awaitable<void>;

// The endpoint to advertise for a broker listening on `host:port`, or nullopt when
// `host` is a wildcard -- `0.0.0.0`/`::` is not something a peer can dial, and
// gossiping it would send the whole clique after an unusable address. A hostname is
// taken at face value (it is presumably resolvable by whoever configured it), and an
// IPv6 literal is bracketed so the result round-trips through `split_endpoint`.
[[nodiscard]] auto advertisable_endpoint(std::string_view host, std::uint16_t port)
    -> std::optional<std::string> {
    if (port == 0) return std::nullopt;
    asio::error_code ec;
    auto const addr = asio::ip::make_address(std::string(host), ec);
    if (!ec) {
        if (addr.is_unspecified()) return std::nullopt;
        if (addr.is_v6()) return "tcp/[" + std::string(host) + "]:" + std::to_string(port);
    }
    return "tcp/" + std::string(host) + ":" + std::to_string(port);
}

// Spread a backoff delay by +/-25% so peers that started together, and therefore
// back off in lockstep, stop colliding. Seeded once per thread; the quality of the
// randomness is irrelevant here, only that it decorrelates the retries.
[[nodiscard]] auto jittered(std::chrono::milliseconds d) -> std::chrono::milliseconds {
    static thread_local std::mt19937 rng{std::random_device{}()};
    std::uniform_int_distribution<std::int64_t> dist(-d.count() / 4, d.count() / 4);
    return d + std::chrono::milliseconds(dist(rng));
}

[[nodiscard]] auto split_endpoint(std::string_view ep)
    -> std::optional<std::pair<std::string, std::uint16_t>> {
    if (ep.starts_with("tcp/")) ep.remove_prefix(4);
    if (ep.empty()) return std::nullopt;

    std::string_view host;
    std::string_view port;
    if (ep.front() == '[') { // bracketed IPv6 literal
        auto const close = ep.find(']');
        if (close == std::string_view::npos || close + 2 >= ep.size() || ep[close + 1] != ':') {
            return std::nullopt;
        }
        host = ep.substr(1, close - 1);
        port = ep.substr(close + 2);
    } else {
        auto const colon = ep.rfind(':');
        if (colon == std::string_view::npos || colon == 0 || colon + 1 == ep.size()) {
            return std::nullopt;
        }
        host = ep.substr(0, colon);
        port = ep.substr(colon + 1);
    }

    std::uint32_t value = 0;
    for (char const c : port) {
        if (c < '0' || c > '9') return std::nullopt;
        value = value * 10 + static_cast<std::uint32_t>(c - '0');
        if (value > 0xffff) return std::nullopt; // also catches an overlong digit run
    }
    if (value == 0) return std::nullopt; // port 0 is meaningless as a dial target
    return std::pair{std::string(host), static_cast<std::uint16_t>(value)};
}

auto peer_connector(asio::strand<asio::any_io_executor> strand, Tables& tables,
                    std::string endpoint, std::shared_ptr<std::atomic<bool>> suppress)
    -> asio::awaitable<void> {
    auto const target = split_endpoint(endpoint);
    if (!target) co_return; // unparseable: nothing worth retrying

    auto token = asio::as_tuple(asio::use_awaitable);
    auto delay = std::chrono::milliseconds(peer_reconnect_min_ms);
    for (;;) {
        if (suppress->load(std::memory_order_relaxed)) {
            // A link to this peer already exists (established from the other side);
            // idle until it goes away rather than dialling into a collapse.
            asio::steady_timer timer{strand, std::chrono::milliseconds(peer_suppressed_poll_ms)};
            co_await timer.async_wait(token);
            continue;
        }

        asio::ip::tcp::resolver resolver{strand};
        auto [rec, results] =
            co_await resolver.async_resolve(target->first, std::to_string(target->second), token);
        if (!rec) {
            asio::ip::tcp::socket sock{strand};
            auto [cec, ep] = co_await asio::async_connect(sock, results, token);
            if (!cec) {
                delay = std::chrono::milliseconds(peer_reconnect_min_ms);
                co_await connect_face(std::move(sock), strand, tables, suppress);
            }
        }
        // The wait applies after a successful link ends too, so a peer that accepts
        // and immediately closes can't become a tight loop. Jittered so that the
        // brokers of a clique, which typically start together and therefore back off
        // in lockstep, don't keep retrying in the same instant as each other.
        asio::steady_timer timer{strand, jittered(delay)};
        co_await timer.async_wait(token);
        delay = std::min(delay * 2, std::chrono::milliseconds(peer_reconnect_max_ms));
    }
}

} // namespace

// The opaque implementation Broker::impl_ points to (see broker.cppm's header
// comment for why `io_context`/`tcp::acceptor` must not appear there directly).
// Fully defined only here, in an implementation unit -- never contributes to the
// module's importable BMI.
struct Broker::Impl {
    asio::io_context ioc;
    asio::ip::tcp::acceptor acceptor{ioc};
    // The acceptor is touched from two places: the accept loop's own coroutine
    // (asio's own worker threads, whichever picks up the next accept completion) and
    // Broker::stop() (called from an arbitrary caller thread, e.g. a test or main()'s
    // signal-handling thread -- not necessarily one of run()'s workers). Without a
    // strand serializing both, `stop()`'s direct `acceptor.close()` races the accept
    // loop's in-flight `async_accept` -- a real bug caught by ThreadSanitizer, not a
    // theoretical one (confirmed: TSan reported a data race in
    // reactive_socket_service_base::close vs. set_internal_non_blocking). Both the
    // accept loop (Broker::run()) and Broker::stop() must reach the acceptor only via
    // this strand.
    asio::strand<asio::any_io_executor> accept_strand{ioc.get_executor()};
    /// The bound port, captured by `do_bind` while the io_context is still idle.
    /// `Broker::port()` returns this instead of asking the acceptor: asio documents
    /// `basic_socket_acceptor` as "Shared objects: Unsafe", and `port()` is called
    /// from arbitrary threads (tests do it while the accept loop is inside
    /// `async_accept`, and it can land concurrently with `stop()`'s `close()`) --
    /// the same race the strand above exists to prevent.
    std::uint16_t bound_port = 0;
    /// Seed peer endpoints from BrokerConfig. Written once by `bind` before any
    /// thread runs the io_context, then only read (by `run`'s connector spawn), so
    /// it needs no strand of its own.
    std::vector<std::string> peers;
    /// Whether an inbound face may become a clique link by announcing
    /// `whatami = router` (BrokerConfig::accept_router_faces). Written once by `bind`
    /// before any thread runs the io_context, then only read.
    bool accept_router_faces = false;
    /// Endpoints that already have a connector coroutine. Guarded by `accept_strand`
    /// (every insertion is posted there), which is also what serializes the seed
    /// peers against gossip-driven dials.
    std::set<std::string> dialing;
    /// Last reported count of unreachable peer brokers, so the partition report
    /// fires on change rather than repeating itself every tick. Touched only from
    /// the routing strand (inside the posted job that reads the count).
    std::size_t reported_unlinked = 0;
};

Broker::Broker(ZenohId router_zid)
    : impl_(std::make_unique<Impl>()),
      tables(asio::strand<asio::any_io_executor>(impl_->ioc.get_executor()), router_zid) {}

Broker::~Broker() { stop(); }

auto Broker::do_bind(std::string_view host, std::uint16_t port) -> bool {
    asio::error_code ec;
    auto const addr = asio::ip::make_address(std::string(host), ec);
    if (ec) return false;
    asio::ip::tcp::endpoint const endpoint(addr, port);

    auto& acceptor = impl_->acceptor;
    acceptor.open(endpoint.protocol(), ec);
    if (ec) return false;
    acceptor.set_option(asio::ip::tcp::acceptor::reuse_address(true), ec);
    if (ec) return false;
    acceptor.bind(endpoint, ec);
    if (ec) return false;
    acceptor.listen(asio::socket_base::max_listen_connections, ec);
    if (ec) return false;

    // Resolve port 0 to the port the OS actually picked, once, here -- no thread is
    // running the io_context yet, so this is the one safe moment to ask.
    asio::error_code local_ec;
    auto const bound = acceptor.local_endpoint(local_ec);
    if (!local_ec) impl_->bound_port = bound.port();
    return true;
}

auto Broker::bind(std::string_view host, std::uint16_t port)
    -> std::expected<std::unique_ptr<Broker>, BindError> {
    return bind(BrokerConfig{.listen_host = std::string(host), .listen_port = port});
}

auto Broker::bind(BrokerConfig cfg) -> std::expected<std::unique_ptr<Broker>, BindError> {
    ZenohId zid{};
    zid.len = 16;
    std::random_device rd;
    for (std::size_t i = 0; i < zid.len; i += sizeof(unsigned)) {
        unsigned const v = rd();
        __builtin_memcpy(zid.bytes.data() + i, &v, sizeof(unsigned));
    }

    // NOLINTNEXTLINE(*-owning-memory) -- private ctor, only callable from here.
    auto broker = std::unique_ptr<Broker>(new Broker(zid));
    if (!broker->do_bind(cfg.listen_host, cfg.listen_port)) {
        return std::unexpected(BindError::bind_failed);
    }
    // Only recorded here: a connector needs the io_context to be running, so the
    // dialling itself starts in `run()`.
    broker->impl_->peers = std::move(cfg.peers);
    broker->impl_->accept_router_faces = cfg.accept_router_faces;

    // Resolved after `do_bind`, not from the config, so that an ephemeral
    // (`port == 0`) listen still advertises the port it actually got.
    std::vector<std::string> advertised;
    if (cfg.advertise) {
        advertised.push_back(*cfg.advertise);
    } else if (auto derived = advertisable_endpoint(cfg.listen_host, broker->port())) {
        advertised.push_back(std::move(*derived));
    }
    broker->tables.set_self_endpoints(std::move(advertised));
    return broker;
}

auto Broker::port() const noexcept -> std::uint16_t { return impl_->bound_port; }

auto Broker::run(unsigned num_threads) -> void {
    if (num_threads == 0) num_threads = 1;
    auto& ioc = impl_->ioc;
    auto& acceptor = impl_->acceptor;

    // A local lambda, not a named/declared coroutine function -- its type is never
    // spelled anywhere outside this function body, so (unlike a named function
    // declared in broker.cppm) it cannot poison the module's BMI. See this file's
    // header comment.
    //
    // Spawned onto `accept_strand`, not the bare `ioc`: the acceptor is shared
    // mutable state touched from both this loop and Broker::stop() (a foreign
    // caller thread), and asio does not serialize concurrent access to one
    // descriptor across threads on its own -- see Impl::accept_strand's comment.
    asio::co_spawn(
        impl_->accept_strand,
        [this, &acceptor]() -> asio::awaitable<void> {
            auto token = asio::as_tuple(asio::use_awaitable);
            for (;;) {
                auto [ec, sock] = co_await acceptor.async_accept(token);
                if (ec) {
                    if (ec == asio::error::operation_aborted) co_return; // stop() closed us
                    continue; // tolerate a transient accept error, keep listening
                }
                // This Face's Tier-1 strand -- constructed here, once, and handed to
                // co_spawn below so accept_face's whole coroutine chain (not just its
                // posted-to-strand write path) actually runs serialized on it. See
                // accept_face's own comment for why this can't be built inside it.
                asio::strand<asio::any_io_executor> strand{sock.get_executor()};
                asio::co_spawn(
                    strand,
                    accept_face(std::move(sock), strand, tables, impl_->accept_router_faces),
                    asio::detached);
            }
        },
        asio::detached);

    // Every outbound link -- seeded from the CLI or learned by gossip -- is opened
    // through this one path, which keeps a set of endpoints already being dialled so
    // the same peer is never connected twice from this side. The set is touched only
    // on `accept_strand`, so it needs no lock of its own.
    auto spawn_connector = [this, &ioc](std::string endpoint) {
        // Rejected here rather than inside the coroutine: `peer_connector` returns
        // immediately on an unparseable endpoint, which used to leave the endpoint in
        // `dialing` forever -- so a peer that later gossiped a *valid* address for the
        // same string could never be dialled.
        if (!split_endpoint(endpoint)) return;
        if (impl_->dialing.size() >= max_dialing) return;    // see max_dialing
        if (!impl_->dialing.insert(endpoint).second) return; // already have a connector
        asio::strand<asio::any_io_executor> strand{ioc.get_executor()};
        asio::co_spawn(strand,
                       peer_connector(strand, tables, std::move(endpoint),
                                      std::make_shared<std::atomic<bool>>(false)),
                       asio::detached);
    };

    // How `Tables` asks for a link once gossip teaches it about a broker it isn't
    // connected to. Invoked from the routing strand, so it hops to `accept_strand`
    // before touching the dialling set -- `Tables` itself never sees an executor.
    tables.set_dial_request([this, spawn_connector](std::string endpoint) {
        asio::post(impl_->accept_strand,
                   [spawn_connector, endpoint = std::move(endpoint)]() mutable {
                       spawn_connector(std::move(endpoint));
                   });
    });

    for (auto const& peer : impl_->peers) {
        asio::post(impl_->accept_strand,
                   [spawn_connector, peer]() mutable { spawn_connector(peer); });
    }

    // Partition reporting. With strict split horizon a dead clique link does not
    // reroute -- the two brokers behind it simply stop seeing each other's clients,
    // while both stay healthy toward everyone else. Nothing about that is visible
    // from the outside, so it is said out loud instead. Reported on change only, so
    // a persistently unreachable peer is one line, not a stream.
    asio::co_spawn(
        impl_->accept_strand,
        [this]() -> asio::awaitable<void> {
            auto token = asio::as_tuple(asio::use_awaitable);
            for (;;) {
                asio::steady_timer timer{impl_->accept_strand,
                                         std::chrono::milliseconds(partition_report_interval_ms)};
                co_await timer.async_wait(token);
                // The count lives on the routing strand, so read it from there.
                asio::post(tables.strand(), [this] {
                    std::size_t const unlinked = tables.unlinked_peer_count();
                    if (unlinked == impl_->reported_unlinked) return;
                    impl_->reported_unlinked = unlinked;
                    if (unlinked == 0) {
                        std::fprintf(stderr, "zenohb: clique complete, all peers reachable\n");
                    } else {
                        std::fprintf(stderr,
                                     "zenohb: %zu known peer broker(s) unreachable -- clients "
                                     "behind them are not visible from here\n",
                                     unlinked);
                    }
                });
            }
        },
        asio::detached);

    std::vector<std::thread> workers;
    workers.reserve(num_threads - 1);
    for (unsigned i = 1; i < num_threads; ++i) {
        workers.emplace_back([&ioc] { ioc.run(); });
    }
    ioc.run(); // this call's own thread is one of the num_threads workers
    for (auto& t : workers) {
        if (t.joinable()) t.join();
    }
}

auto Broker::stop() -> void {
    if (!impl_) return;
    // Route the close through accept_strand rather than touching the acceptor
    // directly from this (possibly foreign, non-io_context) thread -- see
    // Impl::accept_strand's comment; this was a real TSan-caught data race against
    // the accept loop's in-flight async_accept. Bundled with ioc.stop() in the same
    // posted job so the stop only takes effect once the close has actually run.
    asio::post(impl_->accept_strand, [this] {
        asio::error_code ec;
        impl_->acceptor.close(ec);
        impl_->ioc.stop();
    });
}

} // namespace zenoh::broker

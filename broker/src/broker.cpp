module;

#include <asio/as_tuple.hpp>
#include <asio/awaitable.hpp>
#include <asio/bind_allocator.hpp>
#include <asio/bind_executor.hpp>
#include <asio/buffer.hpp>
#include <asio/co_spawn.hpp>
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
#include <expected>
#include <memory>
#include <optional>
#include <random>
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
// Bound on Tables::pending_routing_jobs() before a Face pauses its own reads --
// see Face::throttle_if_backlogged for the rationale (defensive hardening against
// asio::post's own lack of a queue bound, not a fix for a confirmed leak: a single
// fast publisher was measured to never actually backlog the routing strand).
constexpr std::size_t max_pending_routing_jobs = 4096;

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
         Tables* tables)
        : sock_(std::move(sock)), strand_(std::move(strand)), id_(id), tables_(tables) {}

    [[nodiscard]] auto run() -> asio::awaitable<void> {
        if (!co_await handshake()) co_return;

        // Register before the read loop can post anything -- asio::strand preserves
        // FIFO order of *posted* handlers, so this is guaranteed to be processed by
        // Tables before any routing work this face's own reads produce below.
        post_to_tables([handle = build_handle()](Tables& tables) mutable {
            tables.add_face(std::move(handle));
        });

        co_await read_loop();

        post_to_tables([id = id_](Tables& tables) { tables.remove_face(id); });
    }

    /// Frame `msgs` (unframed, already-encoded network messages, in order) into this
    /// face's outbound byte stream and (re)start the async-write chain if idle. Must
    /// run on `strand_` -- the only caller is the closure `build_handle()` posts
    /// through. Taking a whole run at a time is what lets consecutive messages share
    /// one frame and one `write` syscall (see `append_msg`/`pump_tx`).
    auto enqueue_and_pump(const SharedBuf& block, const std::vector<MsgSlice>& slices) -> void {
        assert(strand_.running_in_this_thread());
        auto const bytes = block.bytes();
        for (auto const& slice : slices) {
            if (slice.offset + slice.length <= bytes.size()) {
                append_msg(bytes.subspan(slice.offset, slice.length));
            }
        }
        pump_tx();
    }

  private:
    /// Bytes queued for this face but not yet written to the socket.
    [[nodiscard]] auto queued_bytes() const noexcept -> std::size_t {
        return tx_len_ + tx_inflight_len_;
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
    auto append_msg(std::span<const std::byte> body) -> void {
        if (queued_bytes() >= congested_high_watermark) {
            // Slow consumer: drop rather than close (see congested_high_watermark's
            // comment). Tables should already have stopped routing new messages to
            // this face by the time congested_ is visible on its strand; this is
            // the defensive backstop for whatever was already in flight through
            // that small window, so the queue can't grow past this bound.
            congested_->store(true, std::memory_order_relaxed);
            return;
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
        if (queued_bytes() >= congested_high_watermark) {
            congested_->store(true, std::memory_order_relaxed);
        }
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
            .deliver =
                [self = shared_from_this()](SharedBuf block, std::vector<MsgSlice> slices) {
                    asio::post(self->strand_,
                               asio::bind_allocator(asio::recycling_allocator<void>{},
                                                    [self, block = std::move(block),
                                                     slices = std::move(slices)]() mutable {
                                                        self->enqueue_and_pump(block, slices);
                                                    }));
                },
            .congested = congested_};
    }

    // --- handshake (listener side; mirror of Session::open's client side) ---

    [[nodiscard]] auto handshake() -> asio::awaitable<bool> {
        assert(strand_.running_in_this_thread());
        sock_.set_option(asio::ip::tcp::no_delay(true));

        auto isyn_bytes = co_await next_batch();
        if (!isyn_bytes) co_return false;
        ByteReader isyn_r{*isyn_bytes};
        auto isyn = InitSyn::decode(isyn_r);
        if (!isyn || isyn->version != 9) co_return false;
        zid_ = isyn->identifier.zid;
        peer_batch_size_ = isyn->resolution.batch_size;

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
               max_pending_routing_jobs) {
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
    [[nodiscard]] auto process_batch(std::span<const std::byte> batch) -> bool {
        if (batch.empty()) return true;
        ByteReader r{batch};
        auto pk = r.peek();
        if (!pk) return false;
        std::uint8_t const mid = std::to_integer<std::uint8_t>(*pk) & mid_mask;

        if (mid == FrameHeader::id) {
            if (!FrameHeader::decode(r)) return false;
            return dispatch_frame_body(r, batch);
        }
        if (mid == KeepAlive::id) return true;
        if (mid == Close::id) return false; // peer closed the session
        return true;                        // unknown top-level batch: tolerate (forward-compat)
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
                flush_pushes(); // unknown mid-frame message: cannot be length-skipped safely
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

        // Keep one frame's worth of composed Pushes bounded: a peer can pack a great
        // many small Pushes into one frame, and each may expand (a short resmap-
        // compressed key becomes the full key on the way out). Past the bound, post
        // what's accumulated and start a fresh block rather than growing one without
        // limit.
        if (push_used_ >= max_push_block_bytes) flush_push_batch();

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
        msg.key.assign(key_scratch_);
        if (push->dest) msg.dest = push->dest->zid;
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
        if (!push_block_.unique() || push_block_.capacity() > max_push_block_bytes) {
            push_block_ = SharedBuf{}; // still in flight, or grown oversized: start over
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
        msg.key = *key;
        msg.parameters = std::string(req->payload.query.parameters);
        msg.target = req->target;
        if (req->payload.query.body)
            msg.payload = std::vector<std::byte>(req->payload.query.body->payload.begin(),
                                                 req->payload.query.body->payload.end());
        if (req->dest) msg.dest = req->dest->zid;
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
        std::uint8_t const mode_bits = (std::to_integer<std::uint8_t>(*pk) >> 5) & 0x3;
        if (mode_bits == 0) return InterestFinal::decode(r).has_value();
        return Interest::decode(r).has_value();
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
                sub_ids_[ds->id] = *key;
                post_to_tables([id = id_, key = *key](Tables& tables) {
                    tables.on_declare_subscriber(id, key);
                });
            }
        } else if (auto const* us = std::get_if<UndeclareSubscriber>(&d->body.body)) {
            if (auto it = sub_ids_.find(us->id); it != sub_ids_.end()) {
                std::string key = std::move(it->second);
                sub_ids_.erase(it);
                post_to_tables([id = id_, key = std::move(key)](Tables& tables) mutable {
                    tables.on_undeclare_subscriber(id, key);
                });
            }
        } else if (auto const* dq = std::get_if<DeclareQueryable>(&d->body.body)) {
            auto key = resolve_key(dq->wire_expr);
            if (key && (qbl_ids_.size() < max_decl_ids || qbl_ids_.contains(dq->id))) {
                qbl_ids_[dq->id] = *key;
                QueryableInfo const qinfo = dq->qinfo;
                post_to_tables([id = id_, key = *key, qinfo](Tables& tables) {
                    tables.on_declare_queryable(id, key, qinfo);
                });
            }
        } else if (auto const* uq = std::get_if<UndeclareQueryable>(&d->body.body)) {
            if (auto it = qbl_ids_.find(uq->id); it != qbl_ids_.end()) {
                std::string key = std::move(it->second);
                qbl_ids_.erase(it);
                post_to_tables([id = id_, key = std::move(key)](Tables& tables) mutable {
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
        open_batch_ = no_open_batch; // the batch we just handed off is closed now
        writing_ = true;
        asio::async_write(
            sock_, asio::buffer(tx_inflight_.data(), tx_inflight_len_),
            asio::bind_executor(
                strand_,
                asio::bind_allocator(
                    asio::recycling_allocator<void>{},
                    [this, self = shared_from_this()](const asio::error_code& ec, std::size_t) {
                        writing_ = false;
                        if (ec) {
                            close_now();
                            return;
                        }
                        // async_write transfers everything or fails -- no partial
                        // write to carry over, unlike async_write_some.
                        tx_inflight_len_ = 0; // storage is kept for the next swap
                        if (queued_bytes() <= congested_low_watermark) {
                            congested_->store(false, std::memory_order_relaxed);
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
    ZenohId zid_{};
    std::uint16_t peer_batch_size_ = 0xffff;
    std::uint32_t frame_sn_ = 0;

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
    /// Shared with `Tables` via `FaceHandle::congested` (see its doc comment).
    /// Always non-null: allocated once at construction, never reassigned.
    std::shared_ptr<std::atomic<bool>> congested_ = std::make_shared<std::atomic<bool>>(false);
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
                               asio::strand<asio::any_io_executor> strand, Tables& tables)
    -> asio::awaitable<void> {
    auto face = std::make_shared<Face>(std::move(sock), strand, next_face_id(), &tables);
    co_await face->run();
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
    return !ec;
}

auto Broker::bind(std::string_view host, std::uint16_t port)
    -> std::expected<std::unique_ptr<Broker>, BindError> {
    ZenohId zid{};
    zid.len = 16;
    std::random_device rd;
    for (std::size_t i = 0; i < zid.len; i += sizeof(unsigned)) {
        unsigned const v = rd();
        __builtin_memcpy(zid.bytes.data() + i, &v, sizeof(unsigned));
    }

    // NOLINTNEXTLINE(*-owning-memory) -- private ctor, only callable from here.
    auto broker = std::unique_ptr<Broker>(new Broker(zid));
    if (!broker->do_bind(host, port)) return std::unexpected(BindError::bind_failed);
    return broker;
}

auto Broker::port() const noexcept -> std::uint16_t {
    asio::error_code ec;
    auto const ep = impl_->acceptor.local_endpoint(ec);
    return ec ? 0 : ep.port();
}

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
                asio::co_spawn(strand, accept_face(std::move(sock), strand, tables),
                               asio::detached);
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

module;

#include <asio/any_io_executor.hpp>
#include <asio/strand.hpp>

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <new>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

export module zenoh.broker.tables;

export import zenoh.proto; // ZenohId/QueryTarget/QueryableInfo appear in this module's exported API
export import zenoh.broker.resource; // FaceId/FaceCtx appear in this module's exported API
import zenoh.ke;

// The broker's global routing state: the face registry, the resource table
// (zenoh.broker.resource), and in-flight query bookkeeping. Every mutating method
// here (on_*, add_face, remove_face) assumes it is called from `strand()` — see the
// broker plan's "Concurrency and thread safety" section (Tier 2: one global routing
// strand serializes all of this; per-Face state is a separate Tier 1 concern the
// caller, zenoh.broker.face, is responsible for). `Tables` never touches ASIO sockets
// or `Face` the class directly: a registered face is represented purely by
// `FaceHandle` (id + zid + a strand-safe delivery callback), which is what actually
// decouples this module from zenoh.broker.face and avoids a module dependency cycle.
export namespace zenoh::broker {

/// A refcount-shared, immutable block of encoded, *unframed* network message bytes:
/// either exactly one message (the query path) or every Push decoded from one inbound
/// frame laid end to end (the publish path), addressed by `MsgSlice`.
///
/// Encoding a message is independent of which face receives it (outbound composition
/// always emits `scope == 0`, see docs/BROKER.md), so a fan-out to N subscribers
/// encodes once and hands the same immutable block to all N — the per-target work is
/// reduced to framing those bytes with that face's own SN, which `Face` does by
/// appending to its outbound buffer.
///
/// Deliberately not `std::shared_ptr<std::vector<std::byte>>`, and deliberately one
/// block per *frame* rather than per message: this is the only object the publish
/// path allocates, so it is worth being frugal. Refcount, length and bytes share a
/// single allocation (no separate control block); the storage is handed to the
/// encoder *uninitialized* (`std::vector`'s sized constructor would zero an
/// over-estimated buffer before the encoder overwrote it); and because a `Face` can
/// tell (via `unique()`) that the block it filled last time has since been consumed
/// by everyone it was delivered to, the steady state reuses one block per face and
/// allocates nothing at all. Copying is one relaxed increment — that is what fan-out
/// to an extra face costs.
///
/// Thread-safety is exactly `shared_ptr`'s: the refcount is atomic (a block routinely
/// outlives the strand that encoded it, ending up referenced by several other faces'
/// strands), the bytes are immutable once published, and nothing else is shared.
class SharedBuf {
  public:
    SharedBuf() noexcept = default;
    SharedBuf(const SharedBuf& other) noexcept : rep_(other.rep_) { retain(); }
    SharedBuf(SharedBuf&& other) noexcept : rep_(other.rep_) { other.rep_ = nullptr; }
    auto operator=(const SharedBuf& other) noexcept -> SharedBuf& {
        if (this != &other) {
            release();
            rep_ = other.rep_;
            retain();
        }
        return *this;
    }
    auto operator=(SharedBuf&& other) noexcept -> SharedBuf& {
        if (this != &other) {
            release();
            rep_ = other.rep_;
            other.rep_ = nullptr;
        }
        return *this;
    }
    ~SharedBuf() { release(); }

    /// Allocate an uninitialized block of `capacity` bytes for an encoder to fill,
    /// then declare how much of it is live with `finish`. Empty (falsy) if `capacity`
    /// exceeds what the 32-bit length field can express.
    [[nodiscard]] static auto allocate(std::size_t capacity) -> SharedBuf {
        if (capacity > 0xffff'ffffU) return {};
        SharedBuf m;
        // Single block: header, then `capacity` bytes of storage left uninitialized
        // on purpose — the encoder writes over them immediately.
        void* raw = ::operator new(sizeof(Rep) + capacity);
        m.rep_ =
            ::new (raw) Rep{.refs = 1, .size = 0, .capacity = static_cast<std::uint32_t>(capacity)};
        return m;
    }

    /// Writable view of the whole storage — valid only while this is the sole
    /// reference (see `unique`), i.e. while the owner is still filling it in.
    [[nodiscard]] auto storage() noexcept -> std::span<std::byte> {
        if (rep_ == nullptr) return {};
        return {bytes_of(rep_), rep_->capacity};
    }
    /// Declare the first `n` bytes of the storage live. Immutable to sharers after.
    auto finish(std::size_t n) noexcept -> void {
        if (rep_ != nullptr) rep_->size = static_cast<std::uint32_t>(n);
    }
    /// True when no other reference exists, so the owner may refill the storage.
    /// Acquire-ordered against the last releasing thread's writes, exactly as
    /// `shared_ptr::use_count() == 1` would need to be to be actionable.
    [[nodiscard]] auto unique() const noexcept -> bool {
        return rep_ != nullptr && rep_->refs.load(std::memory_order_acquire) == 1;
    }
    [[nodiscard]] auto capacity() const noexcept -> std::size_t {
        return rep_ == nullptr ? 0 : rep_->capacity;
    }

    [[nodiscard]] explicit operator bool() const noexcept { return rep_ != nullptr; }
    [[nodiscard]] auto bytes() const noexcept -> std::span<const std::byte> {
        if (rep_ == nullptr) return {};
        return {bytes_of(rep_), rep_->size};
    }
    [[nodiscard]] auto size() const noexcept -> std::size_t {
        return rep_ == nullptr ? 0 : rep_->size;
    }

  private:
    struct Rep {
        std::atomic<std::uint32_t> refs;
        std::uint32_t size;
        std::uint32_t capacity;
    };

    [[nodiscard]] static auto bytes_of(Rep* rep) noexcept -> std::byte* {
        return reinterpret_cast<std::byte*>(rep) + sizeof(Rep);
    }
    auto retain() const noexcept -> void {
        // Relaxed: a new reference is only ever created from an existing one held by
        // this thread, so no ordering against the pointee is needed here.
        if (rep_ != nullptr) rep_->refs.fetch_add(1, std::memory_order_relaxed);
    }
    auto release() noexcept -> void {
        if (rep_ == nullptr) return;
        // acq_rel on the last decrement so every other thread's writes/reads through
        // its own reference happen-before this destruction (the standard shared_ptr
        // pattern).
        if (rep_->refs.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            rep_->~Rep();
            ::operator delete(static_cast<void*>(rep_));
        }
        rep_ = nullptr;
    }

    Rep* rep_ = nullptr;
};

/// One encoded message's extent within a `SharedBuf`.
struct MsgSlice {
    std::uint32_t offset = 0;
    std::uint32_t length = 0;
};

/// Encode `push` into `out`, starting at `out[offset]` and growing `out` if needed;
/// returns the slice written, or nullopt on a pathological encode failure (callers
/// skip that message rather than faulting the whole routing step).
///
/// Encoding lives here, and happens *before* the routing strand ever sees the
/// message: `Face` (Tier 1, one strand per connection, so genuinely parallel across
/// the pool) encodes as it decodes, and the single global routing strand (Tier 2)
/// only matches and fans out already-encoded bytes.
[[nodiscard]] auto encode_push_into(SharedBuf& out, std::size_t offset, const Push& push)
    -> std::optional<MsgSlice>;

/// Copy `msg` (one already-encoded network message) into `out` at `offset`, growing
/// `out` if needed; same return contract as `encode_push_into`. Used when a decoded
/// message needs no rewriting at all and can be forwarded exactly as it arrived.
[[nodiscard]] auto copy_msg_into(SharedBuf& out, std::size_t offset, std::span<const std::byte> msg)
    -> std::optional<MsgSlice>;

/// Encode one Push into a block of its own (a whole `SharedBuf` holding just it).
/// Convenience for one-off publishes — the routing path uses `encode_push_into`.
[[nodiscard]] auto make_push_msg(std::string_view key, std::span<const std::byte> payload,
                                 bool is_del) -> SharedBuf;

/// A registered face, as `Tables` sees it: just enough to route. `deliver` hands a
/// run of *unframed* encoded network messages to that face, in order; the callback
/// itself is responsible for posting onto that face's own strand before touching any
/// face-owned state (its per-face frame SN, outbound buffer, socket) — `Tables` never
/// needs to know that seam exists. Deliveries are handed over a whole run at a time
/// (rather than one call per message) so that one inbound frame carrying N Pushes
/// costs one Tables→Face hop per target face, not N: `block` is the shared encoded
/// bytes and `slices` picks out, in order, the messages within it that this
/// particular face is to receive.
struct FaceHandle {
    FaceId id = 0;
    ZenohId zid{};
    std::function<void(SharedBuf block, std::vector<MsgSlice> slices)> deliver;
    /// Set by the owning `Face` itself (from its own strand) once its outbound
    /// queue backs up past a high watermark; cleared once drained below a low
    /// watermark (see `Face::enqueue_and_pump`/`pump_tx` in `broker.cpp`). `Tables`
    /// checks this *before* routing a new message to this face and skips
    /// (drops) delivery to it if congested, rather than queuing further or
    /// disconnecting it -- a slow consumer never stalls the producer or any
    /// other, faster consumer, and never gets disconnected outright either (v1
    /// policy: congestion drops messages rather than blocking or closing -- see
    /// docs/BROKER.md). A plain atomic shared with the owning `Face`, read from
    /// `Tables::strand()` and written from the `Face`'s own strand, by design --
    /// same cross-strand-gauge pattern as `pending_routing_jobs()`. Null only for
    /// a `FaceHandle` that hasn't been fully constructed (never true once
    /// registered via `add_face`).
    std::shared_ptr<std::atomic<bool>> congested;
};

/// A publish/delete, decoded out of a Face's receive buffer and immediately
/// re-encoded into the batch's shared outbound block (never a borrowed view into the
/// receive buffer — see the broker plan's "borrow-only codec boundary" section)
/// before being posted to the routing strand. `key` stays as a separate owned string
/// because that is what the routing strand matches on; `slice` is what it hands to
/// each matching face, relative to the `SharedBuf` passed alongside the batch.
struct RoutedPush {
    MsgSlice slice;              ///< this message's extent in the batch's block
    std::string key;             ///< resolved (resmap-expanded) key, for matching
    std::optional<ZenohId> dest; ///< zid-targeting filter, if the publisher set one
};

/// A query, decoded and copied out of a Face's receive buffer.
struct RoutedRequest {
    std::uint32_t origin_rid = 0; ///< the *requester's own* request id (echoed back)
    std::string key;
    std::string parameters;
    std::optional<std::vector<std::byte>> payload; ///< Query::body's payload, if any
    QueryTarget target = QueryTarget::best_matching;
    std::optional<ZenohId> dest;
};

/// One reply, decoded and copied out of an answering Face's receive buffer.
struct RoutedResponse {
    std::uint32_t local_rid = 0; ///< the rid *the broker* assigned when forwarding
    bool ok = true;              ///< Reply vs Err
    std::string key;             ///< valid iff ok
    std::vector<std::byte> payload;
};

/// Global broker state: face registry, resource table, and query fan-out/fan-in
/// bookkeeping, all serialized on one `asio::strand` (see the file header comment).
class Tables {
  public:
    explicit Tables(asio::strand<asio::any_io_executor> routing_strand, ZenohId router_zid)
        : strand_(std::move(routing_strand)), router_zid_(router_zid) {}

    /// The strand every method below must be called from. Exposed (a documented,
    /// deliberate exception to "keep ASIO out of exported broker signatures") because
    /// `Broker`'s accept loop and tests both need to marshal work onto it.
    [[nodiscard]] auto strand() noexcept -> asio::strand<asio::any_io_executor>& { return strand_; }

    [[nodiscard]] auto router_zid() const noexcept -> const ZenohId& { return router_zid_; }

    /// Backlog gauge for jobs posted to `strand()` but not yet started. `asio::post`
    /// itself has no bound -- if every `Face` (Tier 1) can decode and post work to
    /// this one global routing strand (Tier 2) faster than that single strand can
    /// drain it, the posted-handler queue could in principle grow without limit.
    /// Defensive hardening, not a fix for a confirmed leak: a single fast publisher
    /// was measured (this counter, and flat release-build RSS across 3M+ messages)
    /// to never actually backlog the routing strand at all -- see docs/BROKER.md's
    /// "Performance testing" section for the full empirical record (an apparent
    /// unbounded-RSS-growth reading elsewhere turned out to be an ASan quarantine/
    /// redzone artifact, not this). A plain atomic, not strand-guarded like the
    /// rest of `Tables`: callers increment it from whichever Face thread posts a
    /// job and decrement it from inside the posted job itself (i.e. from the
    /// strand), by design -- see `Face::throttle_if_backlogged` in `broker.cpp`,
    /// which pauses a Face's own reads once this crosses a threshold, giving
    /// Tier 2 real backpressure instead of an unbounded queue, for the many-
    /// simultaneous-fast-publishers case that hasn't actually been observed.
    [[nodiscard]] auto pending_routing_jobs() noexcept -> std::atomic<std::size_t>& {
        return pending_routing_jobs_;
    }

    auto add_face(FaceHandle handle) -> void;
    /// Strip `id` from the resource table and every pending-query entry it owns or
    /// is owed by (a queryable vanishing mid-answer counts as its `ResponseFinal`; a
    /// requester vanishing just erases its entries) — called once when a face
    /// disconnects.
    auto remove_face(FaceId id) -> void;

    auto on_declare_subscriber(FaceId from, std::string_view key) -> void;
    auto on_undeclare_subscriber(FaceId from, std::string_view key) -> void;
    auto on_declare_queryable(FaceId from, std::string_view key, QueryableInfo qinfo) -> void;
    auto on_undeclare_queryable(FaceId from, std::string_view key) -> void;

    /// Route every entry in `msgs` (in order, one entry for an unbatched single
    /// Push), all under one strand visit — filtered by each message's `dest` if
    /// set (a narrowing filter, never a bypass — see `docs/BROKER.md`). Each
    /// entry's `slice` addresses `block`, the one shared buffer holding all their
    /// encoded bytes. `Face::dispatch_frame_body` accumulates every consecutive
    /// Push decoded from one inbound frame into one such batch and posts it here
    /// once, amortizing the Face->Tables `asio::post` hop (and its cross-thread
    /// wakeup cost) over the whole batch instead of paying it per message.
    auto on_push_batch(FaceId from, const SharedBuf& block, const std::vector<RoutedPush>& msgs)
        -> void;
    /// Route a query to every matching queryable face per `msg.target`/`msg.dest`,
    /// recording the fan-out for `on_response`/`on_response_final` to fan back in.
    /// Zero matches synthesizes an immediate `ResponseFinal` back to `from`.
    auto on_request(FaceId from, RoutedRequest msg) -> void;
    /// Forward one reply from an answering face back to the original requester.
    auto on_response(FaceId from, RoutedResponse msg) -> void;
    /// One answering face finished; decrement the fan-in counter and, at zero,
    /// synthesize the requester's own `ResponseFinal`.
    auto on_response_final(FaceId from, std::uint32_t local_rid) -> void;

    // --- test-only introspection; callers must already be on strand() ---
    // Asserted, not just documented: these are as easy to call off-strand by
    // accident as any mutating method (e.g. a test forgetting to wrap one in
    // on_strand()), and being merely read-only doesn't make that safe -- faces_/
    // resources_/pending_queries_/fanout_remaining_ are all written concurrently
    // from strand()-posted handlers.
    [[nodiscard]] auto face_count() const noexcept -> std::size_t {
        assert(strand_.running_in_this_thread());
        return faces_.size();
    }
    [[nodiscard]] auto resource_count() const noexcept -> std::size_t {
        assert(strand_.running_in_this_thread());
        return resources_.resource_count();
    }
    [[nodiscard]] auto pending_query_count() const noexcept -> std::size_t {
        assert(strand_.running_in_this_thread());
        return pending_queries_.size();
    }
    [[nodiscard]] auto fanout_count() const noexcept -> std::size_t {
        assert(strand_.running_in_this_thread());
        return fanout_remaining_.size();
    }
    /// Number of faces registered on the exact resource keyed by `key` (see
    /// `ResourceTable::face_count_for`) — lets a test await "N sessions all declared
    /// the identical subscription/queryable" deterministically.
    [[nodiscard]] auto resource_face_count(std::string_view key) const -> std::size_t {
        assert(strand_.running_in_this_thread());
        return resources_.face_count_for(key);
    }

  private:
    using QueryKey = std::pair<FaceId, std::uint32_t>; ///< (face, that face's local rid)

    /// One target face's share of the batch currently being routed: every message of
    /// that batch that matched it, in arrival order.
    struct Delivery {
        FaceId id = 0;
        std::vector<MsgSlice> slices;
    };

    /// Append `slice` to `to`'s share of the batch in flight, creating its slot in
    /// `deliveries_[0, used)` if this is the first message matching it.
    auto queue_delivery(std::size_t& used, FaceId to, MsgSlice slice) -> void;
    /// Hand each accumulated slot to its face (one `deliver` call per face) and reset.
    auto flush_deliveries(const SharedBuf& block, std::size_t used) -> void;
    /// One-message convenience wrapper around `deliver` (query-path messages, which
    /// have no batch to amortize over).
    static auto deliver_one(const FaceHandle& face, SharedBuf msg) -> void;

    asio::strand<asio::any_io_executor> strand_;
    std::atomic<std::size_t> pending_routing_jobs_{0};
    ZenohId router_zid_;
    std::unordered_map<FaceId, FaceHandle> faces_;
    ResourceTable resources_;
    std::uint32_t next_local_rid_ = 0; ///< monotonic, shared across all forwarded Requests
    std::map<QueryKey, QueryKey> pending_queries_; ///< (answering face,local rid) -> origin
    std::map<QueryKey, int> fanout_remaining_;     ///< origin -> outstanding answer count
    /// Per-face delivery accumulator for the batch currently being routed. A member
    /// (rather than a local) purely to reuse its allocation across calls: only the
    /// first `used` slots of any given `on_push_batch` are live, and nothing outlives
    /// the call.
    std::vector<Delivery> deliveries_;
};

} // namespace zenoh::broker

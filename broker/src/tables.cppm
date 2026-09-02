module;

#include <asio/any_io_executor.hpp>
#include <asio/strand.hpp>

#include <algorithm>
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
export import zenoh.broker.resource;   // FaceId/FaceCtx appear in this module's exported API
export import zenoh.broker.membership; // MemberInfo/Membership appear in this module's exported API
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

/// One encoded message's extent within a `SharedBuf`, plus the one property of it
/// that the transmit path needs to know without re-decoding: whether it may be
/// dropped under congestion.
struct MsgSlice {
    std::uint32_t offset = 0;
    std::uint32_t length = 0;
    /// The standard Zenoh `CongestionControl::Block` bit (QoS bit 3), lifted out of
    /// the message when it was decoded. True means "must not be dropped": a
    /// congested face queues it past the watermark that would otherwise discard it,
    /// and the pressure is pushed back on whoever is producing instead.
    bool block = false;
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

/// What a registered face sits on the other end of: an application `Session`, or a
/// peer broker in the clique (`docs/CLIQUE.md`). Set from the `whatami` the peer
/// announced in its `InitSyn`/`InitAck` -- which the listener-side handshake used to
/// discard -- and it is the *only* thing the split-horizon invariant is enforced
/// from:
///
///   a message received on a router face is never sent to a router face.
///
/// Deliberately a property of the face rather than a marker carried per message (the
/// reference implementation's `NodeId` per-link psid, resolved through a link-state
/// exchange, is the equivalent there): in a clique every broker is exactly one hop
/// away, so "did this arrive from a broker" is all the information routing needs.
/// That bounds every message to at most one inter-broker hop *structurally*, so
/// routing still terminates if the mesh is ever miswired into a cycle -- a stronger
/// guarantee than assuming the clique is correctly configured.
///
/// It is not, however, self-authenticating, and an earlier version of this comment
/// wrongly claimed a peer could not spoof it. The kind of an *inbound* face comes
/// from that peer's own `InitSyn` (`whatami`), which nothing verifies -- so honouring
/// it is opt-in per broker (`BrokerConfig::accept_router_faces`), and off by default.
/// An *outbound* link is a router face unconditionally: this broker chose to dial it.
enum class FaceKind : std::uint8_t { client, router };

/// How far behind a face's outbound queue has fallen. Three levels rather than the
/// original single "congested" bit, because congestion control is now a per-message
/// choice and the two classes have to be distinguishable:
///
///  - `ok`         -- everything is delivered.
///  - `congested`  -- past the high watermark: droppable (`CongestionControl::Drop`)
///                    messages are discarded for this face, but ones marked `Block`
///                    are still queued, and the faces feeding it are read-throttled
///                    so the pressure reaches the producer instead of accumulating.
///  - `saturated`  -- past a hard ceiling far above the watermark, reachable only
///                    once read-throttling has already failed to help, i.e. a peer
///                    that has stopped draining altogether (a dead TCP connection
///                    not yet timed out). Nothing more is queued and the face is
///                    closed: unbounded memory growth becomes a detectable failure.
///
/// Hysteresis lives between `congested` and `ok` (a low watermark), so a face that
/// is merely keeping up does not oscillate.
enum class FacePressure : std::uint8_t { ok = 0, congested = 1, saturated = 2 };

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
    /// Client or peer broker -- see `FaceKind`. Routing reads this on every
    /// fan-out decision, so it is stored inline here rather than looked up.
    FaceKind kind = FaceKind::client;
    /// True if *this* broker dialled the peer, false if it accepted the connection.
    /// Only meaningful for a router face, where it is what lets both ends of a
    /// simultaneous mutual dial agree on which of the two links to keep (see
    /// `keep_outbound_link`).
    bool dialed = false;
    /// Ask the owning `Face` to shut its socket down. Used only to collapse a
    /// duplicate clique link; the read loop then unwinds and posts `remove_face` the
    /// same way any disconnect does, so there is no separate teardown path.
    std::function<void()> close;
    /// For a dialled router face, the flag its connector watches. Setting it parks
    /// that connector instead of letting it re-dial: a link closed as a duplicate
    /// would otherwise reconnect immediately, be collapsed again, and flap forever.
    /// `Tables` clears it once the surviving link to the same peer goes away, which
    /// is what lets the connector take over again. Null for accepted faces.
    std::shared_ptr<std::atomic<bool>> dial_suppress;
    std::function<void(SharedBuf block, std::vector<MsgSlice> slices)> deliver;
    /// Written by the owning `Face` from its own strand as its outbound queue grows
    /// and drains (see `Face::append_msg`/`pump_tx` in `broker.cpp`), and read by
    /// `Tables` from the routing strand *before* routing anything to that face --
    /// the same cross-strand-gauge pattern as `pending_routing_jobs()`. A slow
    /// consumer therefore never stalls the producer or any other, faster consumer;
    /// what it costs is droppable traffic to itself, and only while it is behind.
    /// See `FacePressure` for what each level means. Null only for a `FaceHandle`
    /// that hasn't been fully constructed (never true once registered).
    std::shared_ptr<std::atomic<FacePressure>> pressure;
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
    /// The requester's QoS byte, carried through the forward rather than rebuilt --
    /// otherwise a query marked `CongestionControl::Block` would silently degrade to
    /// `Drop` at the first broker it crossed.
    QoS qos{};
    std::string key;
    std::string parameters;
    std::optional<std::vector<std::byte>> payload; ///< Query::body's payload, if any
    QueryTarget target = QueryTarget::best_matching;
    std::optional<ZenohId> dest;
};

/// One reply, decoded and copied out of an answering Face's receive buffer.
struct RoutedResponse {
    std::uint32_t local_rid = 0; ///< the rid *the broker* assigned when forwarding
    QoS qos{};                   ///< the answering peer's QoS byte, carried through
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

    /// How many peer-broker links are currently behind. Maintained by the `Face`s
    /// themselves (each adjusts it as its own pressure changes) and polled by
    /// *client* faces, which pause their reads while it is non-zero.
    ///
    /// This is what turns "drop" into a last resort rather than a first response: a
    /// clique link carries every client behind it, so rather than discarding their
    /// traffic the moment it backs up, the brokers stop reading from the clients
    /// producing it and let ordinary TCP flow control carry the pressure back to the
    /// publishers. It is also what bounds memory for `CongestionControl::Block`
    /// traffic, which is queued past the watermark and would otherwise have nothing
    /// holding it back. Deliberately coarse -- any congested link throttles every
    /// client -- because the alternative (tracking which clients feed which link)
    /// costs per-message bookkeeping to solve a problem that only bites while a link
    /// is already unhealthy.
    [[nodiscard]] auto congested_router_faces() noexcept -> std::atomic<std::size_t>& {
        return congested_router_faces_;
    }

    /// This broker's own advertised endpoints, for gossip. Called once during
    /// `bind`, before any thread runs the io_context, so it needs no strand.
    auto set_self_endpoints(std::vector<std::string> endpoints) -> void;

    /// How `Tables` asks for an outbound link to be opened. Set once by `Broker`
    /// during `run` (again before any worker thread exists); invoked from
    /// `strand()`, and expected to marshal onto whatever executor actually owns
    /// sockets -- `Tables` itself never touches ASIO.
    auto set_dial_request(std::function<void(std::string endpoint)> fn) -> void;

    /// Ingest a gossip payload received from a peer broker: learn any members it
    /// carries, ask for links to the ones this broker isn't connected to, and
    /// re-advertise if the view changed (which is what closes the clique in one
    /// round). Malformed payloads are ignored, not faulted -- this is untrusted
    /// input off a socket.
    auto on_gossip(FaceId from, std::span<const std::byte> payload) -> void;

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
    /// How many registered faces are peer brokers rather than clients. Lets a clique
    /// test await "both brokers have linked up" deterministically instead of via a
    /// fixed sleep, the same role `resource_face_count` plays for declarations.
    /// How many *other* brokers this one knows to exist, whether or not it
    /// currently holds a link to each. Lets a test assert that gossip converged
    /// separately from asserting that the links did.
    [[nodiscard]] auto known_peer_count() const noexcept -> std::size_t {
        assert(strand_.running_in_this_thread());
        return members_.peer_count();
    }

    /// Known peer brokers this one currently holds no link to.
    ///
    /// Under strict split horizon a single dropped link partitions exactly that pair
    /// while both brokers stay perfectly healthy toward everyone else -- so nothing
    /// looks broken from either side, and the two simply stop being able to see each
    /// other's clients. That silence is the whole reason this exists: a link that
    /// stays down is an operational fault to be surfaced, not routed around.
    [[nodiscard]] auto unlinked_peer_count() const -> std::size_t;

    [[nodiscard]] auto router_face_count() const noexcept -> std::size_t {
        assert(strand_.running_in_this_thread());
        return static_cast<std::size_t>(
            std::count_if(faces_.begin(), faces_.end(),
                          [](const auto& kv) { return kv.second.kind == FaceKind::router; }));
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

    /// What this broker's own *clients* currently declare on one key expression --
    /// i.e. exactly the state that is announced to the clique. Peer-broker faces are
    /// excluded by construction, which is what makes the split-horizon invariant
    /// fall out of the declaration path for free: a declaration relayed from a peer
    /// leaves this unchanged, so nothing is re-announced.
    struct LocalDecl {
        std::size_t subscribers = 0;
        std::size_t queryables = 0;
        /// True if *any* local queryable on this key advertised `complete`. Correct
        /// for `QueryTarget::all_complete`, because the broker on the far side
        /// re-filters against its own queryables at the terminal hop.
        bool complete = false;
        auto operator==(const LocalDecl&) const -> bool = default;
    };

    /// Fold `resources_`'s face list for `canonical_key` into a `LocalDecl`.
    [[nodiscard]] auto local_decl(std::string_view canonical_key) const -> LocalDecl;
    /// Announce to every peer broker whatever changed between `before` and `after`.
    /// Announcements are aggregated: the second client to declare a key sends
    /// nothing, and a withdrawal is only sent once the last one goes.
    auto announce_decl_change(std::string_view key, const LocalDecl& before, const LocalDecl& after)
        -> void;
    /// Hand one already-encoded message to every registered peer-broker face.
    auto to_routers(const SharedBuf& msg) -> void;
    /// Send the whole local declaration state to a peer broker whose link just came
    /// up. This is the entire link-up sync: the reference implementation's routers
    /// do not exchange Interests with each other either, they replay. Idempotent, so
    /// a replay after a link flaps is harmless.
    auto replay_declarations_to(FaceId to) -> void;

    /// One target face's share of the batch currently being routed: every message of
    /// that batch that matched it, in arrival order.
    struct Delivery {
        FaceId id = 0;
        std::vector<MsgSlice> slices;
    };

    /// The registered router face whose peer zid is `zid`, ignoring `except`; 0 if
    /// there is none. Linear over the face registry, which in a clique is tens of
    /// entries and only walked on link-up/link-down.
    [[nodiscard]] auto router_face_with_zid(const ZenohId& zid, FaceId except) const -> FaceId;
    /// Collapse a duplicate link to the same peer, parking the loser's connector.
    /// Returns true if `id` itself was the one closed (so the caller stops treating
    /// it as a live link).
    auto collapse_duplicate_link(FaceId id, const ZenohId& zid, bool dialed) -> bool;
    /// Send the current member view to one peer-broker face.
    auto send_gossip_to(FaceId to) -> void;
    /// Send the current member view to every peer-broker face.
    auto advertise_membership() -> void;
    /// Ask for links to every known broker this one is not connected to.
    auto dial_missing_peers() -> void;
    /// Let any connector that was parked because of a duplicate link to `zid` start
    /// dialling again -- called when a link to that peer is lost.
    auto release_dial_suppression(const ZenohId& zid) -> void;

    /// Whether `id` is a registered peer-broker face. The split-horizon invariant
    /// (see `FaceKind`) is expressed entirely in terms of this. An unregistered id
    /// answers false: a face that has already been removed is not a broker we owe
    /// anything to, and routing to it is skipped moments later anyway.
    [[nodiscard]] auto is_router_face(FaceId id) const -> bool {
        auto it = faces_.find(id);
        return it != faces_.end() && it->second.kind == FaceKind::router;
    }

    /// Append `slice` to `to`'s share of the batch in flight, creating its slot in
    /// `deliveries_[0, used)` if this is the first message matching it.
    auto queue_delivery(std::size_t& used, FaceId to, MsgSlice slice) -> void;
    /// Hand each accumulated slot to its face (one `deliver` call per face) and reset.
    auto flush_deliveries(const SharedBuf& block, std::size_t used) -> void;
    /// One-message convenience wrapper around `deliver` (query-path messages, which
    /// have no batch to amortize over).
    static auto deliver_one(const FaceHandle& face, SharedBuf msg, bool block = false) -> void;

    asio::strand<asio::any_io_executor> strand_;
    std::atomic<std::size_t> pending_routing_jobs_{0};
    std::atomic<std::size_t> congested_router_faces_{0};
    ZenohId router_zid_;
    std::unordered_map<FaceId, FaceHandle> faces_;
    ResourceTable resources_;
    /// Who else is in the clique. Strand-guarded like everything else here.
    Membership members_;
    std::function<void(std::string endpoint)> dial_request_;
    /// Connectors parked because their link lost a duplicate-collapse, paired with
    /// the peer whose surviving link is the reason. Released when that link drops.
    std::vector<std::pair<ZenohId, std::shared_ptr<std::atomic<bool>>>> suppressed_dials_;
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

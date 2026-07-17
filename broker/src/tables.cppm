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
#include <optional>
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

/// A registered face, as `Tables` sees it: just enough to route. `deliver` hands one
/// *unframed* encoded network message (Push/Request/Response/ResponseFinal) to that
/// face; the callback itself is responsible for posting onto that face's own strand
/// before touching any face-owned state (its per-face frame SN, tx queue, socket) —
/// `Tables` never needs to know that seam exists.
struct FaceHandle {
    FaceId id = 0;
    ZenohId zid{};
    std::function<void(std::vector<std::byte>)> deliver;
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

/// A publish/delete, decoded and copied out of a Face's receive buffer (never a
/// borrowed view — see the broker plan's "borrow-only codec boundary" section) before
/// being posted to the routing strand.
struct RoutedPush {
    std::string key;
    std::vector<std::byte> payload; ///< empty for a Del
    bool is_del = false;
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
    /// set (a narrowing filter, never a bypass — see `docs/BROKER.md`).
    /// `Face::dispatch_frame_body` accumulates every consecutive Push decoded
    /// from one inbound frame into one batch and posts it here once, amortizing
    /// the Face->Tables `asio::post` hop (and its cross-thread wakeup cost) over
    /// the whole batch instead of paying it per message.
    auto on_push_batch(FaceId from, const std::vector<RoutedPush>& msgs) -> void;
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

    /// Shared per-message routing body for `on_push_batch` (one message at a time).
    auto route_push(FaceId from, const RoutedPush& msg) -> void;

    asio::strand<asio::any_io_executor> strand_;
    std::atomic<std::size_t> pending_routing_jobs_{0};
    ZenohId router_zid_;
    std::unordered_map<FaceId, FaceHandle> faces_;
    ResourceTable resources_;
    std::uint32_t next_local_rid_ = 0; ///< monotonic, shared across all forwarded Requests
    std::map<QueryKey, QueryKey> pending_queries_; ///< (answering face,local rid) -> origin
    std::map<QueryKey, int> fanout_remaining_;     ///< origin -> outstanding answer count
};

} // namespace zenoh::broker

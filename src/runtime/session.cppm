module;

#include <array>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

export module zenoh.session;

import zenoh.proto;
import zenoh.runtime.tcp;
export import zenoh.runtime.strand; // StrandMode is part of the subscriber/queryable API

// The user-facing API (PLAN.md D8): a vertically-integrated client `Session` that
// opens a TCP transport to a Zenoh router, publishes data (put/batch), and receives
// it (declare_subscriber + the run()/run_once() pump). It owns the link, the protocol
// state (zid, frame SN, keyexpr resmap), and the encode/decode buffers, and drives
// encode->send / recv->decode->dispatch directly — no sans-IO state machine.
export namespace zenoh {

class Session; // forward decl: the handle types below only ever store a `Session*`.

/// Outcome of a session operation. `would_block` is the distinguishing case for
/// `try_put`: the transport could not accept the message without blocking, leaving
/// the decision (drop, retry, back off) to the caller.
enum class ZError : std::uint8_t {
    would_block,        ///< `try_put`: transport not writable right now (backpressure).
    connection_closed,  ///< Peer closed the connection / EOF.
    io_error,           ///< Underlying socket/syscall failure.
    protocol_error,     ///< Router sent an unexpected or malformed message.
    encode_error,       ///< Message could not be encoded (e.g. exceeds the batch size).
    bad_endpoint,       ///< Endpoint string could not be parsed or resolved.
    already_subscribed, ///< A subscriber already exists on this session (single-sub cut).
    already_queryable,  ///< A queryable already exists on this session (single-qbl cut).
    query_timeout,      ///< `get`: no `ResponseFinal` within the requested timeout.
};

/// Whether a received `Sample` is a publication (`put`) or a deletion (`del`).
enum class SampleKind : std::uint8_t { put, del };

/// A received sample: an owned snapshot of one `Push(Put|Del)`. The bytes are copied
/// out of the session receive buffer at delivery time (the decoded proto messages are
/// borrow-only views — PLAN.md D2), so a `Sample` is valid independently of the
/// session and outlives the next `recv`/`run`. Value type (copyable, movable).
class Sample {
  public:
    Sample() = default;
    Sample(std::string key, std::vector<std::byte> payload, SampleKind kind)
        : key_(std::move(key)), payload_(std::move(payload)), kind_(kind) {}

    /// The resolved key expression this sample was published on.
    [[nodiscard]] auto key_expr() const noexcept -> std::string_view { return key_; }
    /// The payload bytes (empty for a `del`).
    [[nodiscard]] auto payload() const noexcept -> std::span<const std::byte> { return payload_; }
    /// Whether this is a put or a delete.
    [[nodiscard]] auto kind() const noexcept -> SampleKind { return kind_; }

  private:
    std::string key_;
    std::vector<std::byte> payload_;
    SampleKind kind_ = SampleKind::put;
};

/// A callback invoked by `run()`/`run_once()` for each delivered sample.
using SampleHandler = std::function<void(const Sample&)>;

/// Per-subscriber delivery tuning. `capacity` bounds the strand (buffered samples);
/// `mode` selects ordered FIFO (backpressure on full) or last-value conflation.
struct SubscriberOptions {
    std::size_t capacity = 256;
    StrandMode mode = StrandMode::ordered;
};

/// A peer's Zenoh id, as returned by `Session::local_zid()` and accepted by the
/// `target_zid` parameter of `put`/`try_put`/`get`. Opaque value type — the
/// conversion to/from the wire `ZenohId` happens only inside session.cpp, since the
/// public `zenoh` module deliberately doesn't re-export `zenoh.proto.fields`.
struct PeerId {
    std::array<std::byte, 16> bytes{};
    std::uint8_t len = 0;
    auto operator==(const PeerId&) const -> bool = default;
};

/// Per-queryable delivery tuning (mirrors `SubscriberOptions`). `complete`/`distance`
/// are advertised to the broker/router via `DeclareQueryable`'s `QueryableInfo`.
struct QueryableOptions {
    std::size_t capacity = 256;
    StrandMode mode = StrandMode::ordered;
    bool complete = false;
    std::uint16_t distance = 0;
};

/// One inbound query delivered to a declared `Queryable`'s handler or via
/// `Queryable::recv()`. Move-only, RAII: the destructor sends the query's
/// `ResponseFinal` best-effort if no reply has finalized it already — mirrors
/// `Subscriber`'s undeclare-on-drop idiom, so a query that receives no explicit
/// reply still terminates cleanly for the requester.
class IncomingQuery {
  public:
    IncomingQuery(const IncomingQuery&) = delete;
    auto operator=(const IncomingQuery&) -> IncomingQuery& = delete;
    IncomingQuery(IncomingQuery&& other) noexcept;
    auto operator=(IncomingQuery&& other) noexcept -> IncomingQuery&;
    ~IncomingQuery();

    /// The (already-resolved) key expression this query was issued against.
    [[nodiscard]] auto key_expr() const noexcept -> std::string_view { return key_; }
    /// The query's `parameters` string (opaque to the session; caller-defined syntax).
    [[nodiscard]] auto parameters() const noexcept -> std::string_view { return params_; }
    /// The query's request payload, if any (empty span if the requester sent none).
    [[nodiscard]] auto payload() const noexcept -> std::span<const std::byte> { return payload_; }

    /// Send one reply (`Response{Reply{Put}}`) on `key_expr`. Callable multiple times —
    /// a queryable may answer with several samples before the query completes.
    [[nodiscard]] auto reply(std::string_view key_expr, std::span<const std::byte> payload)
        -> std::expected<void, ZError>;
    /// Send an error reply (`Response{Err}`).
    [[nodiscard]] auto reply_err(std::span<const std::byte> payload) -> std::expected<void, ZError>;

  private:
    friend class Session;
    IncomingQuery(Session* session, std::uint32_t rid, std::string key, std::string params,
                  std::vector<std::byte> payload) noexcept
        : session_(session), rid_(rid), key_(std::move(key)), params_(std::move(params)),
          payload_(std::move(payload)) {}

    Session* session_ = nullptr; ///< owning session (not owned); null when moved-from
    std::uint32_t rid_ = 0;
    std::string key_;
    std::string params_;
    std::vector<std::byte> payload_;
};

/// A callback invoked by `run()`/`run_once()` for each incoming query.
using QueryHandler = std::function<void(IncomingQuery)>;

/// How multiple replies to a `get()` should be consolidated. Own type (not
/// `zenoh.proto.fields`'s `ConsolidationMode`) for the same reason as `PeerId`: the
/// public `zenoh` module doesn't re-export the codec, so a public API signature can't
/// name a `zenoh.proto` type directly (`import zenoh;` alone wouldn't compile).
enum class GetConsolidation : std::uint8_t { automatic, none, monotonic, latest };

/// Which queryable(s) a `get()` should be routed to. Own type, same reason as
/// `GetConsolidation`.
enum class GetTarget : std::uint8_t { best_matching, all, all_complete };

/// What a router should do with this message when a link it needs is congested.
///
/// This is the standard Zenoh `CongestionControl`, carried in bit 3 of the QoS
/// extension every `Push`/`Request` already has room for -- not a project-local
/// setting -- so a broker that understands it treats a `zenoh-rust` publisher's
/// choice identically to this client's.
///
///  - `drop` (the default) lets a router discard the message rather than let one
///    slow consumer stall the producer or anybody else. Right for telemetry, sensor
///    streams, and anything where the next value supersedes this one.
///  - `block` says this message must not be discarded. A router queues it past the
///    point where it would drop, and pushes back on whoever is producing it instead,
///    so the loss is converted into slowdown. Right for commands, configuration, and
///    anything a receiver cannot reconstruct from a later message.
///
/// The cost of `block` is that the backpressure reaches the publisher, which slows
/// it toward *every* destination rather than only the congested one -- unavoidable
/// for "never dropped", and the reason this is per message rather than a broker-wide
/// mode. A genuinely stuck peer (one that stops draining altogether) is eventually
/// disconnected rather than allowed to consume memory without bound.
enum class CongestionControl : std::uint8_t { drop, block };

/// Options for `Session::put`/`try_put`. Mirrors `GetOptions` on the query side.
struct PutOptions {
    /// When set, the broker narrows delivery to the one peer with this Zenoh id
    /// (still ANDed with normal subscription matching -- never delivered to a peer
    /// without a matching Subscriber declared). Works across a broker clique: the
    /// filter is applied by whichever broker actually owns that peer.
    std::optional<PeerId> target_zid{};
    /// Whether this message may be dropped under congestion. See `CongestionControl`.
    CongestionControl congestion = CongestionControl::drop;
};

/// Options for `Session::get`. `target_zid`, when set, is a broker-enforced filter
/// (see `docs/BROKER.md`) narrowing which queryable(s) may answer — like `put`'s
/// `target_zid`, it only ever narrows normal key-expression-declaration matching,
/// never bypasses it.
struct GetOptions {
    GetConsolidation consolidation = GetConsolidation::automatic;
    GetTarget target = GetTarget::best_matching;
    /// Reply-collection deadline, in milliseconds. Unset uses a 10 s default.
    /// Enforced client-side (`Getter::recv()`'s pump loop, and the callback `get()`'s
    /// internal bookkeeping drained by `run()`/`run_once()`) — the broker does not
    /// enforce query timeouts in v1. A plain millisecond count, not
    /// `std::chrono::milliseconds`: the latter would pull `<chrono>`'s (transitive,
    /// version-dependent) `<vector>` dependency into every importer's exported API
    /// surface, which has been observed to collide with an importer's own unrelated
    /// `<vector>` include under this toolchain's named-module + libc++ combination.
    std::optional<std::uint32_t> timeout_ms{};
    std::optional<PeerId> target_zid{};
    /// Whether the query may be dropped under congestion. See `CongestionControl`.
    /// Applies to the request travelling *out*; replies carry the same setting back.
    CongestionControl congestion = CongestionControl::drop;
};

/// One reply delivered to a `get()` caller: either an Ok sample (`Response{Reply}`)
/// or an error payload (`Response{Err}`). Value type (copyable, movable), mirroring
/// `Sample`.
class GetReply {
  public:
    GetReply() = default;

    /// Whether this is a successful reply (`sample()` valid) or an error
    /// (`error_payload()` valid).
    [[nodiscard]] auto is_ok() const noexcept -> bool { return ok_; }
    /// The reply sample. Only meaningful when `is_ok()`.
    [[nodiscard]] auto sample() const noexcept -> const Sample& { return sample_; }
    /// The error payload. Only meaningful when `!is_ok()`.
    [[nodiscard]] auto error_payload() const noexcept -> std::span<const std::byte> {
        return err_payload_;
    }

  private:
    friend class Session;
    bool ok_ = true;
    Sample sample_{};
    std::vector<std::byte> err_payload_{};
};

/// A callback invoked by `run()`/`run_once()` for each reply of a callback-style `get`.
using GetReplyHandler = std::function<void(const GetReply&)>;

class Batch;
class Subscriber;
struct SubReg; // defined in session.cpp (holds the non-movable Strand + handler)
class Queryable;
struct QblReg; // defined in session.cpp
class Getter;
struct GetReg; // defined in session.cpp

/// Out-of-line deleter for `pending_gets_`'s `unique_ptr<GetReg>` entries. A bare
/// `std::unique_ptr<Incomplete>` *direct* member (see `sub_`/`qbl_` below) is the
/// ordinary, safe pimpl idiom as long as the enclosing class's own destructor is
/// declared here and defined out-of-line where the type is complete (which
/// `Session::~Session()` already is). A `std::unordered_map<K, unique_ptr<Incomplete>>`
/// is not safe the same way: under clang+libstdc++ (this project's Linux/CI
/// toolchain — see `CLAUDE.md`), the container's own implicitly-defined special
/// members eagerly instantiate `std::default_delete<GetReg>::operator()`'s body
/// (and its `static_assert(sizeof(T)>0)`) while parsing this interface unit, even
/// though nothing here actually destroys a `GetReg` — confirmed via bisection: the
/// same code compiles cleanly under clang+libc++ (macOS), so this is a
/// libstdc++-specific eagerness quirk, not a language rule. A custom deleter whose
/// `operator()` is merely *declared* here (its `noexcept` alone is all the
/// container's trait computations need) and *defined* in `session.cpp` (where
/// `GetReg` is complete) sidesteps it entirely.
struct GetRegDeleter {
    auto operator()(GetReg* p) const noexcept -> void;
};

/// A client session to a single Zenoh router over TCP.
///
/// Lifecycle: `open()` performs the 4-way transport handshake and returns a ready
/// session; `put`/`try_put`/`batch` publish; `get` queries; `declare_subscriber` +
/// `run()`/`run_once()` (callback) or `Subscriber::recv()` (pull) receive samples;
/// `declare_queryable` + `run()`/`run_once()` (callback) or `Queryable::recv()`
/// (pull) receive queries; the destructor closes the link. Move-only. NOTE (first
/// cut): the receive path is single-threaded — do not pump from one thread while
/// another publishes; serialize calls.
class Session {
  public:
    Session(const Session&) = delete;
    auto operator=(const Session&) -> Session& = delete;
    Session(Session&&) noexcept;
    auto operator=(Session&&) noexcept -> Session&;
    ~Session();

    /// Connect to `endpoint` (e.g. "tcp/127.0.0.1:7447" or "127.0.0.1:7447") and run
    /// the InitSyn/InitAck/OpenSyn/OpenAck handshake as a client.
    [[nodiscard]] static auto open(std::string_view endpoint) -> std::expected<Session, ZError>;

    /// This session's own Zenoh id (the random 16-byte id generated during `open()`).
    [[nodiscard]] auto local_zid() const noexcept -> PeerId;

    /// Publish `payload` to `key_expr`, blocking until the whole message has been
    /// handed to the transport. Any bytes left pending by a prior `try_put` are
    /// flushed first. If `target_zid` is set, the broker narrows delivery to the one
    /// peer with that Zenoh id (still ANDed with normal subscription matching —
    /// never delivered to a peer without a matching Subscriber declared), and
    /// `opts.congestion` selects whether it may be dropped under congestion.
    [[nodiscard]] auto put(std::string_view key_expr, std::span<const std::byte> payload,
                           PutOptions opts = {}) -> std::expected<void, ZError>;

    /// Like `put`, but never blocks. Returns `ZError::would_block` only when the
    /// transport could not accept *any* bytes of the message right now (nothing was
    /// sent, the SN is untouched). If a partial write succeeds, the unsent tail is
    /// buffered and flushed on the next call, and `try_put` returns success — the
    /// message is committed, just not fully drained yet.
    [[nodiscard]] auto try_put(std::string_view key_expr, std::span<const std::byte> payload,
                               PutOptions opts = {}) -> std::expected<void, ZError>;

    /// Open an API-level batch bound to this session. Put operations on the batch
    /// accumulate into a single Frame and are sent as one TCP batch when it fills,
    /// on `Batch::flush()`, or when the batch is destroyed. The session must outlive
    /// any batch created from it.
    [[nodiscard]] auto batch() -> Batch;

    /// Issue a query on `key_expr` (`parameters` is an opaque, caller-defined query
    /// string) and pull replies with `Getter::recv()` until it returns `nullopt`
    /// (the query completed normally) or an error (incl. `query_timeout`).
    [[nodiscard]] auto get(std::string_view key_expr, std::string_view parameters = {},
                           GetOptions opts = {}) -> std::expected<Getter, ZError>;

    /// Issue a query whose replies are delivered to `on_reply` by `run()`/`run_once()`.
    [[nodiscard]] auto get(std::string_view key_expr, std::string_view parameters,
                           GetReplyHandler on_reply, GetOptions opts = {})
        -> std::expected<void, ZError>;

    /// Declare a subscriber on `key_expr` (sends `Frame(Declare{DeclareSubscriber})`).
    /// Without a handler the subscriber is pull-based — drive it with `Subscriber::recv`.
    /// First cut: at most one subscriber per session (`already_subscribed` otherwise).
    /// The session must outlive the returned subscriber.
    [[nodiscard]] auto declare_subscriber(std::string_view key_expr, SubscriberOptions opts = {})
        -> std::expected<Subscriber, ZError>;

    /// Declare a subscriber whose `on_sample` callback is invoked by `run()`/`run_once()`
    /// for each delivered sample.
    [[nodiscard]] auto declare_subscriber(std::string_view key_expr, SampleHandler on_sample,
                                          SubscriberOptions opts = {})
        -> std::expected<Subscriber, ZError>;

    /// Declare a queryable on `key_expr` (sends `Frame(Declare{DeclareQueryable})`).
    /// Without a handler the queryable is pull-based — drive it with `Queryable::recv`.
    /// First cut: at most one queryable per session (`already_queryable` otherwise).
    /// The session must outlive the returned queryable.
    [[nodiscard]] auto declare_queryable(std::string_view key_expr, QueryableOptions opts = {})
        -> std::expected<Queryable, ZError>;

    /// Declare a queryable whose `on_query` callback is invoked by `run()`/`run_once()`
    /// for each incoming query.
    [[nodiscard]] auto declare_queryable(std::string_view key_expr, QueryHandler on_query,
                                         QueryableOptions opts = {})
        -> std::expected<Queryable, ZError>;

    /// Pump the receive loop once: deliver one batch's worth of progress (decode +
    /// dispatch to the subscriber/queryable/get handlers), send a keepalive if the
    /// link is idle, or report `connection_closed`/`protocol_error`. Returns when it
    /// has made progress or the idle keepalive timer fired. Single-threaded (first cut).
    /// NOTE: calling `get()` reentrantly from within a `GetReplyHandler`/`QueryHandler`
    /// invoked here is not supported — it can invalidate the iterator this call is
    /// using to drain in-flight callback-style `get()`s. Issue follow-up queries after
    /// `run_once()`/`run()` returns, not from inside a handler.
    [[nodiscard]] auto run_once() -> std::expected<void, ZError>;

    /// Pump `run_once()` in a loop until the connection closes or the stream faults.
    /// Returns the terminal error (`connection_closed` on a clean EOF).
    [[nodiscard]] auto run() -> std::expected<void, ZError>;

    /// Send a Close and tear down the link. Idempotent. NOTE: any `Subscriber`/
    /// `Queryable`/`IncomingQuery`/`Getter` destructor (or explicit `undeclare()`)
    /// firing *after* this call silently no-ops its wire message (the link is
    /// already invalid) — drop or undeclare outstanding handles before calling
    /// `close()`, not after, if their cleanup message needs to actually reach the
    /// peer.
    auto close() -> void;

  private:
    friend class Batch;
    friend class Subscriber;
    friend class Queryable;
    friend class IncomingQuery;
    friend class Getter;
    Session() = default;

    /// Wrap `msg_bytes` (one or more concatenated network messages) in a FrameHeader
    /// with the current SN, frame it with the 2-byte batch length, and write it all
    /// (blocking). Advances the SN. Flushes any `try_put` backlog first.
    [[nodiscard]] auto write_frame(std::span<const std::byte> msg_bytes)
        -> std::expected<void, ZError>;

    [[nodiscard]] auto encode_put(std::string_view key_expr, std::span<const std::byte> payload,
                                  const PutOptions& opts) -> std::expected<void, ZError>;
    [[nodiscard]] auto encode_put_head(std::string_view key_expr,
                                       std::span<const std::byte> payload, const PutOptions& opts)
        -> std::expected<std::size_t, ZError>;
    [[nodiscard]] auto flush_pending() -> std::expected<void, ZError>;

    // --- receive path (subscriber) ---
    /// Encode + send a `Frame(Declare{DeclareSubscriber{id, key}})` (blocking).
    [[nodiscard]] auto write_declare_subscriber(std::uint32_t id, std::string_view key)
        -> std::expected<void, ZError>;
    /// Encode + send a `Frame(Declare{UndeclareSubscriber{id}})` (best-effort).
    auto write_undeclare_subscriber(std::uint32_t id) -> void;
    /// Send an SN-less transport `KeepAlive` (blocking).
    [[nodiscard]] auto send_keepalive() -> std::expected<void, ZError>;
    /// One step of decode/dispatch: posts decoded samples/queries/replies into the
    /// relevant strand. `max_wait_ms`, when set, bounds how long this call may block
    /// waiting for data below the session's normal keepalive cadence (used by
    /// `get_recv` so a short `GetOptions::timeout` can be noticed promptly instead of
    /// waiting up to a full keepalive interval) — a keepalive is only actually sent
    /// when the *real* keepalive interval elapsed (`max_wait_ms >= keepalive_ms_`),
    /// never merely because a shortened wait timed out.
    [[nodiscard]] auto pump_step(std::optional<std::int32_t> max_wait_ms = std::nullopt)
        -> std::expected<void, ZError>;
    /// Decode + post network messages from the in-progress frame cursor until a
    /// strand is full or the frame is exhausted.
    [[nodiscard]] auto dispatch_cursor() -> std::expected<void, ZError>;
    /// Resolve a received `WireExpr` to an owned key string (via the resmap).
    [[nodiscard]] auto resolve_key(const WireExpr& we) -> std::expected<std::string, ZError>;
    /// Pull the next sample (drives `pump_step` until the strand yields). For `Subscriber`.
    [[nodiscard]] auto sub_recv() -> std::expected<Sample, ZError>;
    /// Drop the active subscriber registration (after best-effort undeclare). For `Subscriber`.
    auto sub_drop() -> void;

    // --- receive path (queryable) ---
    /// Encode + send a `Frame(Declare{DeclareQueryable{id, key, qinfo}})` (blocking).
    [[nodiscard]] auto write_declare_queryable(std::uint32_t id, std::string_view key,
                                               QueryableInfo qinfo) -> std::expected<void, ZError>;
    /// Encode + send a `Frame(Declare{UndeclareQueryable{id}})` (best-effort).
    auto write_undeclare_queryable(std::uint32_t id) -> void;
    /// Pull the next query (drives `pump_step` until the strand yields). For `Queryable`.
    [[nodiscard]] auto qbl_recv() -> std::expected<IncomingQuery, ZError>;
    /// Drop the active queryable registration (after best-effort undeclare). For `Queryable`.
    auto qbl_drop() -> void;
    /// Encode + send one `Frame(Response{Reply|Err})` for request id `rid` (blocking).
    [[nodiscard]] auto send_response(std::uint32_t rid, std::string_view key_expr,
                                     std::span<const std::byte> payload, bool is_err)
        -> std::expected<void, ZError>;
    /// Encode + send a `Frame(ResponseFinal{rid})` (best-effort). For `IncomingQuery`.
    auto send_response_final(std::uint32_t rid) -> void;

    // --- query path (getter) ---
    /// Encode + send a `Frame(Request{...})` for request id `rid` (blocking).
    [[nodiscard]] auto write_request(std::uint32_t rid, std::string_view key_expr,
                                     std::string_view parameters, const GetOptions& opts)
        -> std::expected<void, ZError>;
    /// Shared body of both `get()` overloads: allocates a request id, sends the
    /// `Request`, and registers `pending_gets_[rid]`.
    [[nodiscard]] auto start_get(std::string_view key_expr, std::string_view parameters,
                                 GetReplyHandler handler, const GetOptions& opts)
        -> std::expected<std::uint32_t, ZError>;
    /// Pull the next reply for `rid` (drives `pump_step`). `nullopt` = query complete.
    /// For `Getter`.
    [[nodiscard]] auto get_recv(std::uint32_t rid)
        -> std::expected<std::optional<GetReply>, ZError>;
    /// Drop a `Getter`'s local bookkeeping (no wire message — `get()` has nothing to
    /// undeclare; this just stops us tracking a rid nobody will ever poll again).
    auto get_drop(std::uint32_t rid) -> void;

    TcpLink link_{};
    ZenohId local_zid_{};
    std::uint32_t frame_sn_ = 0;          ///< next reliable-frame SN (mod resolution mask)
    std::uint16_t batch_size_ = 0xffff;   ///< negotiated max TCP batch (incl. 2-byte length prefix)
    std::vector<std::byte> tx_scratch_{}; ///< reusable encode buffer (framed batch)
    std::vector<std::byte> tx_pending_{}; ///< bytes encoded but not yet fully written
    std::size_t pending_off_ = 0;         ///< how much of tx_pending_ is already sent

    std::vector<std::byte> rx_buf_{}; ///< current received TCP batch
    std::size_t rx_pos_ = 0;          ///< dispatch cursor into rx_buf_ (frame body)
    std::size_t rx_end_ = 0;          ///< end of the in-progress frame body (==pos: none)
    std::unordered_map<std::uint16_t, std::string> resmap_; ///< router keyexpr id -> key
    std::optional<ZError> fault_{};     ///< sticky terminal fault (stream desynced)
    std::uint32_t next_entity_id_ = 0;  ///< monotonic subscriber/queryable entity id
    std::int32_t keepalive_ms_ = 2500;  ///< idle keepalive cadence (negotiated lease / 4)
    std::unique_ptr<SubReg> sub_{};     ///< the single active subscriber (first cut)
    std::unique_ptr<QblReg> qbl_{};     ///< the single active queryable (first cut)
    std::uint32_t next_request_id_ = 0; ///< monotonic get() request id
    std::unordered_map<std::uint32_t, std::unique_ptr<GetReg, GetRegDeleter>>
        pending_gets_{}; ///< in-flight get()s
};

/// An API-level publish batch: accumulates `put`s into a single Frame (one SN, many
/// `Push` messages) and sends them as one TCP batch when full, on `flush()`, or on
/// destruction. Coalescing many small puts into one frame cuts per-message header
/// and syscall overhead.
///
/// Created via `Session::batch()`; the originating session must outlive the batch.
/// Move-only. `put`/`flush` block (like `Session::put`); the destructor flushes any
/// remaining messages best-effort (errors are swallowed — call `flush()` explicitly
/// if you need to observe them).
class Batch {
  public:
    Batch(const Batch&) = delete;
    auto operator=(const Batch&) -> Batch& = delete;
    Batch(Batch&& other) noexcept;
    auto operator=(Batch&& other) noexcept -> Batch&;
    ~Batch();

    /// Append a Put to the batch. If adding it would overflow the frame, the already
    /// buffered messages are sent first (blocking) and this Put begins the next
    /// frame. Returns `encode_error` if a single Put cannot fit in one frame.
    [[nodiscard]] auto put(std::string_view key_expr, std::span<const std::byte> payload,
                           PutOptions opts = {}) -> std::expected<void, ZError>;

    /// Send any buffered messages now (blocking). A no-op if the batch is empty.
    [[nodiscard]] auto flush() -> std::expected<void, ZError>;

    /// Number of Put operations currently buffered (not yet sent).
    [[nodiscard]] auto size() const noexcept -> std::size_t { return count_; }
    /// Whether nothing is buffered.
    [[nodiscard]] auto empty() const noexcept -> bool { return count_ == 0; }

  private:
    friend class Session;
    explicit Batch(Session* session) noexcept : session_(session) {}

    Session* session_ = nullptr;   ///< owning session (not owned); null when moved-from
    std::vector<std::byte> buf_{}; ///< accumulated `Push` bytes
    std::size_t body_len_ = 0;     ///< valid prefix of buf_
    std::size_t count_ = 0;        ///< buffered Put operations
};

/// A handle to a declared subscription. Holds a non-owning pointer to its `Session`
/// (the session must outlive it, like `Batch`). Move-only. The destructor undeclares
/// best-effort. Pull samples with `recv()` (pull-based subscribers), or let the
/// session's `run()`/`run_once()` invoke the handler (callback subscribers).
class Subscriber {
  public:
    Subscriber(const Subscriber&) = delete;
    auto operator=(const Subscriber&) -> Subscriber& = delete;
    Subscriber(Subscriber&& other) noexcept;
    auto operator=(Subscriber&& other) noexcept -> Subscriber&;
    ~Subscriber();

    /// Block until the next sample arrives, pumping the session (and emitting
    /// keepalives) as needed. Returns `connection_closed` on EOF or `protocol_error`
    /// on a malformed stream. Intended for pull-based (no-handler) subscribers.
    [[nodiscard]] auto recv() -> std::expected<Sample, ZError>;

    /// Undeclare and stop receiving (sends `Frame(Declare{UndeclareSubscriber})`).
    /// Idempotent; also run by the destructor.
    auto undeclare() -> void;

    /// The key expression this subscriber was declared on.
    [[nodiscard]] auto key_expr() const noexcept -> std::string_view;

  private:
    friend class Session;
    explicit Subscriber(Session* session) noexcept : session_(session) {}

    Session* session_ = nullptr; ///< owning session (not owned); null when moved-from/undeclared
};

/// A handle to a declared queryable. Holds a non-owning pointer to its `Session`
/// (the session must outlive it, like `Subscriber`). Move-only. The destructor
/// undeclares best-effort. Pull queries with `recv()` (pull-based queryables), or let
/// the session's `run()`/`run_once()` invoke the handler (callback queryables).
class Queryable {
  public:
    Queryable(const Queryable&) = delete;
    auto operator=(const Queryable&) -> Queryable& = delete;
    Queryable(Queryable&& other) noexcept;
    auto operator=(Queryable&& other) noexcept -> Queryable&;
    ~Queryable();

    /// Block until the next query arrives, pumping the session as needed. Returns
    /// `connection_closed` on EOF or `protocol_error` on a malformed stream. Intended
    /// for pull-based (no-handler) queryables.
    [[nodiscard]] auto recv() -> std::expected<IncomingQuery, ZError>;

    /// Undeclare and stop receiving (sends `Frame(Declare{UndeclareQueryable})`).
    /// Idempotent; also run by the destructor.
    auto undeclare() -> void;

    /// The key expression this queryable was declared on.
    [[nodiscard]] auto key_expr() const noexcept -> std::string_view;

  private:
    friend class Session;
    explicit Queryable(Session* session) noexcept : session_(session) {}

    Session* session_ = nullptr; ///< owning session (not owned); null when moved-from/undeclared
};

/// A handle to one in-flight `get()`. Holds a non-owning pointer to its `Session`
/// (the session must outlive it, like `Subscriber`/`Queryable`). Move-only.
/// Transient: unlike `Subscriber`/`Queryable` there is no wire "undeclare" for a
/// `get()`, so dropping a `Getter` just stops local bookkeeping for its request id.
class Getter {
  public:
    Getter(const Getter&) = delete;
    auto operator=(const Getter&) -> Getter& = delete;
    Getter(Getter&& other) noexcept;
    auto operator=(Getter&& other) noexcept -> Getter&;
    ~Getter();

    /// Block until the next reply arrives (pumping the session as needed), returning
    /// `nullopt` once the query has completed (the broker's `ResponseFinal` was
    /// received and every buffered reply drained) — not an error. Returns
    /// `ZError::query_timeout` if `GetOptions::timeout` elapses first.
    [[nodiscard]] auto recv() -> std::expected<std::optional<GetReply>, ZError>;

  private:
    friend class Session;
    Getter(Session* session, std::uint32_t rid) noexcept : session_(session), rid_(rid) {}

    Session* session_ = nullptr; ///< owning session (not owned); null when moved-from
    std::uint32_t rid_ = 0;
};

} // namespace zenoh

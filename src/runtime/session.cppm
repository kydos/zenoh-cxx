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
    /// The key expression is not usable for the operation asked of it.
    ///
    ///  - `declare_computation`: it is not a *concrete*, canonical key. A Computation
    ///    is a computation registered at one key, so a wild key expression
    ///    (`robot/*/reset`, `math/**`) has no meaning on that side of the abstraction
    ///    -- it is what an `Evaluator` supplies. Non-canonical keys (`a//b`,
    ///    `a/**/**/b`) are rejected here too.
    ///  - `eval`/`declare_evaluator`: it is not canonical. Required (though `get` does
    ///    not require it) because the key expression is prefixed internally, and
    ///    prefixing must preserve it rather than splice it into something malformed.
    ///  - any of the above, plus `get`/`declare_queryable`: it names the reserved
    ///    Evaluation namespace (a first chunk of `@eval`). That reservation is what
    ///    stops an ordinary query reaching a `Computation` by naming its wire key
    ///    literally, and stops an ordinary `Queryable` registering where evals route.
    invalid_key_expr,
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

/// The publication settings a `Publisher` fixes once, at declaration time, and then
/// applies to every `put`/`del` made through it — the same two knobs `PutOptions`
/// carries per message, hoisted to the declaration (as in `zenoh-rust`, whose
/// `Publisher` likewise owns its QoS rather than taking it per publication).
///
/// Priority and express are deliberately absent, for the same reason they are absent
/// from `PutOptions` — see `docs/RUNTIME.md`.
struct PublisherOptions {
    /// See `PutOptions::target_zid`. Applies to every publication from this publisher.
    std::optional<PeerId> target_zid{};
    /// See `PutOptions::congestion`. Applies to every publication from this publisher.
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

// --- Evaluation: Computation / Evaluator / Eval ----------------------------------
//
// Evaluation is a second, deliberately different abstraction over the same
// Query/Reply transport (docs/RUNTIME.md "Evaluation"):
//
//     Querier(key expr) --get()---> Queryable(key expr) --> Reply
//     Evaluator(key expr) --eval(argument)--> Computation(key) --> Reply
//
// A `Queryable` answers queries over a *region* of the key space; a `Computation`
// is one computation registered at one *concrete* key, and `Evaluator::eval`
// invokes every Computation whose key matches its key expression -- always all of
// them, never a "best" one, because a computation may have side effects and
// picking one arbitrarily is not a meaningful thing to do to `robot/*/reset`.
//
// The two abstractions are mutually isolated: an eval never reaches an ordinary
// `Queryable`, and an ordinary `get()` never reaches a `Computation`. That is
// enforced by mapping every Computation onto the Zenoh-reserved `@eval/...`
// namespace on the wire -- a verbatim chunk (`zenoh.ke`), so not even `get("**")`
// intersects it -- and by reserving that namespace at the API boundary, so a caller
// cannot reach into it by naming it literally either (`get`/`declare_queryable`
// return `invalid_key_expr` for it). The mapping is entirely internal and never
// appears in this API: `Eval::key_expr()`, `Eval::computation_key()` and every reply
// key are the logical, application-level ones.

/// Per-computation delivery tuning. Mirrors `QueryableOptions` minus two knobs:
///
///  - `complete` is a data-query notion (does this queryable hold the whole answer
///    for its region?) with no meaning for a computation, so the Evaluation API
///    does not expose it (a Computation is always declared incomplete).
///  - `mode` is fixed to `StrandMode::ordered`. Last-value conflation drops an
///    already-queued entry when a newer one arrives on the same key, which is
///    exactly wrong for something that may reset a robot or claim a job: an eval
///    that reached this session must either run or be finalized, never be
///    silently superseded.
struct ComputationOptions {
    std::size_t capacity = 256;
};

/// One inbound evaluation delivered to a declared `Computation`'s handler or via
/// `Computation::recv()` -- the Evaluation-side counterpart of `IncomingQuery`.
///
/// Move-only, RAII: the destructor releases this eval's share of the underlying
/// request, and the request's `ResponseFinal` goes out once *every* Computation
/// this session delivered it to is done with it. (One `Request` reaches a session
/// however many of its Computations match, so finalizing per `Eval` would cut the
/// evaluator's reply stream short while sibling computations were still working.)
///
/// Not modeled, following this runtime's existing scope rather than by choice: an
/// eval's encoding and attachment (this runtime has no public `Encoding` or
/// attachment concept at all — see `Publisher`'s note), and `reply_del`, which is
/// deliberate: a deletion is a data-centric notion with no meaning as the result of
/// evaluating a computation.
class Eval {
  public:
    Eval(const Eval&) = delete;
    auto operator=(const Eval&) -> Eval& = delete;
    Eval(Eval&& other) noexcept;
    auto operator=(Eval&& other) noexcept -> Eval&;
    ~Eval();

    /// The argument the `Evaluator` supplied (empty if it supplied an empty one).
    [[nodiscard]] auto argument() const noexcept -> std::span<const std::byte> { return argument_; }
    /// The key expression the evaluation was issued on -- the logical one
    /// (`robot/*/reset`), never the internal wire namespace.
    [[nodiscard]] auto key_expr() const noexcept -> std::string_view { return key_; }
    /// The concrete key of the Computation currently processing this eval
    /// (`robot/r1/reset`). Always a match of `key_expr()`.
    [[nodiscard]] auto computation_key() const noexcept -> std::string_view {
        return computation_key_;
    }

    /// Send one successful reply, keyed by `computation_key()` -- so the evaluator
    /// can tell which computation produced which result. Callable multiple times
    /// (0..N replies per eval, none of them consolidated).
    ///
    /// Unlike `IncomingQuery::reply`, no key is passed: a Computation *has* one
    /// concrete key, so there is nothing for the caller to choose or to get wrong.
    [[nodiscard]] auto reply(std::span<const std::byte> value) -> std::expected<void, ZError>;
    /// Send an error reply (`Response{Err}`). Also callable multiple times.
    [[nodiscard]] auto reply_err(std::span<const std::byte> error) -> std::expected<void, ZError>;

  private:
    friend class Session;
    Eval(Session* session, std::uint32_t rid, std::string key, std::string computation_key,
         std::vector<std::byte> argument) noexcept
        : session_(session), rid_(rid), key_(std::move(key)),
          computation_key_(std::move(computation_key)), argument_(std::move(argument)) {}

    Session* session_ = nullptr; ///< owning session (not owned); null when moved-from
    std::uint32_t rid_ = 0;
    std::string key_;             ///< the evaluator's key expression (logical)
    std::string computation_key_; ///< this computation's concrete key
    std::vector<std::byte> argument_;
};

/// A callback invoked by `run()`/`run_once()` for each incoming eval.
using EvalHandler = std::function<void(Eval)>;

/// Options for `Session::eval` / `Session::declare_evaluator`, and so for every
/// `Evaluator::eval` made through the declared handle.
///
/// Deliberately *without* `target` and `consolidation`: an eval always uses
/// `QueryTarget::All` (every matching Computation registration runs) and
/// `ConsolidationMode::None` (every reply reaches the caller). Those two are the
/// contract of Evaluation, not tuning knobs, so there is nothing to set.
///
/// Priority and express are absent for the ordinary reason they are absent from
/// `PutOptions`/`GetOptions` — this runtime does not model them — as are the
/// reference's `matching_status()`/`matching_listener()`, which `Publisher` lacks for
/// the same reason (see `docs/RUNTIME.md`).
struct EvalOptions {
    /// Reply-collection deadline in milliseconds; unset uses the same 10 s default
    /// as `get`. Client-enforced, exactly as `GetOptions::timeout_ms`.
    std::optional<std::uint32_t> timeout_ms{};
    /// Broker-enforced filter narrowing which peer's Computations may be reached.
    /// Narrows normal key matching, never bypasses it (see `GetOptions::target_zid`).
    std::optional<PeerId> target_zid{};
    /// Whether the eval request may be dropped under congestion. Note that the
    /// default (`drop`) is right for a computation whose result supersedes the last
    /// one, and wrong for one that actuates something -- see `CongestionControl`.
    CongestionControl congestion = CongestionControl::drop;
};

class Batch;
class Publisher;
class Subscriber;
struct SubReg; // defined in session.cpp (holds the non-movable Strand + handler)
class Queryable;
struct QblReg; // defined in session.cpp
class Computation;
struct CompReg; // defined in session.cpp
class Evaluator;
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

/// Out-of-line deleter for `comps_`'s `unique_ptr<CompReg>` entries, for exactly the
/// reason `GetRegDeleter` exists (a *container* of `unique_ptr<Incomplete>` is what
/// trips libstdc++'s eager `default_delete` instantiation, not a bare member).
struct CompRegDeleter {
    auto operator()(CompReg* p) const noexcept -> void;
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

    /// Declare a publisher on `key_expr`: bind the key expression to a numeric id on
    /// this session's link (`Frame(Declare{DeclareKeyExpr})`) and announce the
    /// publication (`Frame(Interest)`), then return a handle whose `put`/`del` send
    /// the *id* instead of the text — so a long key expression costs one or two bytes
    /// per message rather than its own length. This is what makes a publisher worth
    /// declaring over calling `Session::put` in a loop; everything else about the
    /// data path is identical.
    ///
    /// `opts` fixes the QoS for every publication made through the handle. Unlike
    /// subscribers and queryables there is no per-session limit: publishers hold no
    /// receive-side state, and two on the same key expression share one declared id.
    /// The session must outlive the returned publisher.
    [[nodiscard]] auto declare_publisher(std::string_view key_expr, PublisherOptions opts = {})
        -> std::expected<Publisher, ZError>;

    /// Issue a query on `key_expr` (`parameters` is an opaque, caller-defined query
    /// string) and pull replies with `Getter::recv()` until it returns `nullopt`
    /// (the query completed normally) or an error (incl. `query_timeout`). A key
    /// expression naming the reserved Evaluation namespace is refused
    /// (`invalid_key_expr`): a query is answered by `Queryable`s, never by a
    /// `Computation` — use `eval` for those.
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
    /// The session must outlive the returned queryable. A key expression naming the
    /// reserved Evaluation namespace is refused (`invalid_key_expr`) — see `ZError`.
    [[nodiscard]] auto declare_queryable(std::string_view key_expr, QueryableOptions opts = {})
        -> std::expected<Queryable, ZError>;

    /// Declare a queryable whose `on_query` callback is invoked by `run()`/`run_once()`
    /// for each incoming query.
    [[nodiscard]] auto declare_queryable(std::string_view key_expr, QueryHandler on_query,
                                         QueryableOptions opts = {})
        -> std::expected<Queryable, ZError>;

    /// Declare a computation registered at the concrete key `key`, reachable only
    /// through `eval` (never through `get`). Without a handler the computation is
    /// pull-based — drive it with `Computation::recv`.
    ///
    /// `key` must be a concrete, canonical key expression: `robot/r1/reset`, not
    /// `robot/*/reset` (`ZError::invalid_key_expr`). Unlike subscribers and
    /// queryables there is no per-session limit, and two Computations may share one
    /// key — both then run on every matching eval (see `Session::eval`). The session
    /// must outlive the returned computation.
    [[nodiscard]] auto declare_computation(std::string_view key, ComputationOptions opts = {})
        -> std::expected<Computation, ZError>;

    /// Declare a computation whose `on_eval` callback is invoked by
    /// `run()`/`run_once()` for each incoming eval.
    [[nodiscard]] auto declare_computation(std::string_view key, EvalHandler on_eval,
                                           ComputationOptions opts = {})
        -> std::expected<Computation, ZError>;

    /// Declare an evaluator on `key_expr` (any canonical key expression, wild or not,
    /// outside the reserved Evaluation namespace — `invalid_key_expr` otherwise):
    /// binds the key expression to a numeric id on this link, exactly as
    /// `declare_publisher` does, so each `Evaluator::eval` sends the id rather than
    /// the text. That, and fixing the options once, is the whole difference from
    /// calling `Session::eval` in a loop. The session must outlive the evaluator.
    [[nodiscard]] auto declare_evaluator(std::string_view key_expr, EvalOptions opts = {})
        -> std::expected<Evaluator, ZError>;

    /// Evaluate `argument` on every Computation whose key matches `key_expr`, and
    /// pull the replies with `Getter::recv()` until it returns `nullopt` (every
    /// computation finished) or an error — the undeclared counterpart of
    /// `declare_evaluator`, as `get` is of `Querier` elsewhere.
    ///
    /// Every matching *registration* runs, never a "best" one, and no reply is
    /// consolidated. Each reply is keyed by the concrete key of the computation that
    /// produced it. The argument is mandatory (pass an empty span for a computation
    /// that needs none) because it is the argument of a computation, not data being
    /// stored.
    ///
    /// This is a fan-out over matching registrations and nothing more: it is not
    /// exactly-once, not at-most-once across failures, not idempotent, not
    /// transactional, and not one-execution-per-distinct-key. A computation may have
    /// side effects, so `eval` is never safe to retry blindly.
    ///
    /// `key_expr` must be canonical and must not name the reserved Evaluation
    /// namespace (`invalid_key_expr` otherwise) — see `ZError`.
    [[nodiscard]] auto eval(std::string_view key_expr, std::span<const std::byte> argument,
                            EvalOptions opts = {}) -> std::expected<Getter, ZError>;

    /// Evaluate `argument`, delivering replies to `on_reply` from `run()`/`run_once()`.
    [[nodiscard]] auto eval(std::string_view key_expr, std::span<const std::byte> argument,
                            GetReplyHandler on_reply, EvalOptions opts = {})
        -> std::expected<void, ZError>;

    /// Pump the receive loop once: deliver one batch's worth of progress (decode +
    /// dispatch to the subscriber/queryable/get handlers), send a keepalive if the
    /// link is idle, or report `connection_closed`/`protocol_error`. Returns when it
    /// has made progress or the idle keepalive timer fired. Single-threaded (first cut).
    /// A handler invoked here may start another `get`/`eval` (the "on each reply, ask
    /// the next question" idiom) and may undeclare its own registration: the delivery
    /// loops walk a snapshot of request/registration ids and re-find each entry, so
    /// neither mutation invalidates what this call is iterating.
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
    friend class Publisher;
    friend class Subscriber;
    friend class Queryable;
    friend class IncomingQuery;
    friend class Computation;
    friend class Evaluator;
    friend class Eval;
    friend class Getter;
    Session() = default;

    /// Wrap `msg_bytes` (one or more concatenated network messages) in a FrameHeader
    /// with the current SN, frame it with the 2-byte batch length, and write it all
    /// (blocking). Advances the SN. Flushes any `try_put` backlog first.
    [[nodiscard]] auto write_frame(std::span<const std::byte> msg_bytes)
        -> std::expected<void, ZError>;

    /// The two encoders and the two publish paths all take a key expression already
    /// split into its wire form — a numeric `scope` (0 = none) plus the textual
    /// `suffix` that follows it. `Session::put` passes `(0, key)`; a `Publisher`
    /// passes `(its declared id, "")`, which is the whole point of declaring one.
    [[nodiscard]] auto encode_put(std::uint16_t scope, std::string_view suffix,
                                  std::span<const std::byte> payload, const PutOptions& opts)
        -> std::expected<void, ZError>;
    [[nodiscard]] auto encode_put_head(std::uint16_t scope, std::string_view suffix,
                                       std::span<const std::byte> payload, const PutOptions& opts)
        -> std::expected<std::size_t, ZError>;
    /// Blocking publish of one `Push(Put)` (the body of `put`, and of `Publisher::put`).
    [[nodiscard]] auto put_wire(std::uint16_t scope, std::string_view suffix,
                                std::span<const std::byte> payload, const PutOptions& opts)
        -> std::expected<void, ZError>;
    /// Non-blocking publish of one `Push(Put)` (the body of `try_put`, and of
    /// `Publisher::try_put`).
    [[nodiscard]] auto try_put_wire(std::uint16_t scope, std::string_view suffix,
                                    std::span<const std::byte> payload, const PutOptions& opts)
        -> std::expected<void, ZError>;
    [[nodiscard]] auto flush_pending() -> std::expected<void, ZError>;

    // --- publisher declaration / teardown ---
    /// Bind a numeric id to `key` on this link, sending `Frame(Declare{DeclareKeyExpr})`
    /// the first time it is asked for. Ids are refcounted per key expression, so N
    /// publishers on one key share one declaration (mirroring `zenoh-rust`'s
    /// `local_resources`).
    ///
    /// Returns 0 — a valid outcome, not an error — when no id is available (the
    /// per-session cap is reached, or the declaration could not be written). Scope 0
    /// means "no id" on the wire, so a publisher that gets one simply publishes the
    /// key expression in full, exactly as `Session::put` does.
    [[nodiscard]] auto declare_ke(std::string_view key) -> std::uint16_t;
    /// Drop one reference to `ke_id`, sending `Frame(Declare{UndeclareKeyExpr})` when
    /// the last one goes (best-effort). A no-op for id 0.
    auto undeclare_ke(std::uint16_t ke_id) -> void;
    /// Encode + send a `Frame(Declare{DeclareKeyExpr{id, key}})` (blocking).
    [[nodiscard]] auto write_declare_keyexpr(std::uint16_t id, std::string_view key)
        -> std::expected<void, ZError>;
    /// Encode + send a `Frame(Declare{UndeclareKeyExpr{id}})` (best-effort).
    auto write_undeclare_keyexpr(std::uint16_t id) -> void;
    /// Encode + send the publisher's `Frame(Interest{CurrentFuture, KEYEXPRS|SUBSCRIBERS})`
    /// (blocking) — what a `zenoh-rust` publisher announces on declaration.
    [[nodiscard]] auto write_interest(std::uint32_t id, std::uint16_t scope,
                                      std::string_view suffix) -> std::expected<void, ZError>;
    /// Encode + send a `Frame(InterestFinal{id})`, closing the publisher's interest
    /// (best-effort).
    auto write_interest_final(std::uint32_t id) -> void;
    /// Publish one `Push(Del)` (blocking). For `Publisher::del`.
    [[nodiscard]] auto del_wire(std::uint16_t scope, std::string_view suffix,
                                const PutOptions& opts) -> std::expected<void, ZError>;
    /// Undeclare a publisher: close its interest, then release its key-expression id.
    /// For `Publisher`.
    auto pub_drop(std::uint32_t id, std::uint16_t ke_id) -> void;

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
    /// Read as much of the next batch as the socket has right now.
    ///
    /// Returns the batch's body length once it is complete, `nullopt` when it is still
    /// arriving (progress is kept in `rx_hdr_`/`rx_need_`/`rx_fill_`, so the caller can
    /// go and do something else and come back), or an error for a real failure. Never
    /// blocks.
    [[nodiscard]] auto recv_batch_step() -> std::expected<std::optional<std::size_t>, ZError>;
    /// Walk the received batch from the cursor, decoding and posting each message,
    /// until a strand is full or the batch is exhausted. Returns `false` when it
    /// stopped because a strand was full — the cursor is left where it is, and the
    /// caller must free space (`drain_handlers`, or the application calling `recv`)
    /// before there is any point retrying.
    ///
    /// A batch is a sequence of *transport* messages (Frame, KeepAlive, Close), and a
    /// Frame's body is the run of *network* messages that follows it — which ends at
    /// the first id that is not a network one (`is_network_mid`), not at the end of
    /// the batch. Both kinds are handled by this one loop, so a KeepAlive packed
    /// after a frame is just the next iteration.
    [[nodiscard]] auto dispatch_cursor() -> std::expected<bool, ZError>;
    /// Deliver everything queued for handler-backed subscribers/queryables/gets.
    ///
    /// Callback-style registrations need no application call to drain, so *any* pump
    /// caller can and must drain them — not just `run_once`. Otherwise a full callback
    /// subscriber strand head-of-line blocks the shared receive cursor: a `get()`
    /// pumping for its reply would re-decode the same undeliverable sample forever
    /// (measured: 200k no-progress iterations in 1.2 s) and time out with its reply
    /// already sitting in the receive buffer.
    auto drain_handlers() -> void;
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

    // --- receive path (computation) ---
    /// Post one inbound evaluation to every Computation whose key matches `key`
    /// (the *logical* key expression, already stripped of the internal prefix).
    ///
    /// All-or-nothing: if any matching computation's strand is full, nothing is
    /// posted and this returns `false` so the caller pauses the receive cursor and
    /// retries the whole request later — a partial post would run the computations
    /// that had room twice. Returns `true` (having finalized the request itself)
    /// when no computation matches after all.
    [[nodiscard]] auto deliver_eval(std::uint32_t rid, std::string_view key,
                                    std::span<const std::byte> argument) -> bool;
    /// Release one Computation's share of request `rid`, sending its `ResponseFinal`
    /// once the last one goes. For `Eval`, and for a `Computation` undeclared with
    /// evals still queued.
    auto eval_finish(std::uint32_t rid) -> void;
    /// Take a reference on the shared wire declaration for `key`, sending
    /// `Frame(Declare{DeclareQueryable})` for the internally-namespaced key the first
    /// time it is asked for. Refcounted per key, exactly as `declare_ke` is: several
    /// Computations may share one key, and they must share one declaration.
    [[nodiscard]] auto declare_comp_key(std::string_view key) -> std::expected<void, ZError>;
    /// Drop one reference to `key`'s declaration, sending
    /// `Frame(Declare{UndeclareQueryable})` when the last one goes (best-effort).
    auto undeclare_comp_key(std::string_view key) -> void;
    /// The registration with entity id `id`, or null once it has been undeclared.
    [[nodiscard]] auto find_comp(std::uint32_t id) noexcept -> CompReg*;
    /// Pull the next eval for computation `id` (drives `pump_step`). For `Computation`.
    [[nodiscard]] auto comp_recv(std::uint32_t id) -> std::expected<Eval, ZError>;
    /// Drop computation `id` (after best-effort undeclare), finalizing any evals
    /// still sitting in its strand. For `Computation`.
    auto comp_drop(std::uint32_t id) -> void;

    // --- query path (getter) ---
    /// Encode + send a `Frame(Request{...})` for request id `rid` (blocking). The key
    /// expression is split into `scope`/`suffix` exactly as on the publish path (a
    /// declared `Evaluator` sends its id and an empty suffix; `get`/`eval` send
    /// `(0, text)`), and `payload`, when non-empty, travels as the `Query`'s value —
    /// the eval argument.
    [[nodiscard]] auto write_request(std::uint32_t rid, std::uint16_t scope,
                                     std::string_view suffix, std::string_view parameters,
                                     std::span<const std::byte> payload, const GetOptions& opts)
        -> std::expected<void, ZError>;
    /// Shared body of both `get()` overloads (and, through `start_eval`, of both
    /// `eval()` ones): allocates a request id, sends the `Request`, and registers
    /// `pending_gets_[rid]`.
    [[nodiscard]] auto start_get(std::uint16_t scope, std::string_view suffix,
                                 std::string_view parameters, std::span<const std::byte> payload,
                                 GetReplyHandler handler, const GetOptions& opts)
        -> std::expected<std::uint32_t, ZError>;
    /// `start_get` with the Evaluation contract applied: the internal key namespace,
    /// `QueryTarget::all`, `ConsolidationMode::none`, and replies accepted from any
    /// key expression. For `Session::eval` and `Evaluator::eval`.
    [[nodiscard]] auto start_eval(std::uint16_t scope, std::string_view suffix,
                                  std::span<const std::byte> argument, GetReplyHandler handler,
                                  const EvalOptions& opts) -> std::expected<std::uint32_t, ZError>;
    /// Pull the next reply for `rid` (drives `pump_step`). `nullopt` = query complete.
    /// For `Getter`.
    [[nodiscard]] auto get_recv(std::uint32_t rid)
        -> std::expected<std::optional<GetReply>, ZError>;
    /// The request ids of every callback-style (handler-backed) in-flight request, as
    /// a snapshot. Iterating that snapshot rather than `pending_gets_` itself is what
    /// lets a reply handler start another `get`/`eval` without invalidating the loop
    /// (see `drain_handlers`). Allocates nothing when nothing is in flight.
    [[nodiscard]] auto callback_get_rids() -> std::vector<std::uint32_t>;
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
    std::size_t rx_pos_ = 0;          ///< dispatch cursor into rx_buf_
    std::size_t rx_end_ = 0;          ///< end of the in-progress batch (==pos: none)
    /// Partially-received batch, so the pump can leave one half-arrived and come back.
    ///
    /// Reading a batch used to mean two blocking `read_exact`s. Once the 2-byte length
    /// prefix had been consumed, the session was committed to waiting for the whole
    /// body with no timeout: a peer that announces 100 bytes, sends 5 and stalls
    /// froze the pump — `get()` deadlines went unnoticed, no keepalive went out, and
    /// the router eventually dropped us for lease expiry. Timing out and discarding
    /// the partial read is not an option either: those bytes are gone from the socket,
    /// so the byte stream would desynchronize. Keeping the progress is what makes
    /// returning to the caller safe.
    std::array<std::byte, 2> rx_hdr_{}; ///< length prefix as it arrives
    std::size_t rx_hdr_fill_ = 0;       ///< bytes of `rx_hdr_` received (0..2)
    std::size_t rx_need_ = 0;           ///< body length once the prefix is complete
    std::size_t rx_fill_ = 0;           ///< bytes of the body already in `rx_buf_`
    std::unordered_map<std::uint16_t, std::string> resmap_; ///< router keyexpr id -> key

    /// One key expression this session has declared an id for (see `declare_ke`).
    /// Refcounted, because two publishers on the same key expression share one
    /// declaration and the id may only be released when the second one goes.
    struct KeReg {
        std::uint16_t id = 0;
        std::size_t refs = 0;
    };
    std::unordered_map<std::string, KeReg> ke_by_key_{}; ///< our declared keyexpr ids, by key
    std::unordered_map<std::uint16_t, std::string> ke_by_id_{}; ///< ... and the reverse index
    std::uint16_t next_ke_id_ = 1; ///< id allocation cursor (0 = "no id" on the wire)

    std::optional<ZError> fault_{};     ///< sticky terminal fault (stream desynced)
    std::uint32_t next_entity_id_ = 0;  ///< monotonic subscriber/queryable entity id
    std::int32_t keepalive_ms_ = 2500;  ///< idle keepalive cadence (negotiated lease / 4)
    std::unique_ptr<SubReg> sub_{};     ///< the single active subscriber (first cut)
    std::unique_ptr<QblReg> qbl_{};     ///< the single active queryable (first cut)
    std::uint32_t next_request_id_ = 0; ///< monotonic get() request id
    std::unordered_map<std::uint32_t, std::unique_ptr<GetReg, GetRegDeleter>>
        pending_gets_{}; ///< in-flight get()s
    /// Declared computations. A plain vector: there is no per-session limit and no
    /// key uniqueness (two Computations may share a key), the lookup on the receive
    /// path is a key-expression *match* rather than an exact one, and the counts
    /// involved are small.
    std::vector<std::unique_ptr<CompReg, CompRegDeleter>> comps_{};
    /// One wire declaration shared by every Computation on the same key, refcounted
    /// so the last one to go is what undeclares it (see `declare_comp_key`).
    struct CompDecl {
        std::uint32_t id = 0;
        std::size_t refs = 0;
    };
    /// Transparent hash so a `string_view` key looks up without a temporary `string`
    /// (the same idiom `zenoh.broker.resource` and `Strand` use); `std::equal_to<>` is
    /// the matching transparent comparator.
    struct TransparentStringHash {
        using is_transparent = void;
        [[nodiscard]] auto operator()(std::string_view sv) const noexcept -> std::size_t {
            return std::hash<std::string_view>{}(sv);
        }
    };
    std::unordered_map<std::string, CompDecl, TransparentStringHash, std::equal_to<>> comp_decls_{};
    /// Inbound eval requests still owed a `ResponseFinal`, and how many `Eval`s of
    /// theirs are still outstanding (one entry per in-flight request id, not per
    /// computation).
    std::unordered_map<std::uint32_t, std::size_t> eval_pending_{};
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

/// A handle to a declared publication: one key expression, one fixed set of
/// publication settings, and the numeric id the router bound to that key expression
/// when it was declared.
///
/// `put`/`del` through a publisher are the same wire messages `Session::put` sends,
/// with one difference that is the reason publishers exist: the `Push` carries the
/// declared *id* in place of the key expression text, so publishing on
/// `demo/example/some/long/key` costs a couple of bytes per message instead of thirty.
/// Both are `Push(Put)`/`Push(Del)` on the session's frame SN, so a publisher and a
/// bare `put` may be interleaved freely.
///
/// Created via `Session::declare_publisher`; the originating session must outlive the
/// publisher (like `Batch`/`Subscriber`). Move-only. The destructor undeclares
/// best-effort.
///
/// Not modeled, following this runtime's existing `PutOptions` scope: per-publication
/// encoding, attachments and timestamps, priority/express QoS, and the reference's
/// `matching_status()`/`matching_listener()` (which need the router to answer the
/// publisher's `Interest` with the declarations it matches — v1 routers here decode
/// that query and deliberately do not reply; see `docs/BROKER.md`).
class Publisher {
  public:
    Publisher(const Publisher&) = delete;
    auto operator=(const Publisher&) -> Publisher& = delete;
    Publisher(Publisher&& other) noexcept;
    auto operator=(Publisher&& other) noexcept -> Publisher&;
    ~Publisher();

    /// Publish `payload` on this publisher's key expression, blocking until the whole
    /// message has been handed to the transport. Identical in every respect to
    /// `Session::put(key_expr(), payload, {...})` with this publisher's settings,
    /// except that the key expression travels as its declared id.
    [[nodiscard]] auto put(std::span<const std::byte> payload) -> std::expected<void, ZError>;

    /// Like `put`, but never blocks — same `would_block` / partial-write commit
    /// semantics as `Session::try_put`.
    [[nodiscard]] auto try_put(std::span<const std::byte> payload) -> std::expected<void, ZError>;

    /// Publish a deletion (`Push(Del)`) on this publisher's key expression, blocking.
    /// Subscribers receive a `Sample` with `SampleKind::del` and an empty payload.
    [[nodiscard]] auto del() -> std::expected<void, ZError>;

    /// Undeclare the publisher: close its interest and release its key-expression id
    /// (the latter only once no other publisher on the same key expression holds it).
    /// Idempotent; also run by the destructor. Further `put`s return
    /// `connection_closed`.
    auto undeclare() -> void;

    /// The key expression this publisher was declared on.
    [[nodiscard]] auto key_expr() const noexcept -> std::string_view { return key_; }
    /// The congestion-control setting applied to every publication from this handle.
    [[nodiscard]] auto congestion_control() const noexcept -> CongestionControl {
        return opts_.congestion;
    }
    /// The zid filter applied to every publication from this handle, if any.
    [[nodiscard]] auto target_zid() const noexcept -> const std::optional<PeerId>& {
        return opts_.target_zid;
    }
    /// The numeric id the key expression was declared under, or 0 when it is published
    /// in full (no id could be allocated — see `Session::declare_ke`). Exposed for
    /// tests and diagnostics; it changes nothing an application can observe.
    [[nodiscard]] auto keyexpr_id() const noexcept -> std::uint16_t { return ke_id_; }

  private:
    friend class Session;
    Publisher(Session* session, std::uint32_t id, std::uint16_t ke_id, std::string key,
              PutOptions opts) noexcept
        : session_(session), id_(id), ke_id_(ke_id), key_(std::move(key)), opts_(std::move(opts)) {}

    /// The text half of this publisher's wire key expression: empty once an id is
    /// bound (the id *is* the key expression), the whole key expression otherwise.
    [[nodiscard]] auto wire_suffix() const noexcept -> std::string_view {
        return ke_id_ == 0 ? std::string_view{key_} : std::string_view{};
    }

    Session* session_ = nullptr; ///< owning session (not owned); null when moved-from/undeclared
    std::uint32_t id_ = 0;       ///< entity id, and the id of this publisher's Interest
    std::uint16_t ke_id_ = 0;    ///< declared key-expression id (0 = publish the text)
    std::string key_;            ///< the key expression, owned (the handle outlives the caller's)
    PutOptions opts_{};          ///< the settings every publication from this handle carries
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

/// A handle to a declared computation. Holds a non-owning pointer to its `Session`
/// (the session must outlive it, like `Queryable`). Move-only. The destructor
/// undeclares best-effort. Pull evals with `recv()` (pull-based computations), or
/// let the session's `run()`/`run_once()` invoke the handler (callback computations).
///
/// Undeclaring with evals still queued finalizes them rather than stranding the
/// evaluator waiting for replies that are no longer coming.
class Computation {
  public:
    Computation(const Computation&) = delete;
    auto operator=(const Computation&) -> Computation& = delete;
    Computation(Computation&& other) noexcept;
    auto operator=(Computation&& other) noexcept -> Computation&;
    ~Computation();

    /// Block until the next eval arrives, pumping the session as needed. Returns
    /// `connection_closed` on EOF or `protocol_error` on a malformed stream.
    /// Intended for pull-based (no-handler) computations.
    [[nodiscard]] auto recv() -> std::expected<Eval, ZError>;

    /// Undeclare and stop receiving (sends `Frame(Declare{UndeclareQueryable})` for
    /// the internal registration). Idempotent; also run by the destructor.
    auto undeclare() -> void;

    /// The concrete key this computation is registered at.
    [[nodiscard]] auto key() const noexcept -> std::string_view;

  private:
    friend class Session;
    Computation(Session* session, std::uint32_t id) noexcept : session_(session), id_(id) {}

    Session* session_ = nullptr; ///< owning session (not owned); null when moved-from/undeclared
    std::uint32_t id_ = 0;       ///< entity id of the registration in `Session::comps_`
};

/// A handle to a declared evaluator: one key expression, one fixed set of eval
/// settings, and the numeric id the router bound to the (internally namespaced) key
/// expression when it was declared.
///
/// `eval` through an evaluator is the same wire exchange `Session::eval` performs,
/// with the one difference that makes declaring it worthwhile — the `Request` carries
/// the declared *id* instead of the key expression text, exactly as a `Publisher`'s
/// `put` does. Created via `Session::declare_evaluator`; the originating session must
/// outlive it. Move-only; the destructor releases the key-expression id best-effort.
///
/// There is deliberately no `target()`/`consolidation()`: see `EvalOptions`.
class Evaluator {
  public:
    Evaluator(const Evaluator&) = delete;
    auto operator=(const Evaluator&) -> Evaluator& = delete;
    Evaluator(Evaluator&& other) noexcept;
    auto operator=(Evaluator&& other) noexcept -> Evaluator&;
    ~Evaluator();

    /// Evaluate `argument` on every Computation matching this evaluator's key
    /// expression, and pull the replies with `Getter::recv()`. Identical in every
    /// respect to `Session::eval(key_expr(), argument, {...})` with this evaluator's
    /// settings, except that the key expression travels as its declared id.
    [[nodiscard]] auto eval(std::span<const std::byte> argument) -> std::expected<Getter, ZError>;

    /// Evaluate `argument`, delivering replies to `on_reply` from `run()`/`run_once()`.
    [[nodiscard]] auto eval(std::span<const std::byte> argument, GetReplyHandler on_reply)
        -> std::expected<void, ZError>;

    /// Release the declared key-expression id (only once no other declaration on the
    /// same key expression holds it). Idempotent; also run by the destructor. Further
    /// `eval`s return `connection_closed`.
    auto undeclare() -> void;

    /// The key expression this evaluator was declared on — the logical one, never the
    /// internal wire namespace.
    [[nodiscard]] auto key_expr() const noexcept -> std::string_view { return key_; }
    /// The numeric id the (internally namespaced) key expression was declared under,
    /// or 0 when it is sent in full — see `Publisher::keyexpr_id`, which this mirrors.
    /// Exposed for tests and diagnostics.
    [[nodiscard]] auto keyexpr_id() const noexcept -> std::uint16_t { return ke_id_; }

  private:
    friend class Session;
    Evaluator(Session* session, std::uint16_t ke_id, std::string key, std::string wire_key,
              EvalOptions opts) noexcept
        : session_(session), ke_id_(ke_id), key_(std::move(key)), wire_key_(std::move(wire_key)),
          opts_(std::move(opts)) {}

    /// The text half of the wire key expression: empty once an id is bound (the id
    /// *is* the key expression), the whole internal key expression otherwise.
    [[nodiscard]] auto wire_suffix() const noexcept -> std::string_view {
        return ke_id_ == 0 ? std::string_view{wire_key_} : std::string_view{};
    }

    Session* session_ = nullptr; ///< owning session (not owned); null when moved-from/undeclared
    std::uint16_t ke_id_ = 0;    ///< declared key-expression id (0 = send the text)
    std::string key_;            ///< the logical key expression, owned
    std::string wire_key_;       ///< ... and its internally-namespaced form
    EvalOptions opts_{};         ///< the settings every eval from this handle carries
};

/// A handle to one in-flight `get()`. Holds a non-owning pointer to its `Session`
/// (the session must outlive it, like `Subscriber`/`Queryable`). Move-only.
/// Transient: unlike `Subscriber`/`Queryable` there is no wire "undeclare" for a
/// `get()`, so dropping a `Getter` just stops local bookkeeping for its request id.
/// Also the reply handle of `Session::eval`/`Evaluator::eval` — an evaluation
/// collects ordinary `GetReply`s, each keyed by the Computation that produced it.
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
    friend class Evaluator; // `Evaluator::eval` hands back a Getter of its own
    Getter(Session* session, std::uint32_t rid) noexcept : session_(session), rid_(rid) {}

    Session* session_ = nullptr; ///< owning session (not owned); null when moved-from
    std::uint32_t rid_ = 0;
};

} // namespace zenoh

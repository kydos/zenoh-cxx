module;

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
export import zenoh.runtime.strand; // StrandMode is part of the subscriber API

// The user-facing API (PLAN.md D8): a vertically-integrated client `Session` that
// opens a TCP transport to a Zenoh router, publishes data (put/batch), and receives
// it (declare_subscriber + the run()/run_once() pump). It owns the link, the protocol
// state (zid, frame SN, keyexpr resmap), and the encode/decode buffers, and drives
// encode->send / recv->decode->dispatch directly — no sans-IO state machine.
export namespace zenoh {

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

class Batch;
class Subscriber;
struct SubReg; // defined in session.cpp (holds the non-movable Strand + handler)

/// A client session to a single Zenoh router over TCP.
///
/// Lifecycle: `open()` performs the 4-way transport handshake and returns a ready
/// session; `put`/`try_put`/`batch` publish; `declare_subscriber` + `run()`/`run_once()`
/// (callback) or `Subscriber::recv()` (pull) receive; the destructor closes the link.
/// Move-only. NOTE (first cut): the receive path is single-threaded — do not pump from
/// one thread while another publishes; serialize calls.
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

    /// Publish `payload` to `key_expr`, blocking until the whole message has been
    /// handed to the transport. Any bytes left pending by a prior `try_put` are
    /// flushed first (also blocking).
    [[nodiscard]] auto put(std::string_view key_expr, std::span<const std::byte> payload)
        -> std::expected<void, ZError>;

    /// Like `put`, but never blocks. Returns `ZError::would_block` only when the
    /// transport could not accept *any* bytes of the message right now (nothing was
    /// sent, the SN is untouched). If a partial write succeeds, the unsent tail is
    /// buffered and flushed on the next call, and `try_put` returns success — the
    /// message is committed, just not fully drained yet.
    [[nodiscard]] auto try_put(std::string_view key_expr, std::span<const std::byte> payload)
        -> std::expected<void, ZError>;

    /// Open an API-level batch bound to this session. Put operations on the batch
    /// accumulate into a single Frame and are sent as one TCP batch when it fills,
    /// on `Batch::flush()`, or when the batch is destroyed. The session must outlive
    /// any batch created from it.
    [[nodiscard]] auto batch() -> Batch;

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

    /// Pump the receive loop once: deliver one batch's worth of progress (decode +
    /// dispatch to the subscriber's handler), send a keepalive if the link is idle, or
    /// report `connection_closed`/`protocol_error`. Returns when it has made progress
    /// or the idle keepalive timer fired. Single-threaded (first cut).
    [[nodiscard]] auto run_once() -> std::expected<void, ZError>;

    /// Pump `run_once()` in a loop until the connection closes or the stream faults.
    /// Returns the terminal error (`connection_closed` on a clean EOF).
    [[nodiscard]] auto run() -> std::expected<void, ZError>;

    /// Send a Close and tear down the link. Idempotent.
    auto close() -> void;

  private:
    friend class Batch;
    friend class Subscriber;
    Session() = default;

    /// Wrap `msg_bytes` (one or more concatenated network messages) in a FrameHeader
    /// with the current SN, frame it with the 2-byte batch length, and write it all
    /// (blocking). Advances the SN. Flushes any `try_put` backlog first.
    [[nodiscard]] auto write_frame(std::span<const std::byte> msg_bytes)
        -> std::expected<void, ZError>;

    [[nodiscard]] auto encode_put(std::string_view key_expr, std::span<const std::byte> payload)
        -> std::expected<void, ZError>;
    [[nodiscard]] auto encode_put_head(std::string_view key_expr,
                                       std::span<const std::byte> payload)
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
    /// One step of decode/dispatch: posts decoded samples into the subscriber strand.
    [[nodiscard]] auto pump_step() -> std::expected<void, ZError>;
    /// Decode + post network messages from the in-progress frame cursor until the
    /// strand is full or the frame is exhausted.
    [[nodiscard]] auto dispatch_cursor() -> std::expected<void, ZError>;
    /// Resolve a received `WireExpr` to an owned key string (via the resmap).
    [[nodiscard]] auto resolve_key(const WireExpr& we) -> std::expected<std::string, ZError>;
    /// Pull the next sample (drives `pump_step` until the strand yields). For `Subscriber`.
    [[nodiscard]] auto sub_recv() -> std::expected<Sample, ZError>;
    /// Drop the active subscriber registration (after best-effort undeclare). For `Subscriber`.
    auto sub_drop() -> void;

    TcpLink link_{};
    std::uint32_t frame_sn_ = 0;          ///< next reliable-frame SN (mod resolution mask)
    std::uint16_t batch_size_ = 0xffff;   ///< negotiated max TCP batch (incl. 2-byte length prefix)
    std::vector<std::byte> tx_scratch_{}; ///< reusable encode buffer (framed batch)
    std::vector<std::byte> tx_pending_{}; ///< bytes encoded but not yet fully written
    std::size_t pending_off_ = 0;         ///< how much of tx_pending_ is already sent

    std::vector<std::byte> rx_buf_{}; ///< current received TCP batch
    std::size_t rx_pos_ = 0;          ///< dispatch cursor into rx_buf_ (frame body)
    std::size_t rx_end_ = 0;          ///< end of the in-progress frame body (==pos: none)
    std::unordered_map<std::uint16_t, std::string> resmap_; ///< router keyexpr id -> key
    std::optional<ZError> fault_{};    ///< sticky terminal fault (stream desynced)
    std::uint32_t next_entity_id_ = 0; ///< monotonic subscriber/entity id
    std::int32_t keepalive_ms_ = 2500; ///< idle keepalive cadence (negotiated lease / 4)
    std::unique_ptr<SubReg> sub_{};    ///< the single active subscriber (first cut)
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
    [[nodiscard]] auto put(std::string_view key_expr, std::span<const std::byte> payload)
        -> std::expected<void, ZError>;

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

} // namespace zenoh

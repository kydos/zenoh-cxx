module;

#include <cstddef>
#include <cstdint>
#include <expected>
#include <span>
#include <string_view>
#include <vector>

export module zenoh.session;

import zenoh.proto;
import zenoh.runtime.tcp;

// The first slice of the user-facing API (PLAN.md D8): a vertically-integrated
// client `Session` that opens a TCP transport to a Zenoh router and publishes data.
// It owns the link, the protocol state (zid, frame SN), and the encode buffers, and
// drives encode->send directly — no sans-IO state machine in between.
export namespace zenoh {

/// Outcome of a session operation. `would_block` is the distinguishing case for
/// `try_put`: the transport could not accept the message without blocking, leaving
/// the decision (drop, retry, back off) to the caller.
enum class ZError : std::uint8_t {
    would_block,       ///< `try_put`: transport not writable right now (backpressure).
    connection_closed, ///< Peer closed the connection / EOF.
    io_error,          ///< Underlying socket/syscall failure.
    protocol_error,    ///< Router sent an unexpected or malformed handshake reply.
    encode_error,      ///< Message could not be encoded (e.g. exceeds the batch size).
    bad_endpoint,      ///< Endpoint string could not be parsed or resolved.
};

class Batch;

/// A client session to a single Zenoh router over TCP.
///
/// Lifecycle: `open()` performs the 4-way transport handshake and returns a ready
/// session; `put`/`try_put` publish samples; the destructor closes the link.
/// Move-only (owns the socket and encode buffers).
class Session {
public:
    Session(const Session&) = delete;
    auto operator=(const Session&) -> Session& = delete;
    Session(Session&&) noexcept = default;
    auto operator=(Session&&) noexcept -> Session& = default;
    ~Session() = default;

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

    /// Send a Close and tear down the link. Idempotent.
    auto close() -> void;

private:
    friend class Batch;
    Session() = default;

    /// Wrap `push_bytes` (one or more concatenated `Push` messages) in a FrameHeader
    /// with the current SN, frame it with the 2-byte batch length, and write it all
    /// (blocking). Advances the SN. Flushes any `try_put` backlog first.
    [[nodiscard]] auto write_frame(std::span<const std::byte> push_bytes)
        -> std::expected<void, ZError>;

    /// Encode `Frame(Push(Put(key_expr, payload)))` into `tx_scratch_`, framed with
    /// the 2-byte little-endian batch length, using the current frame SN. The SN is
    /// advanced by the caller only once the frame is committed (sent or buffered).
    [[nodiscard]] auto encode_put(std::string_view key_expr, std::span<const std::byte> payload)
        -> std::expected<void, ZError>;

    /// Try to drain `tx_pending_` without blocking. Returns `would_block` if bytes
    /// remain after the attempt.
    [[nodiscard]] auto flush_pending() -> std::expected<void, ZError>;

    TcpLink link_{};
    std::uint32_t frame_sn_ = 0;            ///< next reliable-frame SN (mod resolution mask)
    std::uint16_t batch_size_ = 0xffff;     ///< negotiated max TCP batch (incl. 2-byte length prefix)
    std::vector<std::byte> tx_scratch_{};   ///< reusable encode buffer (framed batch)
    std::vector<std::byte> tx_pending_{};   ///< bytes encoded but not yet fully written
    std::size_t pending_off_ = 0;           ///< how much of tx_pending_ is already sent
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

    Session* session_ = nullptr;       ///< owning session (not owned); null when moved-from
    std::vector<std::byte> buf_{};     ///< accumulated `Push` bytes
    std::size_t body_len_ = 0;         ///< valid prefix of buf_
    std::size_t count_ = 0;            ///< buffered Put operations
};

} // namespace zenoh

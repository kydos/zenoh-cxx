module;

#include <cstddef>
#include <cstdint>
#include <expected>
#include <span>
#include <string>

export module zenoh.runtime.tcp;

// A thin, blocking-or-non-blocking POSIX TCP link (Linux/macOS). This is the I/O
// seam of the vertically-integrated runtime (PLAN.md D8): the session owns one of
// these and drives encode->send / recv->decode directly. POSIX headers stay in the
// implementation unit (tcp.cpp) — none leak through this interface.
export namespace zenoh {

/// Low-level transport I/O outcome.
enum class IoError : std::uint8_t {
    would_block, ///< Non-blocking op cannot proceed right now (EAGAIN/EWOULDBLOCK).
    closed,      ///< Peer closed the connection (EOF) or pipe broken.
    failed,      ///< A syscall failed (connect/resolve/socket error).
};

/// Owning RAII handle for a connected TCP socket. Move-only.
class TcpLink {
public:
    TcpLink() noexcept = default;
    TcpLink(const TcpLink&) = delete;
    auto operator=(const TcpLink&) -> TcpLink& = delete;
    TcpLink(TcpLink&& other) noexcept : fd_(other.fd_) { other.fd_ = -1; }
    auto operator=(TcpLink&& other) noexcept -> TcpLink&;
    ~TcpLink();

    /// Resolve `host` and connect to `host:port` (blocking). TCP_NODELAY is set.
    [[nodiscard]] static auto connect(const std::string& host, std::uint16_t port) noexcept
        -> std::expected<TcpLink, IoError>;

    /// Switch the socket to non-blocking mode (used after the handshake).
    [[nodiscard]] auto set_nonblocking() noexcept -> std::expected<void, IoError>;

    /// Write all of `data`, blocking as needed (polls POLLOUT on EAGAIN). Suitable
    /// for the handshake and for the blocking `put`.
    [[nodiscard]] auto write_all(std::span<const std::byte> data) noexcept
        -> std::expected<void, IoError>;

    /// Write all of `first` then `second` as one scatter-gather sequence (`writev`),
    /// blocking as needed. Lets a caller emit a header plus a borrowed payload without
    /// first copying them into one contiguous buffer.
    [[nodiscard]] auto writev_all(std::span<const std::byte> first,
                                  std::span<const std::byte> second) noexcept
        -> std::expected<void, IoError>;

    /// Write as much of `data` as the socket accepts right now without blocking;
    /// returns the number of bytes written (may be 0..data.size()). Returns
    /// `would_block` only when nothing at all can be written. For the non-blocking
    /// `try_put` flush path.
    [[nodiscard]] auto write_some(std::span<const std::byte> data) noexcept
        -> std::expected<std::size_t, IoError>;

    /// Read exactly `out.size()` bytes, blocking as needed (polls POLLIN on EAGAIN).
    [[nodiscard]] auto read_exact(std::span<std::byte> out) noexcept
        -> std::expected<void, IoError>;

    /// Wait up to `timeout_ms` for the socket to become readable. Returns `true` if it
    /// is readable, `false` if the timeout elapsed first, or `closed`/`failed` on a
    /// hangup/error. A negative `timeout_ms` waits indefinitely. The seam the receive
    /// pump uses to interleave a keepalive timer with blocking reads.
    [[nodiscard]] auto poll_readable(int timeout_ms) noexcept -> std::expected<bool, IoError>;

    [[nodiscard]] auto valid() const noexcept -> bool { return fd_ >= 0; }

private:
    explicit TcpLink(int fd) noexcept : fd_(fd) {}
    int fd_ = -1;
};

} // namespace zenoh

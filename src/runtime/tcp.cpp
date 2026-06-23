module;

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <expected>
#include <span>
#include <string>
#include <string_view>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

module zenoh.runtime.tcp;

namespace zenoh {
namespace {

/// Map an in-flight `errno` to an IoError (EAGAIN was already handled by callers).
[[nodiscard]] auto errno_to_io(int e) noexcept -> IoError {
    switch (e) {
    case EPIPE:
    case ECONNRESET:
    case ENOTCONN:
    case ESHUTDOWN:
        return IoError::closed;
    default:
        return IoError::failed;
    }
}

/// Block until `fd` is ready for `events` (POLLIN/POLLOUT). Retries on EINTR.
[[nodiscard]] auto wait_ready(int fd, short events) noexcept -> std::expected<void, IoError> {
    for (;;) {
        ::pollfd pfd{.fd = fd, .events = events, .revents = 0};
        int const n = ::poll(&pfd, 1, -1);
        if (n < 0) {
            if (errno == EINTR) continue;
            return std::unexpected(IoError::failed);
        }
        if ((pfd.revents & (POLLERR | POLLHUP | POLLNVAL)) != 0) {
            return std::unexpected(IoError::closed);
        }
        return {};
    }
}

} // namespace

auto TcpLink::operator=(TcpLink&& other) noexcept -> TcpLink& {
    if (this != &other) {
        if (fd_ >= 0) ::close(fd_);
        fd_ = other.fd_;
        other.fd_ = -1;
    }
    return *this;
}

TcpLink::~TcpLink() {
    if (fd_ >= 0) ::close(fd_);
}

auto TcpLink::connect(std::string_view host, std::uint16_t port) noexcept
    -> std::expected<TcpLink, IoError> {
    std::string const host_s(host);
    std::string const port_s = std::to_string(port);

    ::addrinfo hints{};
    hints.ai_family = AF_UNSPEC;     // IPv4 or IPv6
    hints.ai_socktype = SOCK_STREAM; // TCP
    hints.ai_protocol = IPPROTO_TCP;

    ::addrinfo* res = nullptr;
    if (::getaddrinfo(host_s.c_str(), port_s.c_str(), &hints, &res) != 0 || res == nullptr) {
        return std::unexpected(IoError::failed);
    }

    int fd = -1;
    for (::addrinfo* ai = res; ai != nullptr; ai = ai->ai_next) {
        fd = ::socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
        if (fd < 0) continue;
        if (::connect(fd, ai->ai_addr, ai->ai_addrlen) == 0) break;
        ::close(fd);
        fd = -1;
    }
    ::freeaddrinfo(res);

    if (fd < 0) return std::unexpected(IoError::failed);

    int one = 1;
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

    return TcpLink(fd);
}

auto TcpLink::set_nonblocking() noexcept -> std::expected<void, IoError> {
    int const flags = ::fcntl(fd_, F_GETFL, 0);
    if (flags < 0) return std::unexpected(IoError::failed);
    if (::fcntl(fd_, F_SETFL, flags | O_NONBLOCK) < 0) return std::unexpected(IoError::failed);
    return {};
}

auto TcpLink::write_all(std::span<const std::byte> data) noexcept -> std::expected<void, IoError> {
    std::size_t off = 0;
    while (off < data.size()) {
        ssize_t const n = ::send(fd_, data.data() + off, data.size() - off, MSG_NOSIGNAL);
        if (n > 0) {
            off += static_cast<std::size_t>(n);
            continue;
        }
        if (n == 0) return std::unexpected(IoError::closed);
        if (errno == EINTR) continue;
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            if (auto r = wait_ready(fd_, POLLOUT); !r) return std::unexpected(r.error());
            continue;
        }
        return std::unexpected(errno_to_io(errno));
    }
    return {};
}

auto TcpLink::write_some(std::span<const std::byte> data) noexcept
    -> std::expected<std::size_t, IoError> {
    std::size_t off = 0;
    while (off < data.size()) {
        ssize_t const n = ::send(fd_, data.data() + off, data.size() - off, MSG_NOSIGNAL);
        if (n > 0) {
            off += static_cast<std::size_t>(n);
            continue;
        }
        if (n == 0) return std::unexpected(IoError::closed);
        if (errno == EINTR) continue;
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            if (off == 0) return std::unexpected(IoError::would_block);
            break; // partial progress: report what we wrote
        }
        return std::unexpected(errno_to_io(errno));
    }
    return off;
}

auto TcpLink::read_exact(std::span<std::byte> out) noexcept -> std::expected<void, IoError> {
    std::size_t off = 0;
    while (off < out.size()) {
        ssize_t const n = ::recv(fd_, out.data() + off, out.size() - off, 0);
        if (n > 0) {
            off += static_cast<std::size_t>(n);
            continue;
        }
        if (n == 0) return std::unexpected(IoError::closed);
        if (errno == EINTR) continue;
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            if (auto r = wait_ready(fd_, POLLIN); !r) return std::unexpected(r.error());
            continue;
        }
        return std::unexpected(errno_to_io(errno));
    }
    return {};
}

} // namespace zenoh

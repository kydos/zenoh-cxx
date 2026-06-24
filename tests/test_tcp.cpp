// Direct tests for the TCP link (zenoh.runtime.tcp) against a controlled loopback
// peer, exercising the paths the Session integration test doesn't reach on its own:
// scatter-gather writes (incl. empty-span fallbacks), non-blocking would_block,
// multi-recv reads, and peer-EOF detection.
import zenoh.runtime.tcp;

#include "ztest.hpp"

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <span>
#include <thread>
#include <vector>

#include <arpa/inet.h>
#include <csignal>
#include <cerrno>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

using namespace zenoh;

namespace {

// A loopback listener on an ephemeral port; `rcvbuf` (if > 0) shrinks the accepted
// socket's receive buffer so a non-reading peer fills the window quickly.
auto make_listener(std::uint16_t& port, int rcvbuf = 0) -> int {
    std::signal(SIGPIPE, SIG_IGN);
    int lf = ::socket(AF_INET, SOCK_STREAM, 0);
    int one = 1;
    ::setsockopt(lf, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    if (rcvbuf > 0) ::setsockopt(lf, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    ::bind(lf, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    ::listen(lf, 1);
    socklen_t len = sizeof(addr);
    ::getsockname(lf, reinterpret_cast<sockaddr*>(&addr), &len);
    port = ntohs(addr.sin_port);
    return lf;
}

auto read_n(int fd, std::span<std::byte> out) -> bool {
    std::size_t off = 0;
    while (off < out.size()) {
        ssize_t const n = ::recv(fd, out.data() + off, out.size() - off, 0);
        if (n > 0) {
            off += static_cast<std::size_t>(n);
            continue;
        }
        if (n < 0 && errno == EINTR) continue;
        return false;
    }
    return true;
}

auto byte(int v) -> std::byte { return static_cast<std::byte>(v); }
auto i(std::byte b) -> int { return std::to_integer<int>(b); }

} // namespace

TEST("TcpLink::read_exact reports closed on peer EOF") {
    std::uint16_t port = 0;
    int const lf = make_listener(port);
    auto link = TcpLink::connect("127.0.0.1", port);
    CHECK(link.has_value());
    int const peer = ::accept(lf, nullptr, nullptr);
    CHECK(peer >= 0);

    ::close(peer); // peer hangs up
    std::array<std::byte, 4> buf{};
    auto r = link->read_exact(buf);
    CHECK(!r.has_value() && r.error() == IoError::closed);

    ::close(lf);
}

TEST("TcpLink::writev_all gathers two spans and handles empty spans") {
    std::uint16_t port = 0;
    int const lf = make_listener(port);
    auto link = TcpLink::connect("127.0.0.1", port);
    CHECK(link.has_value());
    int const peer = ::accept(lf, nullptr, nullptr);
    CHECK(peer >= 0);

    std::array<std::byte, 3> a{byte(1), byte(2), byte(3)};
    std::array<std::byte, 2> b{byte(4), byte(5)};

    // Both non-empty -> genuine scatter-gather.
    CHECK(link->writev_all(a, b).has_value());
    std::array<std::byte, 5> got{};
    CHECK(read_n(peer, got));
    CHECK(i(got[0]) == 1 && i(got[2]) == 3 && i(got[4]) == 5);

    // Empty second -> falls back to write_all(first).
    CHECK(link->writev_all(a, {}).has_value());
    std::array<std::byte, 3> g3{};
    CHECK(read_n(peer, g3));
    CHECK(i(g3[2]) == 3);

    // Empty first -> falls back to write_all(second).
    CHECK(link->writev_all({}, b).has_value());
    std::array<std::byte, 2> g2{};
    CHECK(read_n(peer, g2));
    CHECK(i(g2[1]) == 5);

    ::close(peer);
    ::close(lf);
}

TEST("TcpLink::write_some yields would_block when the send buffer is full") {
    std::uint16_t port = 0;
    int const lf = make_listener(port, /*rcvbuf=*/2048); // tiny peer window
    auto link = TcpLink::connect("127.0.0.1", port);
    CHECK(link.has_value());
    int const peer = ::accept(lf, nullptr, nullptr); // never read -> window fills
    CHECK(peer >= 0);

    CHECK(link->set_nonblocking().has_value());

    std::vector<std::byte> big(256 * 1024, byte(0xAB));
    bool would_block = false;
    for (int n = 0; n < 256 && !would_block; ++n) {
        auto r = link->write_some(big);
        if (!r) {
            CHECK(r.error() == IoError::would_block);
            would_block = true;
        }
    }
    CHECK(would_block);

    ::close(peer);
    ::close(lf);
}

TEST("TcpLink::read_exact loops across multiple recvs") {
    std::uint16_t port = 0;
    int const lf = make_listener(port);
    auto link = TcpLink::connect("127.0.0.1", port);
    CHECK(link.has_value());
    int const peer = ::accept(lf, nullptr, nullptr);
    CHECK(peer >= 0);

    // Peer dribbles the 4 bytes out in two sends so read_exact must loop.
    std::thread writer([peer] {
        std::array<std::byte, 2> h{byte(1), byte(2)};
        ::send(peer, h.data(), h.size(), 0);
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        std::array<std::byte, 2> t{byte(3), byte(4)};
        ::send(peer, t.data(), t.size(), 0);
    });

    std::array<std::byte, 4> buf{};
    auto r = link->read_exact(buf);
    CHECK(r.has_value());
    CHECK(i(buf[0]) == 1 && i(buf[1]) == 2 && i(buf[2]) == 3 && i(buf[3]) == 4);

    writer.join();
    ::close(peer);
    ::close(lf);
}

TEST("TcpLink connect fails for an unresolvable host") {
    auto r = TcpLink::connect("no.such.host.invalid.", 7447);
    CHECK(!r.has_value() && r.error() == IoError::failed);
}

TEST("TcpLink::poll_readable times out when no data is available") {
    std::uint16_t port = 0;
    int const lf = make_listener(port);
    auto link = TcpLink::connect("127.0.0.1", port);
    CHECK(link.has_value());
    int const peer = ::accept(lf, nullptr, nullptr);
    CHECK(peer >= 0);

    auto r = link->poll_readable(50); // nothing sent -> timeout
    CHECK(r.has_value() && r.value() == false);

    ::close(peer);
    ::close(lf);
}

TEST("TcpLink::poll_readable reports readable once data arrives") {
    std::uint16_t port = 0;
    int const lf = make_listener(port);
    auto link = TcpLink::connect("127.0.0.1", port);
    CHECK(link.has_value());
    int const peer = ::accept(lf, nullptr, nullptr);
    CHECK(peer >= 0);

    // Peer sends a byte after a short delay; poll must wait then report readable.
    std::thread writer([peer] {
        std::this_thread::sleep_for(std::chrono::milliseconds(40));
        std::byte b = byte(0x7);
        ::send(peer, &b, 1, 0);
    });

    auto r = link->poll_readable(2000);
    CHECK(r.has_value() && r.value() == true);
    // The byte is really there: a read does not block and returns it.
    std::array<std::byte, 1> got{};
    CHECK(link->read_exact(got).has_value());
    CHECK(i(got[0]) == 7);

    writer.join();
    ::close(peer);
    ::close(lf);
}

TEST("TcpLink::poll_readable does not time out on peer hangup") {
    std::uint16_t port = 0;
    int const lf = make_listener(port);
    auto link = TcpLink::connect("127.0.0.1", port);
    CHECK(link.has_value());
    int const peer = ::accept(lf, nullptr, nullptr);
    CHECK(peer >= 0);

    ::close(peer); // hang up
    // A hangup is reported as either readable (EOF) or closed — never a timeout.
    auto r = link->poll_readable(2000);
    bool const not_timeout = !r.has_value() || r.value() == true;
    CHECK(not_timeout);
    // Either way, the subsequent read surfaces the closure.
    std::array<std::byte, 1> buf{};
    auto rd = link->read_exact(buf);
    CHECK(!rd.has_value() && rd.error() == IoError::closed);

    ::close(lf);
}

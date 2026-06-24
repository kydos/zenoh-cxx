// Smoke test: the `zenoh.proto` umbrella alone exposes the message types and the
// codec essentials (ByteReader/ByteWriter, CodecError) needed to use them.
import zenoh.proto;

#include "ztest.hpp"

#include <array>
#include <cstddef>
#include <span>
#include <variant>

using namespace zenoh;

TEST("zenoh.proto umbrella exposes messages + buffers + error type") {
    std::array<std::byte, 3> pl{std::byte{1}, std::byte{2}, std::byte{3}};
    Push p{};               // from zenoh.proto.network
    p.payload.body = Put{}; // Put + PushBody variant
    std::get<Put>(p.payload.body).payload = pl;

    std::array<std::byte, 64> buf{};
    ByteWriter w{buf}; // from zenoh.buffer (re-exported)
    CHECK(p.encode(w).has_value());

    ByteReader r{std::span<const std::byte>{buf.data(), w.written()}};
    auto decoded = Push::decode(r);
    CHECK(decoded.has_value());
    if (decoded) CHECK(*decoded == p);

    // CodecError (from zenoh.util) is reachable through the umbrella too.
    CodecError const e = CodecError::malformed;
    CHECK(e == CodecError::malformed);
}

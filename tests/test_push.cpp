import zenoh.proto.network;
import zenoh.proto.fields;
import zenoh.proto.exts;
import zenoh.buffer;
import zenoh.util;

#include "ztest.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string_view>

using namespace zenoh;

namespace {

template <class T> auto roundtrip(const T& value) -> void {
    std::array<std::byte, 512> buf{};
    ByteWriter w{buf};
    CHECK(value.encode(w).has_value());

    ByteReader r{std::span<const std::byte>{buf.data(), w.written()}};
    auto decoded = T::decode(r);
    CHECK(decoded.has_value());
    if (decoded) {
        CHECK(*decoded == value);
        CHECK(r.remaining() == 0);
    }
}

} // namespace

TEST("Push minimal (default fields) round-trips") {
    Push p{};
    roundtrip(p);
}

TEST("Push with a keyexpr suffix sets the N flag and round-trips") {
    Push p{};
    p.wire_expr.scope = 7;
    p.wire_expr.mapping = Mapping::sender;
    p.wire_expr.suffix = std::string_view{"demo/example"};
    roundtrip(p);
}

TEST("Push with all extensions (qos, timestamp, nodeid) round-trips") {
    Push p{};
    p.wire_expr.scope = 1;
    p.wire_expr.suffix = std::string_view{"a/b"};
    p.qos.inner = 0b0001'0101; // express, block, priority 5 -> non-default
    p.nodeid.node_id = 42;

    Timestamp ts{};
    ts.time = 1234567;
    ts.id_len = 2;
    ts.id[0] = std::byte{0xAB};
    ts.id[1] = std::byte{0xCD};
    p.timestamp = ts;

    roundtrip(p);
}

TEST("Push carries a Put payload round-trip") {
    std::array<std::byte, 4> pl{std::byte{9}, std::byte{8}, std::byte{7}, std::byte{6}};
    Push p{};
    p.wire_expr.suffix = std::string_view{"k"};
    Put put{};
    put.payload = pl;
    p.payload.body = put;
    roundtrip(p);
}

TEST("Push carries a Del payload round-trip") {
    Push p{};
    p.wire_expr.suffix = std::string_view{"k"};
    Del del{};
    del.timestamp = Timestamp{.time = 42, .id = {}, .id_len = 0};
    del.timestamp->id_len = 1;
    p.payload.body = del;
    roundtrip(p);
}

TEST("Push header carries the right message id and flags") {
    Push p{};
    p.wire_expr.mapping = Mapping::sender;      // M
    p.wire_expr.suffix = std::string_view{"x"}; // N
    p.nodeid.node_id = 1;                       // Z (an extension present)

    std::array<std::byte, 64> buf{};
    ByteWriter w{buf};
    CHECK(p.encode(w).has_value());

    std::uint8_t const h = std::to_integer<std::uint8_t>(buf[0]);
    CHECK((h & 0x1f) == 0x1d); // PUSH id
    CHECK((h & 0x80) != 0);    // Z
    CHECK((h & 0x40) != 0);    // M
    CHECK((h & 0x20) != 0);    // N
}

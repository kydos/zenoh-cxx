import zenoh.proto.transport;
import zenoh.proto.fields;
import zenoh.buffer;
import zenoh.util;

#include "ztest.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>

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

auto make_zid(std::initializer_list<int> bytes) -> ZenohId {
    ZenohId z{};
    z.len = static_cast<std::uint8_t>(bytes.size());
    std::size_t i = 0;
    for (int b : bytes) z.bytes[i++] = static_cast<std::byte>(b);
    return z;
}

} // namespace

TEST("InitAck minimal round-trips") {
    static constexpr std::array<std::byte, 2> cookie{std::byte{0xAA}, std::byte{0xBB}};
    InitAck a{};
    a.version = 9;
    a.identifier = InitIdentifier{.whatami = WhatAmI::router, .zid = make_zid({1, 2, 3, 4})};
    a.cookie = cookie;
    roundtrip(a);
}

TEST("InitAck with non-default resolution sets S and round-trips") {
    static constexpr std::array<std::byte, 3> cookie{std::byte{1}, std::byte{2}, std::byte{3}};
    InitAck a{};
    a.version = 9;
    a.identifier =
        InitIdentifier{.whatami = WhatAmI::peer,
                       .zid = make_zid({0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88})};
    a.resolution = InitResolution{.resolution = 0x05, .batch_size = 256};
    a.cookie = cookie;
    roundtrip(a);
}

TEST("InitAck with a Unit (HasQoS) extension round-trips") {
    static constexpr std::array<std::byte, 1> cookie{std::byte{0x42}};
    InitAck a{};
    a.version = 9;
    a.identifier = InitIdentifier{.whatami = WhatAmI::router, .zid = make_zid({0xFF})};
    a.cookie = cookie;
    a.qos = HasQoS{};
    roundtrip(a);
}

TEST("InitAck with same-id (U64) + zstruct + unit + patch exts round-trips") {
    static constexpr std::array<std::byte, 1> cookie{std::byte{0x01}};
    static constexpr std::array<std::byte, 2> auth_payload{std::byte{0xDE}, std::byte{0xAD}};
    InitAck a{};
    a.version = 9;
    a.identifier =
        InitIdentifier{.whatami = WhatAmI::client,
                       .zid = make_zid({0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15})};
    a.cookie = cookie;
    a.qos_link = QoSLink{0x1234};
    a.auth = Auth{auth_payload};
    a.lowlatency = HasLowLatency{};
    a.patch = Patch{2};
    roundtrip(a);
}

TEST("InitAck rejects a header without the Ack bit") {
    std::array<std::byte, 2> bad{std::byte{0x01}, std::byte{0x00}}; // id ok, A bit clear
    ByteReader r{bad};
    auto d = InitAck::decode(r);
    CHECK(!d.has_value());
}

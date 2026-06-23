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

template <class T>
auto roundtrip(const T& value) -> void {
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

TEST("Close round-trips for both behaviours") {
    roundtrip(Close{.reason = 5, .behaviour = CloseBehaviour::link});
    roundtrip(Close{.reason = 1, .behaviour = CloseBehaviour::session});
}

TEST("KeepAlive round-trips") {
    roundtrip(KeepAlive{});
}

TEST("FrameHeader round-trips (default and with QoS)") {
    roundtrip(FrameHeader{.reliability = Reliability::reliable, .sn = 42, .qos = QoS{}});
    roundtrip(FrameHeader{
        .reliability = Reliability::best_effort, .sn = 1000, .qos = QoS{0b0001'0101}});
}

TEST("InitSyn round-trips (minimal and with extensions)") {
    InitSyn s{};
    s.version = 9;
    s.identifier = InitIdentifier{.whatami = WhatAmI::client, .zid = make_zid({0xCA, 0xFE})};
    roundtrip(s);

    s.qos_link = QoSLink{7};
    s.patch = Patch{1};
    roundtrip(s);
}

TEST("OpenSyn round-trips (seconds lease + same-id mlink exts)") {
    static constexpr std::array<std::byte, 1> cookie{std::byte{0xAA}};
    static constexpr std::array<std::byte, 2> ml{std::byte{0xDE}, std::byte{0xAD}};
    OpenSyn o{};
    o.lease = Duration::from_millis(20000); // 20s -> whole seconds, T set
    o.sn = 99;
    o.cookie = cookie;
    o.mlink_syn = MultiLinkSyn{ml};
    o.lowlatency = HasLowLatency{};
    roundtrip(o);
}

TEST("OpenAck round-trips (millisecond lease, no T flag)") {
    OpenAck o{};
    o.lease = Duration::from_millis(1500); // not whole seconds -> T clear
    o.sn = 3;
    roundtrip(o);
}

TEST("transport decoders reject the wrong message id / ack bit") {
    std::array<std::byte, 2> not_close{std::byte{0x04}, std::byte{0x00}};
    ByteReader r1{not_close};
    CHECK(!Close::decode(r1).has_value());

    // OpenAck requires the Ack bit; a Syn-shaped header (A=0) must be rejected.
    std::array<std::byte, 4> opensyn_shaped{std::byte{0x02}, std::byte{0x0a}, std::byte{0x00},
                                            std::byte{0x00}};
    ByteReader r2{opensyn_shaped};
    CHECK(!OpenAck::decode(r2).has_value());
}

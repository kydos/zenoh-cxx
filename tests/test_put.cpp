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

using namespace zenoh;

namespace {

// Encode `value`, decode it back, and check structural equality + that decode
// consumed exactly the bytes encode produced.
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

TEST("Put with only a payload round-trips") {
    std::array<std::byte, 3> pl{std::byte{1}, std::byte{2}, std::byte{3}};
    Put p{};
    p.payload = pl;
    roundtrip(p);
}

TEST("Put with an empty payload round-trips") {
    Put p{};
    roundtrip(p);
}

TEST("Put with timestamp + encoding round-trips") {
    std::array<std::byte, 4> schema{std::byte{'j'}, std::byte{'s'}, std::byte{'o'}, std::byte{'n'}};
    std::array<std::byte, 2> pl{std::byte{0xAA}, std::byte{0xBB}};

    Timestamp ts{};
    ts.time = 0x0123456789ABCDEFull;
    ts.id_len = 4;
    ts.id[0] = std::byte{9};
    ts.id[1] = std::byte{8};
    ts.id[2] = std::byte{7};
    ts.id[3] = std::byte{6};

    Put p{};
    p.timestamp = ts;
    p.encoding = Encoding{.id = 1234, .has_schema = true, .schema = schema};
    p.payload = pl;
    roundtrip(p);
}

TEST("Put with source-info and attachment extensions round-trips") {
    std::array<std::byte, 5> att{std::byte{1}, std::byte{2}, std::byte{3}, std::byte{4},
                                 std::byte{5}};
    std::array<std::byte, 1> pl{std::byte{0x42}};

    SourceInfo si{};
    si.id.zid = make_zid({0xDE, 0xAD, 0xBE, 0xEF});
    si.id.eid = 7;
    si.sn = 99;

    Put p{};
    p.sinfo = si;
    p.attachment = Attachment{att};
    p.payload = pl;
    roundtrip(p);
}

TEST("Put rejects a wrong message id") {
    std::array<std::byte, 2> bad{std::byte{0x1f}, std::byte{0x00}};
    ByteReader r{bad};
    auto d = Put::decode(r);
    CHECK(!d.has_value());
}

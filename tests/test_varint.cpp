import zenoh.varint;
import zenoh.buffer;
import zenoh.util;

#include "ztest.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <span>

using namespace zenoh;

namespace {

// Encode then decode `x`, reporting the encoded length via `len`.
auto roundtrip(std::uint64_t x, std::size_t& len) -> std::expected<std::uint64_t, CodecError> {
    std::array<std::byte, 16> buf{};
    ByteWriter w{buf};
    if (auto e = encode_vle(w, x); !e) {
        return std::unexpected(e.error());
    }
    len = w.written();
    ByteReader r{std::span<const std::byte>{buf.data(), w.written()}};
    return decode_vle(r);
}

} // namespace

TEST("vle_len matches the 7-bit-per-byte thresholds") {
    CHECK(vle_len(0) == 1);
    CHECK(vle_len(127) == 1);
    CHECK(vle_len(128) == 2);
    CHECK(vle_len(16383) == 2);
    CHECK(vle_len(16384) == 3);
    CHECK(vle_len((1ull << 56) - 1) == 8);
    CHECK(vle_len(1ull << 56) == 9);
    CHECK(vle_len(UINT64_MAX) == 9);
}

TEST("vle round-trips representative values with minimal length") {
    const std::uint64_t values[] = {
        0ull, 1ull, 127ull, 128ull, 300ull, 16384ull,
        (1ull << 35), (1ull << 56) - 1, (1ull << 56), UINT64_MAX,
    };
    for (std::uint64_t x : values) {
        std::size_t len = 0;
        auto v = roundtrip(x, len);
        CHECK(v.has_value());
        CHECK(*v == x);
        CHECK(len == vle_len(x));
    }
}

TEST("vle encodes 300 as 0xAC 0x02") {
    std::array<std::byte, 16> buf{};
    ByteWriter w{buf};
    CHECK(encode_vle(w, 300).has_value());
    CHECK(w.written() == 2);
    CHECK(std::to_integer<unsigned>(buf[0]) == 0xAC);
    CHECK(std::to_integer<unsigned>(buf[1]) == 0x02);
}

TEST("vle never exceeds 9 bytes (UINT64_MAX)") {
    std::array<std::byte, 16> buf{};
    ByteWriter w{buf};
    CHECK(encode_vle(w, UINT64_MAX).has_value());
    CHECK(w.written() == 9);
}

TEST("vle decode of a truncated continuation errors") {
    std::array<std::byte, 1> bad{std::byte{0x80}}; // says "more", but buffer ends
    ByteReader r{bad};
    auto v = decode_vle(r);
    CHECK(!v.has_value());
    CHECK(v.error() == CodecError::src_exhausted);
}

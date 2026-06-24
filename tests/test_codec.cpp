// Direct unit tests for the primitive wire codecs (zenoh.codec): VLE integers,
// narrowed integers, length-prefixed bytes/text, UTF-8 validation, raw bytes, and
// fixed little-endian u16 — including the error/edge paths the message-level tests
// don't reach.
import zenoh.codec;
import zenoh.buffer;
import zenoh.util;

#include "ztest.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>
#include <vector>

using namespace zenoh;

namespace {

// Encode via `fn` into an owned buffer, returning the written bytes.
template <class Fn> auto encode_into(Fn&& fn, std::size_t cap = 64) -> std::vector<std::byte> {
    std::vector<std::byte> buf(cap);
    ByteWriter w{buf};
    CHECK(fn(w).has_value());
    return {buf.data(), buf.data() + w.written()};
}

auto reader(const std::vector<std::byte>& v) -> ByteReader { return ByteReader{v}; }

} // namespace

TEST("put_uint/get_uint round-trips across the VLE range") {
    for (std::uint64_t v : {std::uint64_t{0}, std::uint64_t{1}, std::uint64_t{127},
                            std::uint64_t{128}, std::uint64_t{16383}, std::uint64_t{16384},
                            std::uint64_t{0xffffffffULL}, ~std::uint64_t{0}}) {
        auto const bytes = encode_into([&](ByteWriter& w) { return put_uint(w, v); }, 16);
        CHECK(bytes.size() == len_uint(v));
        auto r = reader(bytes);
        auto got = get_uint(r);
        CHECK(got.has_value() && *got == v);
        CHECK(r.remaining() == 0);
    }
}

TEST("get_uint rejects a truncated VLE") {
    // 0x80 = continuation bit set, but no following byte.
    std::array<std::byte, 1> buf{std::byte{0x80}};
    ByteReader r{buf};
    auto got = get_uint(r);
    CHECK(!got.has_value());
}

TEST("get_uint_as narrows in range and rejects overflow") {
    auto const u16ok = encode_into([&](ByteWriter& w) { return put_uint(w, 0xfffeULL); }, 16);
    {
        auto r = reader(u16ok);
        auto v = get_uint_as<std::uint16_t>(r);
        CHECK(v.has_value() && *v == 0xfffe);
    }
    auto const tooBig = encode_into([&](ByteWriter& w) { return put_uint(w, 0x10000ULL); }, 16);
    {
        auto r = reader(tooBig);
        auto v = get_uint_as<std::uint16_t>(r);
        CHECK(!v.has_value() && v.error() == CodecError::malformed);
    }
    // u8 boundary.
    auto const u8over = encode_into([&](ByteWriter& w) { return put_uint(w, 256ULL); }, 16);
    {
        auto r = reader(u8over);
        CHECK(!get_uint_as<std::uint8_t>(r).has_value());
    }
}

TEST("put_prefixed/get_prefixed round-trip and borrow") {
    std::array<std::byte, 3> payload{std::byte{0xAA}, std::byte{0xBB}, std::byte{0xCC}};
    auto const bytes = encode_into([&](ByteWriter& w) { return put_prefixed(w, payload); });
    CHECK(bytes.size() == len_prefixed(payload));
    auto r = reader(bytes);
    auto got = get_prefixed(r);
    CHECK(got.has_value());
    CHECK(got->size() == 3 && std::to_integer<int>((*got)[0]) == 0xAA);
    CHECK(r.remaining() == 0);
}

TEST("get_prefixed rejects a length that overruns the buffer") {
    // len = 5, but only 1 payload byte follows.
    std::array<std::byte, 2> buf{std::byte{5}, std::byte{1}};
    ByteReader r{buf};
    auto got = get_prefixed(r);
    CHECK(!got.has_value() && got.error() == CodecError::src_exhausted);
}

TEST("put_prefixed reports a full destination") {
    std::array<std::byte, 2> small{};
    ByteWriter w{small}; // 2 bytes: fits len=2 prefix but not the 4 payload bytes
    std::array<std::byte, 4> payload{};
    CHECK(!put_prefixed(w, payload).has_value());
}

TEST("prefixed string round-trips and validates UTF-8") {
    auto const bytes = encode_into([&](ByteWriter& w) { return put_prefixed_str(w, "héllo/ä"); });
    auto r = reader(bytes);
    auto got = get_prefixed_str(r);
    CHECK(got.has_value() && *got == std::string_view{"héllo/ä"});
}

TEST("get_prefixed_str rejects non-UTF-8 as invalid_field") {
    // length 2 then a lone 0xFF 0xFE (invalid UTF-8).
    std::array<std::byte, 3> buf{std::byte{2}, std::byte{0xFF}, std::byte{0xFE}};
    ByteReader r{buf};
    auto got = get_prefixed_str(r);
    CHECK(!got.has_value() && got.error() == CodecError::invalid_field);
}

TEST("is_valid_utf8 accepts well-formed and rejects ill-formed sequences") {
    auto u = [](std::initializer_list<int> bs) {
        std::vector<std::byte> v;
        for (int b : bs) v.push_back(static_cast<std::byte>(b));
        return v;
    };
    // valid: ASCII, 2-byte (é), 3-byte (€), 4-byte (U+10348).
    CHECK(is_valid_utf8(u({'a', 'b', 'c'})));
    CHECK(is_valid_utf8(u({0xC3, 0xA9})));
    CHECK(is_valid_utf8(u({0xE2, 0x82, 0xAC})));
    CHECK(is_valid_utf8(u({0xF0, 0x90, 0x8D, 0x88})));
    // invalid: overlong 2-byte, lone continuation, bad continuation, truncated,
    // surrogate (U+D800), > U+10FFFF, and a 0xF8 lead byte.
    CHECK(!is_valid_utf8(u({0xC0, 0x80})));
    CHECK(!is_valid_utf8(u({0x80})));
    CHECK(!is_valid_utf8(u({0xE2, 0x00, 0xAC})));
    CHECK(!is_valid_utf8(u({0xE2, 0x82})));
    CHECK(!is_valid_utf8(u({0xED, 0xA0, 0x80})));
    CHECK(!is_valid_utf8(u({0xF4, 0x90, 0x80, 0x80})));
    CHECK(!is_valid_utf8(u({0xF8, 0x80, 0x80, 0x80})));
    // overlong 3-byte (U+0000 encoded in 3 bytes).
    CHECK(!is_valid_utf8(u({0xE0, 0x80, 0x80})));
}

TEST("put_u16_le/get_u16_le round-trip little-endian") {
    auto const bytes = encode_into([&](ByteWriter& w) { return put_u16_le(w, 0x1234); }, 4);
    CHECK(bytes.size() == 2);
    CHECK(std::to_integer<int>(bytes[0]) == 0x34 && std::to_integer<int>(bytes[1]) == 0x12);
    auto r = reader(bytes);
    auto got = get_u16_le(r);
    CHECK(got.has_value() && *got == 0x1234);
}

TEST("get_u16_le rejects a short buffer") {
    std::array<std::byte, 1> buf{std::byte{0x34}};
    ByteReader r{buf};
    CHECK(!get_u16_le(r).has_value());
}

TEST("put_raw writes bytes verbatim with no prefix") {
    std::array<std::byte, 3> src{std::byte{1}, std::byte{2}, std::byte{3}};
    auto const bytes = encode_into([&](ByteWriter& w) { return put_raw(w, src); });
    CHECK(bytes.size() == 3 && std::to_integer<int>(bytes[2]) == 3);
}

TEST("as_bytes/as_str round-trip a view") {
    std::string_view const s = "key/expr";
    auto const b = as_bytes(s);
    CHECK(b.size() == s.size());
    CHECK(as_str(b) == s);
}

// Direct unit tests for the extension (optional-field) codec helpers
// (zenoh.codec.ext): the header byte, the three wire kinds (Unit / U64 / ZStruct),
// peek/skip, and the malformed/overflow error paths.
import zenoh.codec.ext;
import zenoh.codec;
import zenoh.buffer;
import zenoh.util;

#include "ztest.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <span>
#include <vector>

using namespace zenoh;

namespace {

template <class Fn> auto encode_into(Fn&& fn, std::size_t cap = 64) -> std::vector<std::byte> {
    std::vector<std::byte> buf(cap);
    ByteWriter w{buf};
    CHECK(fn(w).has_value());
    return {buf.data(), buf.data() + w.written()};
}

} // namespace

TEST("ext header byte round-trips id/kind/mandatory/more") {
    for (bool mand : {false, true}) {
        for (bool more : {false, true}) {
            for (ExtKind kind : {ExtKind::unit, ExtKind::u64, ExtKind::zstruct}) {
                auto const b = ext_header_byte(0x5, kind, mand, more);
                std::array<std::byte, 1> buf{b};
                ByteReader r{buf};
                auto eh = peek_ext_header(r);
                CHECK(eh.has_value());
                CHECK(eh->id == 0x5);
                CHECK(eh->kind == kind);
                CHECK(eh->mandatory == mand);
                CHECK(eh->more == more);
                CHECK(r.remaining() == 1); // peek does not consume
            }
        }
    }
}

TEST("peek_ext_header rejects the reserved kind (0b11)") {
    std::array<std::byte, 1> buf{std::byte{0b0110'0000}}; // kind bits = 11
    ByteReader r{buf};
    auto eh = peek_ext_header(r);
    CHECK(!eh.has_value() && eh.error() == CodecError::malformed);
}

TEST("peek_ext_header reports exhaustion on empty input") {
    std::array<std::byte, 0> buf{};
    ByteReader r{buf};
    CHECK(!peek_ext_header(r).has_value());
}

TEST("Unit extension: write then read consumes exactly the header") {
    auto const bytes =
        encode_into([&](ByteWriter& w) { return put_ext_unit(w, 0x1, false, true); });
    CHECK(bytes.size() == 1);
    ByteReader r{bytes};
    auto eh = peek_ext_header(r);
    CHECK(eh.has_value() && eh->kind == ExtKind::unit && eh->more);
    CHECK(read_ext_unit(r).has_value());
    CHECK(r.remaining() == 0);
}

TEST("U64 extension: write then read the value") {
    auto const bytes =
        encode_into([&](ByteWriter& w) { return put_ext_u64(w, 0x2, true, false, 0xDEADBEEFULL); });
    ByteReader r{bytes};
    auto eh = peek_ext_header(r);
    CHECK(eh.has_value() && eh->kind == ExtKind::u64 && eh->mandatory);
    auto v = read_ext_u64(r);
    CHECK(v.has_value() && *v == 0xDEADBEEFULL);
    CHECK(r.remaining() == 0);
}

TEST("read_ext_uint narrows and rejects overflow") {
    auto const ok =
        encode_into([&](ByteWriter& w) { return put_ext_u64(w, 0x1, false, false, 250); });
    {
        ByteReader r{ok};
        auto v = read_ext_uint<std::uint8_t>(r);
        CHECK(v.has_value() && *v == 250);
    }
    auto const over =
        encode_into([&](ByteWriter& w) { return put_ext_u64(w, 0x1, false, false, 300); });
    {
        ByteReader r{over};
        auto v = read_ext_uint<std::uint8_t>(r);
        CHECK(!v.has_value() && v.error() == CodecError::malformed);
    }
}

TEST("ZStruct extension: write then read the length-delimited body") {
    std::array<std::byte, 3> body{std::byte{7}, std::byte{8}, std::byte{9}};
    auto const bytes = encode_into([&](ByteWriter& w) {
        return put_ext_zstruct(w, 0x4, false, false, body.size(),
                               [&](ByteWriter& ww) { return put_raw(ww, body); });
    });
    ByteReader r{bytes};
    auto eh = peek_ext_header(r);
    CHECK(eh.has_value() && eh->kind == ExtKind::zstruct);
    auto got = read_ext_zstruct(r);
    CHECK(got.has_value() && got->size() == 3 && std::to_integer<int>((*got)[2]) == 9);
    CHECK(r.remaining() == 0);
}

TEST("read_ext_zstruct rejects a body length that overruns") {
    // header (zstruct), len=4, but only 1 body byte.
    std::array<std::byte, 3> buf{ext_header_byte(0x4, ExtKind::zstruct, false, false), std::byte{4},
                                 std::byte{1}};
    ByteReader r{buf};
    CHECK(!read_ext_zstruct(r).has_value());
}

TEST("skip_ext consumes each kind's body") {
    // Unit.
    {
        auto const e =
            encode_into([&](ByteWriter& w) { return put_ext_unit(w, 0x1, false, false); });
        ByteReader r{e};
        CHECK(skip_ext(r, ExtKind::unit).has_value());
        CHECK(r.remaining() == 0);
    }
    // U64.
    {
        auto const e =
            encode_into([&](ByteWriter& w) { return put_ext_u64(w, 0x1, false, false, 99999); });
        ByteReader r{e};
        CHECK(skip_ext(r, ExtKind::u64).has_value());
        CHECK(r.remaining() == 0);
    }
    // ZStruct.
    {
        std::array<std::byte, 2> body{std::byte{1}, std::byte{2}};
        auto const e = encode_into([&](ByteWriter& w) {
            return put_ext_zstruct(w, 0x1, false, false, body.size(),
                                   [&](ByteWriter& ww) { return put_raw(ww, body); });
        });
        ByteReader r{e};
        CHECK(skip_ext(r, ExtKind::zstruct).has_value());
        CHECK(r.remaining() == 0);
    }
}

TEST("a chain of extensions decodes via the more bit") {
    auto const bytes = encode_into([&](ByteWriter& w) -> std::expected<void, CodecError> {
        if (!put_ext_unit(w, 0x1, false, /*more=*/true))
            return std::unexpected(CodecError::dst_full);
        if (!put_ext_u64(w, 0x2, false, /*more=*/true, 42))
            return std::unexpected(CodecError::dst_full);
        return put_ext_unit(w, 0x3, false, /*more=*/false);
    });
    ByteReader r{bytes};
    int seen = 0;
    bool more = true;
    while (more) {
        auto eh = peek_ext_header(r);
        CHECK(eh.has_value());
        if (!eh) break;
        CHECK(skip_ext(r, eh->kind).has_value());
        more = eh->more;
        ++seen;
    }
    CHECK(seen == 3);
    CHECK(r.remaining() == 0);
}

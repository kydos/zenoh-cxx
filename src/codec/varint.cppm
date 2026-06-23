module;

#include <bit>
#include <cstddef>
#include <cstdint>
#include <expected>

#include "zenoh/detail/try.hpp"

export module zenoh.varint;

import zenoh.buffer;
import zenoh.util;

// Variable-length integer (the protocol's `zint`). Kept inline in the interface
// unit: this is the hottest primitive and must inline into every codec call.
export namespace zenoh {

/// Number of bytes the VLE encoding of `x` occupies: 1..9.
///
/// Each byte carries 7 payload bits except the 9th, which carries a full 8 (so a
/// 64-bit value fits in 9 bytes, never 10). Branchless via countl_zero.
[[nodiscard]] constexpr auto vle_len(std::uint64_t x) noexcept -> std::size_t {
    unsigned const bits = 64u - static_cast<unsigned>(std::countl_zero(x | 1ull));
    std::size_t const n = (bits + 6u) / 7u;
    return n < 9u ? n : 9u;
}

/// Encode `x` as a VLE integer: low 7 bits per byte, bit 7 = "more bytes follow",
/// capped at 9 bytes (the 9th carries the remaining 8 bits, no continuation).
[[nodiscard]] inline auto encode_vle(ByteWriter& w, std::uint64_t x) noexcept
    -> std::expected<void, CodecError> {
    // Up to 8 continuation bytes of 7 bits each ...
    for (int i = 0; i < 8; ++i) {
        if ((x & ~std::uint64_t{0x7f}) == 0) {
            return w.write_byte(static_cast<std::byte>(x));
        }
        ZTRY(w.write_byte(static_cast<std::byte>((x & 0x7f) | 0x80)));
        x >>= 7;
    }
    // ... then the 9th byte holds the final 8 bits in full.
    return w.write_byte(static_cast<std::byte>(x));
}

/// Decode a VLE integer written by `encode_vle`. The 9-byte cap is enforced by the
/// `i != 56` guard: the 9th byte is consumed whole, continuation bit ignored. A
/// truncated input (continuation set with no following byte) errors via read_byte.
[[nodiscard]] inline auto decode_vle(ByteReader& r) noexcept
    -> std::expected<std::uint64_t, CodecError> {
    std::byte b = ZTRY(r.read_byte());
    std::uint64_t v = 0;
    unsigned i = 0;
    while ((std::to_integer<unsigned>(b) & 0x80u) != 0 && i != 56u) {
        v |= static_cast<std::uint64_t>(std::to_integer<unsigned>(b) & 0x7fu) << i;
        b = ZTRY(r.read_byte());
        i += 7;
    }
    v |= static_cast<std::uint64_t>(std::to_integer<unsigned>(b)) << i;
    return v;
}

} // namespace zenoh

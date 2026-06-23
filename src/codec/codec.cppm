module;

#include <concepts>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <limits>
#include <span>
#include <string_view>

#include "zenoh/detail/try.hpp"

export module zenoh.codec;

import zenoh.buffer;
import zenoh.util;
import zenoh.varint;

// Primitive wire codecs shared by every message. Kept inline in the interface unit
// (hot path): hand-written message codecs are call sequences over these, and those
// sequences collapse to straight-line code only if these inline.
export namespace zenoh {

// --- unsigned integers, VLE-encoded (the protocol's `zint`) ---

/// Encode an unsigned integer as VLE.
[[nodiscard]] inline auto put_uint(ByteWriter& w, std::uint64_t v) noexcept
    -> std::expected<void, CodecError> {
    return encode_vle(w, v);
}

/// Decode a VLE unsigned integer (full 64-bit range).
[[nodiscard]] inline auto get_uint(ByteReader& r) noexcept
    -> std::expected<std::uint64_t, CodecError> {
    return decode_vle(r);
}

/// Encoded length of `v` as VLE.
[[nodiscard]] inline auto len_uint(std::uint64_t v) noexcept -> std::size_t {
    return vle_len(v);
}

/// Decode a VLE integer into a narrower unsigned type, rejecting (as `malformed`)
/// any wire value that does not fit. Protocol `zint` fields are semantically
/// bounded (e.g. scope is u16, ids are u32), so an out-of-range value is malformed
/// rather than silently truncated.
template <std::unsigned_integral T>
[[nodiscard]] auto get_uint_as(ByteReader& r) noexcept -> std::expected<T, CodecError> {
    auto const v = ZTRY(decode_vle(r));
    if (v > static_cast<std::uint64_t>(std::numeric_limits<T>::max())) {
        return std::unexpected(CodecError::malformed);
    }
    return static_cast<T>(v);
}

// --- length-prefixed byte slice: VLE(len) ++ raw bytes ---

/// Encoded length of a length-prefixed byte slice.
[[nodiscard]] inline auto len_prefixed(std::span<const std::byte> s) noexcept -> std::size_t {
    return vle_len(s.size()) + s.size();
}

/// Write a length-prefixed byte slice.
[[nodiscard]] inline auto put_prefixed(ByteWriter& w, std::span<const std::byte> s) noexcept
    -> std::expected<void, CodecError> {
    ZTRY(encode_vle(w, s.size()));
    return w.write(s);
}

/// Read a length-prefixed byte slice (a borrowed view into the source).
[[nodiscard]] inline auto get_prefixed(ByteReader& r) noexcept
    -> std::expected<std::span<const std::byte>, CodecError> {
    auto const n = ZTRY(decode_vle(r));
    return r.read_slice(static_cast<std::size_t>(n));
}

// --- prefixed UTF-8 text (keyexpr suffixes, query parameters) ---

/// View a string as raw bytes.
[[nodiscard]] inline auto as_bytes(std::string_view s) noexcept -> std::span<const std::byte> {
    return {reinterpret_cast<const std::byte*>(s.data()), s.size()};
}

/// View raw bytes as a string.
[[nodiscard]] inline auto as_str(std::span<const std::byte> s) noexcept -> std::string_view {
    return {reinterpret_cast<const char*>(s.data()), s.size()};
}

/// Encoded length of a length-prefixed string.
[[nodiscard]] inline auto len_prefixed_str(std::string_view s) noexcept -> std::size_t {
    return len_prefixed(as_bytes(s));
}

/// Write a length-prefixed string.
[[nodiscard]] inline auto put_prefixed_str(ByteWriter& w, std::string_view s) noexcept
    -> std::expected<void, CodecError> {
    return put_prefixed(w, as_bytes(s));
}

/// Validate a well-formed UTF-8 byte sequence (rejecting overlong forms, UTF-16
/// surrogates, and code points above U+10FFFF), matching the reference's `&str`
/// decode. Text protocol fields (keyexpr suffix, query parameters) must be UTF-8.
[[nodiscard]] inline auto is_valid_utf8(std::span<const std::byte> s) noexcept -> bool {
    std::size_t i = 0;
    std::size_t const n = s.size();
    while (i < n) {
        std::uint8_t const b0 = std::to_integer<std::uint8_t>(s[i]);
        std::size_t len = 0;
        std::uint32_t cp = 0;
        if (b0 < 0x80) {
            ++i;
            continue;
        } else if ((b0 & 0xE0) == 0xC0) {
            if (b0 < 0xC2) return false; // overlong 2-byte
            len = 2;
            cp = b0 & 0x1Fu;
        } else if ((b0 & 0xF0) == 0xE0) {
            len = 3;
            cp = b0 & 0x0Fu;
        } else if ((b0 & 0xF8) == 0xF0) {
            if (b0 > 0xF4) return false; // > U+10FFFF
            len = 4;
            cp = b0 & 0x07u;
        } else {
            return false;
        }
        if (i + len > n) return false;
        for (std::size_t k = 1; k < len; ++k) {
            std::uint8_t const bk = std::to_integer<std::uint8_t>(s[i + k]);
            if ((bk & 0xC0) != 0x80) return false;
            cp = (cp << 6) | (bk & 0x3Fu);
        }
        if (len == 3 && cp < 0x800) return false;       // overlong 3-byte
        if (len == 4 && cp < 0x10000) return false;      // overlong 4-byte
        if (cp >= 0xD800 && cp <= 0xDFFF) return false;  // surrogate
        if (cp > 0x10FFFF) return false;                 // out of range
        i += len;
    }
    return true;
}

/// Read a length-prefixed UTF-8 string, rejecting non-UTF-8 as `invalid_field`.
[[nodiscard]] inline auto get_prefixed_str(ByteReader& r) noexcept
    -> std::expected<std::string_view, CodecError> {
    auto const s = ZTRY(get_prefixed(r));
    if (!is_valid_utf8(s)) return std::unexpected(CodecError::invalid_field);
    return as_str(s);
}

// --- raw bytes, no length prefix (fixed-size or "rest of buffer") ---

/// Write raw bytes (no length prefix).
[[nodiscard]] inline auto put_raw(ByteWriter& w, std::span<const std::byte> s) noexcept
    -> std::expected<void, CodecError> {
    return w.write(s);
}

// --- fixed little-endian u16 (e.g. BatchSize) ---

/// Write a fixed little-endian u16.
[[nodiscard]] inline auto put_u16_le(ByteWriter& w, std::uint16_t v) noexcept
    -> std::expected<void, CodecError> {
    std::byte b[2];
    store_le<std::uint16_t>(b, v);
    return w.write(std::span<const std::byte>{b, 2});
}

/// Read a fixed little-endian u16.
[[nodiscard]] inline auto get_u16_le(ByteReader& r) noexcept
    -> std::expected<std::uint16_t, CodecError> {
    auto const s = ZTRY(r.read_slice(2));
    return load_le<std::uint16_t>(s.data());
}

} // namespace zenoh

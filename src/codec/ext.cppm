module;

#include <concepts>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <limits>
#include <span>

#include "zenoh/detail/try.hpp"

export module zenoh.codec.ext;

import zenoh.buffer;
import zenoh.util;
import zenoh.varint;
import zenoh.codec;

// Minimal helpers for the protocol's optional fields ("extensions"). NOT a
// framework (PLAN.md D4): these just read/write the single extension-header byte
// and body for the three wire encodings. Kept inline (hot path).
export namespace zenoh {

/// Extension wire encoding, in header bits 6:5.
enum class ExtKind : std::uint8_t {
    unit    = 0b00 << 5, ///< 0x00 - presence flag, no body
    u64     = 0b01 << 5, ///< 0x20 - VLE u64 body
    zstruct = 0b10 << 5, ///< 0x40 - VLE(len)-prefixed structured body
};

/// Parsed extension header byte: `MORE(7) | KIND(6:5) | MAND(4) | ID(3:0)`.
struct ExtHeader {
    std::uint8_t id;
    ExtKind kind;
    bool mandatory;
    bool more;
};

inline constexpr std::uint8_t ext_id_mask = 0x0f;
inline constexpr std::uint8_t ext_kind_mask = 0b0110'0000;
inline constexpr std::uint8_t ext_flag_mandatory = 1u << 4;
inline constexpr std::uint8_t ext_flag_more = 1u << 7;

/// Build an extension header byte from its fields.
[[nodiscard]] inline auto
ext_header_byte(std::uint8_t id, ExtKind kind, bool mandatory, bool more) noexcept -> std::byte {
    std::uint8_t h = static_cast<std::uint8_t>((id & ext_id_mask) | static_cast<std::uint8_t>(kind));
    if (mandatory) h |= ext_flag_mandatory;
    if (more) h |= ext_flag_more;
    return static_cast<std::byte>(h);
}

/// Parse (without consuming) the next extension header so the caller can decide
/// whether to decode or skip it. Rejects the reserved KIND.
[[nodiscard]] inline auto peek_ext_header(ByteReader& r) noexcept
    -> std::expected<ExtHeader, CodecError> {
    auto const b = ZTRY(r.peek());
    std::uint8_t const h = std::to_integer<std::uint8_t>(b);
    ExtHeader eh{};
    eh.id = h & ext_id_mask;
    switch (h & ext_kind_mask) {
        case static_cast<std::uint8_t>(ExtKind::unit): eh.kind = ExtKind::unit; break;
        case static_cast<std::uint8_t>(ExtKind::u64): eh.kind = ExtKind::u64; break;
        case static_cast<std::uint8_t>(ExtKind::zstruct): eh.kind = ExtKind::zstruct; break;
        default: return std::unexpected(CodecError::malformed);
    }
    eh.mandatory = (h & ext_flag_mandatory) != 0;
    eh.more = (h & ext_flag_more) != 0;
    return eh;
}

// --- writers ---

/// Write a Unit extension (header byte only).
[[nodiscard]] inline auto put_ext_unit(ByteWriter& w, std::uint8_t id, bool mandatory,
                                       bool more) noexcept -> std::expected<void, CodecError> {
    return w.write_byte(ext_header_byte(id, ExtKind::unit, mandatory, more));
}

/// Write a U64 extension (header byte ++ VLE value).
[[nodiscard]] inline auto put_ext_u64(ByteWriter& w, std::uint8_t id, bool mandatory, bool more,
                                      std::uint64_t value) noexcept
    -> std::expected<void, CodecError> {
    ZTRY(w.write_byte(ext_header_byte(id, ExtKind::u64, mandatory, more)));
    return encode_vle(w, value);
}

/// Write a ZStruct extension: header ++ VLE(body_len) ++ body. `encode_body(w)`
/// must write exactly `body_len` bytes.
template <class EncodeBody>
[[nodiscard]] auto put_ext_zstruct(ByteWriter& w, std::uint8_t id, bool mandatory, bool more,
                                   std::size_t body_len, EncodeBody&& encode_body) noexcept
    -> std::expected<void, CodecError> {
    ZTRY(w.write_byte(ext_header_byte(id, ExtKind::zstruct, mandatory, more)));
    ZTRY(encode_vle(w, body_len));
    return encode_body(w);
}

// --- readers (header already peeked; these consume it) ---

/// Read a U64 extension value.
[[nodiscard]] inline auto read_ext_u64(ByteReader& r) noexcept
    -> std::expected<std::uint64_t, CodecError> {
    ZTRY(r.read_byte()); // discard header
    return decode_vle(r);
}

/// Read a U64 extension into a narrower unsigned type, rejecting (as `malformed`)
/// a value that does not fit (e.g. QoS u8, NodeId u16).
template <std::unsigned_integral T>
[[nodiscard]] auto read_ext_uint(ByteReader& r) noexcept -> std::expected<T, CodecError> {
    auto const v = ZTRY(read_ext_u64(r));
    if (v > static_cast<std::uint64_t>(std::numeric_limits<T>::max())) {
        return std::unexpected(CodecError::malformed);
    }
    return static_cast<T>(v);
}

/// Consume a Unit extension's header.
[[nodiscard]] inline auto read_ext_unit(ByteReader& r) noexcept
    -> std::expected<void, CodecError> {
    ZTRY(r.read_byte());
    return {};
}

/// Consume header + VLE(len) and return the `len`-byte body as a borrowed slice
/// (decode it from a sub-`ByteReader`).
[[nodiscard]] inline auto read_ext_zstruct(ByteReader& r) noexcept
    -> std::expected<std::span<const std::byte>, CodecError> {
    ZTRY(r.read_byte()); // discard header
    auto const n = ZTRY(decode_vle(r));
    return r.read_slice(static_cast<std::size_t>(n));
}

/// Discard an unrecognized (non-mandatory) extension by kind.
[[nodiscard]] inline auto skip_ext(ByteReader& r, ExtKind kind) noexcept
    -> std::expected<void, CodecError> {
    ZTRY(r.read_byte()); // discard header
    switch (kind) {
        case ExtKind::unit:
            break;
        case ExtKind::u64:
            ZTRY(decode_vle(r));
            break;
        case ExtKind::zstruct: {
            auto const n = ZTRY(decode_vle(r));
            ZTRY(r.read_slice(static_cast<std::size_t>(n)));
            break;
        }
    }
    return {};
}

} // namespace zenoh

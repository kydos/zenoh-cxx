module;

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <limits>
#include <ranges>
#include <span>
#include <string_view>

#include "zenoh/detail/try.hpp"

module zenoh.proto.fields;

import zenoh.buffer;
import zenoh.util;
import zenoh.codec;

// Implementation unit for zenoh.proto.fields: the encode/decode bodies for the
// value types whose declarations live in fields.cppm.
namespace zenoh {

auto Timestamp::encode(ByteWriter& w) const noexcept -> std::expected<void, CodecError> {
    // `id_len` indexes a fixed 16-byte array, but it is a plain public member: an
    // out-of-range value would make the span below read past `id` (and `encoded_len`
    // would obligingly size a buffer large enough for the whole overread to land).
    // Every sibling that nibble-encodes a zid length checks the same bounds --
    // EntityGlobalId::encode_body, DestinationId::encode_body, InitIdentifier::encode.
    if (id_len == 0 || id_len > 16) return std::unexpected(CodecError::malformed);
    ZTRY(put_uint(w, time));
    ZTRY(put_uint(w, id_len));
    return put_raw(w, std::span<const std::byte>{id.data(), id_len});
}

auto Timestamp::decode(ByteReader& r) noexcept -> std::expected<Timestamp, CodecError> {
    Timestamp t{};
    t.time = ZTRY(get_uint(r));
    auto const n = ZTRY(get_uint(r));
    // 1..16, not 0..16: a uhlc source id is never empty, and the reference rejects an
    // empty one in `uhlc::ID::try_from` (its inner value is a NonZeroU128). Accepting
    // it here would round-trip a timestamp no reference peer considers valid.
    if (n == 0 || n > 16) return std::unexpected(CodecError::malformed);
    t.id_len = static_cast<std::uint8_t>(n);
    auto const s = ZTRY(r.read_slice(t.id_len));
    std::ranges::copy(s, t.id.begin());
    return t;
}

auto Encoding::encode(ByteWriter& w) const noexcept -> std::expected<void, CodecError> {
    std::uint64_t const combined = (static_cast<std::uint64_t>(id) << 1) | (has_schema ? 1u : 0u);
    ZTRY(put_uint(w, combined));
    if (has_schema) return put_prefixed(w, schema);
    return {};
}

auto Encoding::decode(ByteReader& r) noexcept -> std::expected<Encoding, CodecError> {
    Encoding e{};
    auto const combined = ZTRY(get_uint(r));
    e.has_schema = (combined & 1u) != 0;
    // Reject rather than truncate, matching how every other narrowed field in this
    // codec behaves (`get_uint_as<T>`/`read_ext_uint<T>`, and docs/PROTO.md's rule).
    // A silent narrowing here decodes to a *different* encoding than the wire carried
    // and re-encodes to different bytes -- and since Put/Err elide the E flag when the
    // encoding is default, an id that truncates to 0 drops the field entirely on
    // relay. A deliberate divergence: the reference bounds this VLE to u32 and then
    // truncates to its u16 EncodingId, so it accepts (and mangles) ids we refuse.
    auto const raw_id = combined >> 1;
    if (raw_id > std::numeric_limits<std::uint16_t>::max()) {
        return std::unexpected(CodecError::malformed);
    }
    e.id = static_cast<std::uint16_t>(raw_id);
    if (e.has_schema) e.schema = ZTRY(get_prefixed(r));
    return e;
}

auto WireExpr::encode_body(ByteWriter& w) const noexcept -> std::expected<void, CodecError> {
    ZTRY(put_uint(w, scope));
    if (has_suffix()) return put_prefixed_str(w, suffix);
    return {};
}

auto WireExpr::decode_body(ByteReader& r, bool m, bool n) noexcept
    -> std::expected<WireExpr, CodecError> {
    WireExpr we{};
    we.scope = ZTRY(get_uint_as<std::uint16_t>(r));
    we.mapping = m ? Mapping::sender : Mapping::receiver;
    we.suffix = n ? ZTRY(get_prefixed_str(r)) : std::string_view{};
    return we;
}

auto WireExpr::encode_full(ByteWriter& w) const noexcept -> std::expected<void, CodecError> {
    std::uint8_t const flags =
        static_cast<std::uint8_t>(flag_if(is_sender(), 0b10) | flag_if(has_suffix(), 0b01));
    ZTRY(w.write_byte(static_cast<std::byte>(flags)));
    ZTRY(put_uint(w, scope));
    if (has_suffix()) return put_raw(w, as_bytes(suffix));
    return {};
}

auto WireExpr::decode_full(ByteReader& r) noexcept -> std::expected<WireExpr, CodecError> {
    WireExpr we{};
    std::uint8_t const flags = std::to_integer<std::uint8_t>(ZTRY(r.read_byte()));
    we.scope = ZTRY(get_uint_as<std::uint16_t>(r));
    we.mapping = (flags & 0b10) != 0 ? Mapping::sender : Mapping::receiver;
    if ((flags & 0b01) != 0) {
        auto const suf = ZTRY(r.read_slice(r.remaining()));
        if (!is_valid_utf8(suf)) return std::unexpected(CodecError::invalid_field);
        we.suffix = as_str(suf);
    }
    return we;
}

} // namespace zenoh

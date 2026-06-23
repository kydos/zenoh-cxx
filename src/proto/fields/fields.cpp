module;

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <expected>
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
    ZTRY(put_uint(w, time));
    ZTRY(put_uint(w, id_len));
    return put_raw(w, std::span<const std::byte>{id.data(), id_len});
}

auto Timestamp::decode(ByteReader& r) noexcept -> std::expected<Timestamp, CodecError> {
    Timestamp t{};
    t.time = ZTRY(get_uint(r));
    auto const n = ZTRY(get_uint(r));
    if (n > 16) return std::unexpected(CodecError::malformed);
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
    e.id = static_cast<std::uint16_t>(combined >> 1);
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
        static_cast<std::uint8_t>((is_sender() ? 0b10 : 0) | (has_suffix() ? 0b01 : 0));
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

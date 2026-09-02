module;

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <span>

#include "zenoh/detail/try.hpp"

module zenoh.proto.exts;

import zenoh.buffer;
import zenoh.util;
import zenoh.codec;
import zenoh.proto.fields;

// Implementation unit for zenoh.proto.exts: the structured-extension body codecs.
namespace zenoh {

auto EntityGlobalId::encode_body(ByteWriter& w) const noexcept -> std::expected<void, CodecError> {
    // The zid length (1..16) is stored as len-1 in the header's high nibble, so a
    // 16-byte id fits in 4 bits. An empty id would underflow len-1 to 0xf (decoding
    // back as 16) and silently corrupt the stream, so reject it.
    if (zid.len == 0 || zid.len > 16) return std::unexpected(CodecError::malformed);
    std::uint8_t const nibble = static_cast<std::uint8_t>((unsigned{zid.len} - 1U) & 0x0f);
    ZTRY(w.write_byte(static_cast<std::byte>(static_cast<std::uint8_t>(nibble << 4))));
    ZTRY(put_raw(w, zid.view()));
    return put_uint(w, eid);
}

auto EntityGlobalId::decode_body(ByteReader& r) noexcept
    -> std::expected<EntityGlobalId, CodecError> {
    EntityGlobalId g{};
    auto const h = ZTRY(r.read_byte());
    std::uint8_t const zlen =
        static_cast<std::uint8_t>((std::to_integer<std::uint8_t>(h) >> 4) + 1);
    auto const s = ZTRY(r.read_slice(zlen));
    g.zid.len = zlen;
    std::ranges::copy(s, g.zid.bytes.begin());
    g.eid = ZTRY(get_uint_as<std::uint32_t>(r));
    return g;
}

auto DestinationId::encode_body(ByteWriter& w) const noexcept -> std::expected<void, CodecError> {
    // Same nibble-encoded-length shape as EntityGlobalId::encode_body, minus the
    // trailing eid; see that function's comment for the len==0/len>16 rationale.
    if (zid.len == 0 || zid.len > 16) return std::unexpected(CodecError::malformed);
    std::uint8_t const nibble = static_cast<std::uint8_t>((unsigned{zid.len} - 1U) & 0x0f);
    ZTRY(w.write_byte(static_cast<std::byte>(static_cast<std::uint8_t>(nibble << 4))));
    return put_raw(w, zid.view());
}

auto DestinationId::decode_body(ByteReader& r) noexcept
    -> std::expected<DestinationId, CodecError> {
    DestinationId d{};
    auto const h = ZTRY(r.read_byte());
    std::uint8_t const zlen =
        static_cast<std::uint8_t>((std::to_integer<std::uint8_t>(h) >> 4) + 1);
    auto const s = ZTRY(r.read_slice(zlen));
    d.zid.len = zlen;
    std::ranges::copy(s, d.zid.bytes.begin());
    return d;
}

auto SourceInfo::encode_body(ByteWriter& w) const noexcept -> std::expected<void, CodecError> {
    ZTRY(id.encode_body(w));
    return put_uint(w, sn);
}

auto SourceInfo::decode_body(ByteReader& r) noexcept -> std::expected<SourceInfo, CodecError> {
    SourceInfo si{};
    si.id = ZTRY(EntityGlobalId::decode_body(r));
    si.sn = ZTRY(get_uint_as<std::uint32_t>(r));
    return si;
}

auto Attachment::encode_body(ByteWriter& w) const noexcept -> std::expected<void, CodecError> {
    return put_raw(w, buffer);
}

auto Value::encode_body(ByteWriter& w) const noexcept -> std::expected<void, CodecError> {
    ZTRY(encoding.encode(w));
    return put_raw(w, payload);
}

auto Value::decode_body(ByteReader& r) noexcept -> std::expected<Value, CodecError> {
    Value v{};
    v.encoding = ZTRY(Encoding::decode(r));
    v.payload = ZTRY(r.read_slice(r.remaining())); // remainder of the ext slice
    return v;
}

} // namespace zenoh

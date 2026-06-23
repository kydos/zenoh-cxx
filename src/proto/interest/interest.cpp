module;

#include <cstddef>
#include <cstdint>
#include <expected>
#include <optional>

#include "zenoh/detail/try.hpp"

module zenoh.proto.interest;

import zenoh.buffer;
import zenoh.util;
import zenoh.codec;
import zenoh.codec.ext;
import zenoh.proto.fields;

// Implementation unit for zenoh.proto.interest.
namespace zenoh {

auto InterestInner::encode(ByteWriter& w) const noexcept -> std::expected<void, CodecError> {
    std::uint8_t h = static_cast<std::uint8_t>(options & opt_mask);
    if (wire_expr) {
        h |= flag_r;
        if (wire_expr->is_sender()) h |= flag_m;
        if (wire_expr->has_suffix()) h |= flag_n;
    }
    ZTRY(w.write_byte(static_cast<std::byte>(h)));
    if (wire_expr) return wire_expr->encode_body(w);
    return {};
}

auto InterestInner::decode(ByteReader& r) noexcept -> std::expected<InterestInner, CodecError> {
    InterestInner inner{};
    std::uint8_t const h = std::to_integer<std::uint8_t>(ZTRY(r.read_byte()));
    inner.options = h;
    if ((h & flag_r) != 0) {
        inner.wire_expr = ZTRY(WireExpr::decode_body(r, (h & flag_m) != 0, (h & flag_n) != 0));
    }
    return inner;
}

auto Interest::encode(ByteWriter& w) const noexcept -> std::expected<void, CodecError> {
    bool const e_qos = !(qos == QoS{});
    bool const e_ts = timestamp.has_value();
    bool const e_nid = !(nodeid == NodeId{});
    bool const z = e_qos || e_ts || e_nid;

    auto const h =
        static_cast<std::uint8_t>(mid | (z ? flag_z : 0) | (static_cast<std::uint8_t>(mode) << 5));
    ZTRY(w.write_byte(static_cast<std::byte>(h)));
    ZTRY(put_uint(w, id));
    ZTRY(inner.encode(w));

    if (e_qos) ZTRY(put_ext_u64(w, 0x1, false, e_ts || e_nid, qos.inner));
    if (e_ts)
        ZTRY(put_ext_zstruct(w, 0x2, false, e_nid, timestamp->encoded_len(),
                             [&](auto& ww) { return timestamp->encode(ww); }));
    if (e_nid) ZTRY(put_ext_u64(w, 0x3, true, false, nodeid.node_id));
    return {};
}

auto Interest::decode(ByteReader& r) noexcept -> std::expected<Interest, CodecError> {
    std::uint8_t const h = std::to_integer<std::uint8_t>(ZTRY(r.read_byte()));
    if ((h & mid_mask) != mid) return std::unexpected(CodecError::malformed);
    bool has_ext = (h & flag_z) != 0;

    Interest it{};
    it.mode = static_cast<InterestMode>((h >> 5) & 0x3);
    it.id = ZTRY(get_uint_as<std::uint32_t>(r));
    it.inner = ZTRY(InterestInner::decode(r));

    while (has_ext) {
        auto const eh = ZTRY(peek_ext_header(r));
        switch (eh.id) {
            case 0x1: it.qos.inner = ZTRY(read_ext_uint<std::uint8_t>(r)); break;
            case 0x2: {
                ByteReader sub{ZTRY(read_ext_zstruct(r))};
                it.timestamp = ZTRY(Timestamp::decode(sub));
                break;
            }
            case 0x3: it.nodeid.node_id = ZTRY(read_ext_uint<std::uint16_t>(r)); break;
            default:
                if (eh.mandatory) return std::unexpected(CodecError::malformed);
                ZTRY(skip_ext(r, eh.kind));
        }
        has_ext = eh.more;
    }
    return it;
}

auto InterestFinal::encode(ByteWriter& w) const noexcept -> std::expected<void, CodecError> {
    bool const e_qos = !(qos == QoS{});
    bool const e_ts = timestamp.has_value();
    bool const e_nid = !(nodeid == NodeId{});
    bool const z = e_qos || e_ts || e_nid;

    auto const h = static_cast<std::uint8_t>(mid | (z ? flag_z : 0)); // MODE = 0
    ZTRY(w.write_byte(static_cast<std::byte>(h)));
    ZTRY(put_uint(w, id));

    if (e_qos) ZTRY(put_ext_u64(w, 0x1, false, e_ts || e_nid, qos.inner));
    if (e_ts)
        ZTRY(put_ext_zstruct(w, 0x2, false, e_nid, timestamp->encoded_len(),
                             [&](auto& ww) { return timestamp->encode(ww); }));
    if (e_nid) ZTRY(put_ext_u64(w, 0x3, true, false, nodeid.node_id));
    return {};
}

auto InterestFinal::decode(ByteReader& r) noexcept -> std::expected<InterestFinal, CodecError> {
    std::uint8_t const h = std::to_integer<std::uint8_t>(ZTRY(r.read_byte()));
    if ((h & mid_mask) != mid || (h & mode_mask) != 0) return std::unexpected(CodecError::malformed);
    bool has_ext = (h & flag_z) != 0;

    InterestFinal it{};
    it.id = ZTRY(get_uint_as<std::uint32_t>(r));
    while (has_ext) {
        auto const eh = ZTRY(peek_ext_header(r));
        switch (eh.id) {
            case 0x1: it.qos.inner = ZTRY(read_ext_uint<std::uint8_t>(r)); break;
            case 0x2: {
                ByteReader sub{ZTRY(read_ext_zstruct(r))};
                it.timestamp = ZTRY(Timestamp::decode(sub));
                break;
            }
            case 0x3: it.nodeid.node_id = ZTRY(read_ext_uint<std::uint16_t>(r)); break;
            default:
                if (eh.mandatory) return std::unexpected(CodecError::malformed);
                ZTRY(skip_ext(r, eh.kind));
        }
        has_ext = eh.more;
    }
    return it;
}

} // namespace zenoh

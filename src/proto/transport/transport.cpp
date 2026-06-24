module;

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <optional>
#include <span>

#include "zenoh/detail/try.hpp"

module zenoh.proto.transport;

import zenoh.buffer;
import zenoh.util;
import zenoh.codec;
import zenoh.codec.ext;
import zenoh.proto.fields;

// Implementation unit for zenoh.proto.transport: identifier + message codecs.
namespace zenoh {

// --- InitIdentifier / InitResolution ---

auto InitIdentifier::encode(ByteWriter& w) const noexcept -> std::expected<void, CodecError> {
    // zid length is 1..16, stored as len-1 in the high nibble; an empty id would
    // underflow to 0xf (decoding back as 16) and corrupt the stream, so reject it.
    if (zid.len == 0 || zid.len > 16) return std::unexpected(CodecError::malformed);
    std::uint8_t const nibble = static_cast<std::uint8_t>((zid.len - 1) & 0x0f);
    std::uint8_t const h =
        static_cast<std::uint8_t>((nibble << 4) | (static_cast<std::uint8_t>(whatami) & 0x03));
    ZTRY(w.write_byte(static_cast<std::byte>(h)));
    return put_raw(w, zid.view());
}

auto InitIdentifier::decode(ByteReader& r) noexcept -> std::expected<InitIdentifier, CodecError> {
    InitIdentifier i{};
    std::uint8_t const h = std::to_integer<std::uint8_t>(ZTRY(r.read_byte()));
    std::uint8_t const w = h & 0x03;
    if (w > 2) return std::unexpected(CodecError::malformed); // 0b11 is not a valid WhatAmI
    i.whatami = static_cast<WhatAmI>(w);
    std::uint8_t const zlen = static_cast<std::uint8_t>(((h >> 4) & 0x0f) + 1);
    auto const s = ZTRY(r.read_slice(zlen));
    i.zid.len = zlen;
    std::ranges::copy(s, i.zid.bytes.begin());
    return i;
}

auto InitResolution::encode(ByteWriter& w) const noexcept -> std::expected<void, CodecError> {
    ZTRY(w.write_byte(static_cast<std::byte>(resolution)));
    return put_u16_le(w, batch_size);
}

auto InitResolution::decode(ByteReader& r) noexcept -> std::expected<InitResolution, CodecError> {
    InitResolution ir{};
    ir.resolution = std::to_integer<std::uint8_t>(ZTRY(r.read_byte()));
    ir.batch_size = ZTRY(get_u16_le(r));
    return ir;
}

// --- InitAck ---

auto InitAck::encode(ByteWriter& w) const noexcept -> std::expected<void, CodecError> {
    bool const s = !(resolution == InitResolution{});

    bool const e_qos = qos.has_value();
    bool const e_qoslink = qos_link.has_value();
    bool const e_auth = auth.has_value();
    bool const e_mlink = mlink.has_value();
    bool const e_lowlat = lowlatency.has_value();
    bool const e_comp = compression.has_value();
    bool const e_patch = !(patch == Patch{});
    bool const z = e_qos || e_qoslink || e_auth || e_mlink || e_lowlat || e_comp || e_patch;

    auto const h = static_cast<std::uint8_t>(id | flag_a | (s ? flag_s : 0) | (z ? flag_z : 0));
    ZTRY(w.write_byte(static_cast<std::byte>(h)));

    ZTRY(w.write_byte(static_cast<std::byte>(version)));
    ZTRY(identifier.encode(w));
    if (s) ZTRY(resolution.encode(w));
    ZTRY(put_prefixed(w, cookie));

    // Extensions, ascending id; `more` = any later extension is present.
    if (e_qos)
        ZTRY(put_ext_unit(w, 0x1, false,
                          e_qoslink || e_auth || e_mlink || e_lowlat || e_comp || e_patch));
    if (e_qoslink)
        ZTRY(put_ext_u64(w, 0x1, false, e_auth || e_mlink || e_lowlat || e_comp || e_patch,
                         qos_link->qos));
    if (e_auth)
        ZTRY(put_ext_zstruct(w, 0x3, false, e_mlink || e_lowlat || e_comp || e_patch,
                             auth->payload.size(),
                             [&](auto& ww) { return put_raw(ww, auth->payload); }));
    if (e_mlink)
        ZTRY(put_ext_zstruct(w, 0x4, false, e_lowlat || e_comp || e_patch, mlink->payload.size(),
                             [&](auto& ww) { return put_raw(ww, mlink->payload); }));
    if (e_lowlat) ZTRY(put_ext_unit(w, 0x5, false, e_comp || e_patch));
    if (e_comp) ZTRY(put_ext_unit(w, 0x6, false, e_patch));
    if (e_patch) ZTRY(put_ext_u64(w, 0x7, false, false, patch.value));

    return {};
}

auto InitAck::decode(ByteReader& r) noexcept -> std::expected<InitAck, CodecError> {
    std::uint8_t const h = std::to_integer<std::uint8_t>(ZTRY(r.read_byte()));
    if ((h & mid_mask) != id || (h & flag_a) == 0) return std::unexpected(CodecError::malformed);
    bool const s = (h & flag_s) != 0;
    bool has_ext = (h & flag_z) != 0;

    InitAck a{};
    a.version = std::to_integer<std::uint8_t>(ZTRY(r.read_byte()));
    a.identifier = ZTRY(InitIdentifier::decode(r));
    if (s) a.resolution = ZTRY(InitResolution::decode(r));
    a.cookie = ZTRY(get_prefixed(r));

    while (has_ext) {
        auto const eh = ZTRY(peek_ext_header(r));
        switch (eh.id) {
        case 0x1: // qos (Unit) and qos_link (U64) share this id
            if (eh.kind == ExtKind::unit) {
                ZTRY(read_ext_unit(r));
                a.qos = HasQoS{};
            } else if (eh.kind == ExtKind::u64) {
                a.qos_link = QoSLink{ZTRY(read_ext_u64(r))};
            } else {
                ZTRY(skip_ext(r, eh.kind));
            }
            break;
        case 0x3:
            a.auth = Auth{ZTRY(read_ext_zstruct(r))};
            break;
        case 0x4:
            a.mlink = MultiLink{ZTRY(read_ext_zstruct(r))};
            break;
        case 0x5:
            ZTRY(read_ext_unit(r));
            a.lowlatency = HasLowLatency{};
            break;
        case 0x6:
            ZTRY(read_ext_unit(r));
            a.compression = HasCompression{};
            break;
        case 0x7:
            a.patch.value = ZTRY(read_ext_uint<std::uint8_t>(r));
            break;
        default:
            if (eh.mandatory) return std::unexpected(CodecError::malformed);
            ZTRY(skip_ext(r, eh.kind));
        }
        has_ext = eh.more;
    }
    return a;
}

// --- InitSyn ---

auto InitSyn::encode(ByteWriter& w) const noexcept -> std::expected<void, CodecError> {
    bool const s = !(resolution == InitResolution{});
    bool const e_qos = qos.has_value();
    bool const e_qoslink = qos_link.has_value();
    bool const e_auth = auth.has_value();
    bool const e_mlink = mlink.has_value();
    bool const e_lowlat = lowlatency.has_value();
    bool const e_comp = compression.has_value();
    bool const e_patch = !(patch == Patch{});
    bool const z = e_qos || e_qoslink || e_auth || e_mlink || e_lowlat || e_comp || e_patch;

    auto const h = static_cast<std::uint8_t>(id | (s ? flag_s : 0) | (z ? flag_z : 0));
    ZTRY(w.write_byte(static_cast<std::byte>(h)));
    ZTRY(w.write_byte(static_cast<std::byte>(version)));
    ZTRY(identifier.encode(w));
    if (s) ZTRY(resolution.encode(w));

    if (e_qos)
        ZTRY(put_ext_unit(w, 0x1, false,
                          e_qoslink || e_auth || e_mlink || e_lowlat || e_comp || e_patch));
    if (e_qoslink)
        ZTRY(put_ext_u64(w, 0x1, false, e_auth || e_mlink || e_lowlat || e_comp || e_patch,
                         qos_link->qos));
    if (e_auth)
        ZTRY(put_ext_zstruct(w, 0x3, false, e_mlink || e_lowlat || e_comp || e_patch,
                             auth->payload.size(),
                             [&](auto& ww) { return put_raw(ww, auth->payload); }));
    if (e_mlink)
        ZTRY(put_ext_zstruct(w, 0x4, false, e_lowlat || e_comp || e_patch, mlink->payload.size(),
                             [&](auto& ww) { return put_raw(ww, mlink->payload); }));
    if (e_lowlat) ZTRY(put_ext_unit(w, 0x5, false, e_comp || e_patch));
    if (e_comp) ZTRY(put_ext_unit(w, 0x6, false, e_patch));
    if (e_patch) ZTRY(put_ext_u64(w, 0x7, false, false, patch.value));
    return {};
}

auto InitSyn::decode(ByteReader& r) noexcept -> std::expected<InitSyn, CodecError> {
    std::uint8_t const h = std::to_integer<std::uint8_t>(ZTRY(r.read_byte()));
    if ((h & mid_mask) != id || (h & flag_a) != 0) return std::unexpected(CodecError::malformed);
    bool const s = (h & flag_s) != 0;
    bool has_ext = (h & flag_z) != 0;

    InitSyn a{};
    a.version = std::to_integer<std::uint8_t>(ZTRY(r.read_byte()));
    a.identifier = ZTRY(InitIdentifier::decode(r));
    if (s) a.resolution = ZTRY(InitResolution::decode(r));

    while (has_ext) {
        auto const eh = ZTRY(peek_ext_header(r));
        switch (eh.id) {
        case 0x1:
            if (eh.kind == ExtKind::unit) {
                ZTRY(read_ext_unit(r));
                a.qos = HasQoS{};
            } else if (eh.kind == ExtKind::u64) {
                a.qos_link = QoSLink{ZTRY(read_ext_u64(r))};
            } else {
                ZTRY(skip_ext(r, eh.kind));
            }
            break;
        case 0x3:
            a.auth = Auth{ZTRY(read_ext_zstruct(r))};
            break;
        case 0x4:
            a.mlink = MultiLink{ZTRY(read_ext_zstruct(r))};
            break;
        case 0x5:
            ZTRY(read_ext_unit(r));
            a.lowlatency = HasLowLatency{};
            break;
        case 0x6:
            ZTRY(read_ext_unit(r));
            a.compression = HasCompression{};
            break;
        case 0x7:
            a.patch.value = ZTRY(read_ext_uint<std::uint8_t>(r));
            break;
        default:
            if (eh.mandatory) return std::unexpected(CodecError::malformed);
            ZTRY(skip_ext(r, eh.kind));
        }
        has_ext = eh.more;
    }
    return a;
}

// --- Close / KeepAlive ---

auto Close::encode(ByteWriter& w) const noexcept -> std::expected<void, CodecError> {
    auto const h =
        static_cast<std::uint8_t>(id | (behaviour == CloseBehaviour::session ? flag_s : 0));
    ZTRY(w.write_byte(static_cast<std::byte>(h)));
    return w.write_byte(static_cast<std::byte>(reason));
}

auto Close::decode(ByteReader& r) noexcept -> std::expected<Close, CodecError> {
    Close c{};
    std::uint8_t const h = std::to_integer<std::uint8_t>(ZTRY(r.read_byte()));
    if ((h & mid_mask) != id) return std::unexpected(CodecError::malformed);
    c.behaviour = (h & flag_s) != 0 ? CloseBehaviour::session : CloseBehaviour::link;
    c.reason = std::to_integer<std::uint8_t>(ZTRY(r.read_byte()));
    return c;
}

auto KeepAlive::encode(ByteWriter& w) const noexcept -> std::expected<void, CodecError> {
    return w.write_byte(static_cast<std::byte>(id));
}

auto KeepAlive::decode(ByteReader& r) noexcept -> std::expected<KeepAlive, CodecError> {
    std::uint8_t const h = std::to_integer<std::uint8_t>(ZTRY(r.read_byte()));
    if ((h & mid_mask) != id) return std::unexpected(CodecError::malformed);
    return KeepAlive{};
}

// --- FrameHeader ---

auto FrameHeader::encode(ByteWriter& w) const noexcept -> std::expected<void, CodecError> {
    bool const rel = reliability == Reliability::reliable;
    bool const q = !(qos == QoS{});
    auto const h = static_cast<std::uint8_t>(id | (q ? flag_z : 0) | (rel ? flag_r : 0));
    ZTRY(w.write_byte(static_cast<std::byte>(h)));
    ZTRY(put_uint(w, sn));
    if (q) ZTRY(put_ext_u64(w, 0x1, /*mandatory=*/true, false, qos.inner));
    return {};
}

auto FrameHeader::decode(ByteReader& r) noexcept -> std::expected<FrameHeader, CodecError> {
    FrameHeader f{};
    std::uint8_t const h = std::to_integer<std::uint8_t>(ZTRY(r.read_byte()));
    if ((h & mid_mask) != id) return std::unexpected(CodecError::malformed);
    f.reliability = (h & flag_r) != 0 ? Reliability::reliable : Reliability::best_effort;
    bool has_ext = (h & flag_z) != 0;
    f.sn = ZTRY(get_uint_as<std::uint32_t>(r));
    while (has_ext) {
        auto const eh = ZTRY(peek_ext_header(r));
        switch (eh.id) {
        case 0x1:
            f.qos.inner = ZTRY(read_ext_uint<std::uint8_t>(r));
            break;
        default:
            if (eh.mandatory) return std::unexpected(CodecError::malformed);
            ZTRY(skip_ext(r, eh.kind));
        }
        has_ext = eh.more;
    }
    return f;
}

// --- OpenSyn / OpenAck ---

auto OpenSyn::encode(ByteWriter& w) const noexcept -> std::expected<void, CodecError> {
    bool const t = lease.is_seconds();
    bool const e_qos = qos.has_value();
    bool const e_auth = auth.has_value();
    bool const e_msyn = mlink_syn.has_value();
    bool const e_mack = mlink_ack.has_value();
    bool const e_lowlat = lowlatency.has_value();
    bool const e_comp = compression.has_value();
    bool const z = e_qos || e_auth || e_msyn || e_mack || e_lowlat || e_comp;

    auto const h = static_cast<std::uint8_t>(id | (z ? flag_z : 0) | (t ? flag_t : 0));
    ZTRY(w.write_byte(static_cast<std::byte>(h)));
    ZTRY(put_uint(w, lease.wire_value()));
    ZTRY(put_uint(w, sn));
    ZTRY(put_prefixed(w, cookie));

    if (e_qos) ZTRY(put_ext_unit(w, 0x1, false, e_auth || e_msyn || e_mack || e_lowlat || e_comp));
    if (e_auth)
        ZTRY(put_ext_zstruct(w, 0x3, false, e_msyn || e_mack || e_lowlat || e_comp,
                             auth->payload.size(),
                             [&](auto& ww) { return put_raw(ww, auth->payload); }));
    if (e_msyn)
        ZTRY(put_ext_zstruct(w, 0x4, false, e_mack || e_lowlat || e_comp, mlink_syn->payload.size(),
                             [&](auto& ww) { return put_raw(ww, mlink_syn->payload); }));
    if (e_mack) ZTRY(put_ext_unit(w, 0x4, false, e_lowlat || e_comp));
    if (e_lowlat) ZTRY(put_ext_unit(w, 0x5, false, e_comp));
    if (e_comp) ZTRY(put_ext_unit(w, 0x6, false, false));
    return {};
}

auto OpenSyn::decode(ByteReader& r) noexcept -> std::expected<OpenSyn, CodecError> {
    std::uint8_t const h = std::to_integer<std::uint8_t>(ZTRY(r.read_byte()));
    if ((h & mid_mask) != id || (h & flag_a) != 0) return std::unexpected(CodecError::malformed);
    bool const t = (h & flag_t) != 0;
    bool has_ext = (h & flag_z) != 0;

    OpenSyn o{};
    o.lease = ZTRY(Duration::decode_value(r, t));
    o.sn = ZTRY(get_uint_as<std::uint32_t>(r));
    o.cookie = ZTRY(get_prefixed(r));

    while (has_ext) {
        auto const eh = ZTRY(peek_ext_header(r));
        switch (eh.id) {
        case 0x1:
            ZTRY(read_ext_unit(r));
            o.qos = HasQoS{};
            break;
        case 0x3:
            o.auth = Auth{ZTRY(read_ext_zstruct(r))};
            break;
        case 0x4: // mlink_syn (ZStruct) vs mlink_ack (Unit)
            if (eh.kind == ExtKind::zstruct) {
                o.mlink_syn = MultiLinkSyn{ZTRY(read_ext_zstruct(r))};
            } else if (eh.kind == ExtKind::unit) {
                ZTRY(read_ext_unit(r));
                o.mlink_ack = HasMultiLinkAck{};
            } else {
                ZTRY(skip_ext(r, eh.kind));
            }
            break;
        case 0x5:
            ZTRY(read_ext_unit(r));
            o.lowlatency = HasLowLatency{};
            break;
        case 0x6:
            ZTRY(read_ext_unit(r));
            o.compression = HasCompression{};
            break;
        default:
            if (eh.mandatory) return std::unexpected(CodecError::malformed);
            ZTRY(skip_ext(r, eh.kind));
        }
        has_ext = eh.more;
    }
    return o;
}

auto OpenAck::encode(ByteWriter& w) const noexcept -> std::expected<void, CodecError> {
    bool const t = lease.is_seconds();
    bool const e_qos = qos.has_value();
    bool const e_auth = auth.has_value();
    bool const e_msyn = mlink_syn.has_value();
    bool const e_mack = mlink_ack.has_value();
    bool const e_lowlat = lowlatency.has_value();
    bool const e_comp = compression.has_value();
    bool const z = e_qos || e_auth || e_msyn || e_mack || e_lowlat || e_comp;

    auto const h = static_cast<std::uint8_t>(id | flag_a | (z ? flag_z : 0) | (t ? flag_t : 0));
    ZTRY(w.write_byte(static_cast<std::byte>(h)));
    ZTRY(put_uint(w, lease.wire_value()));
    ZTRY(put_uint(w, sn));

    if (e_qos) ZTRY(put_ext_unit(w, 0x1, false, e_auth || e_msyn || e_mack || e_lowlat || e_comp));
    if (e_auth)
        ZTRY(put_ext_zstruct(w, 0x3, false, e_msyn || e_mack || e_lowlat || e_comp,
                             auth->payload.size(),
                             [&](auto& ww) { return put_raw(ww, auth->payload); }));
    if (e_msyn)
        ZTRY(put_ext_zstruct(w, 0x4, false, e_mack || e_lowlat || e_comp, mlink_syn->payload.size(),
                             [&](auto& ww) { return put_raw(ww, mlink_syn->payload); }));
    if (e_mack) ZTRY(put_ext_unit(w, 0x4, false, e_lowlat || e_comp));
    if (e_lowlat) ZTRY(put_ext_unit(w, 0x5, false, e_comp));
    if (e_comp) ZTRY(put_ext_unit(w, 0x6, false, false));
    return {};
}

auto OpenAck::decode(ByteReader& r) noexcept -> std::expected<OpenAck, CodecError> {
    std::uint8_t const h = std::to_integer<std::uint8_t>(ZTRY(r.read_byte()));
    if ((h & mid_mask) != id || (h & flag_a) == 0) return std::unexpected(CodecError::malformed);
    bool const t = (h & flag_t) != 0;
    bool has_ext = (h & flag_z) != 0;

    OpenAck o{};
    o.lease = ZTRY(Duration::decode_value(r, t));
    o.sn = ZTRY(get_uint_as<std::uint32_t>(r));

    while (has_ext) {
        auto const eh = ZTRY(peek_ext_header(r));
        switch (eh.id) {
        case 0x1:
            ZTRY(read_ext_unit(r));
            o.qos = HasQoS{};
            break;
        case 0x3:
            o.auth = Auth{ZTRY(read_ext_zstruct(r))};
            break;
        case 0x4:
            if (eh.kind == ExtKind::zstruct) {
                o.mlink_syn = MultiLinkSyn{ZTRY(read_ext_zstruct(r))};
            } else if (eh.kind == ExtKind::unit) {
                ZTRY(read_ext_unit(r));
                o.mlink_ack = HasMultiLinkAck{};
            } else {
                ZTRY(skip_ext(r, eh.kind));
            }
            break;
        case 0x5:
            ZTRY(read_ext_unit(r));
            o.lowlatency = HasLowLatency{};
            break;
        case 0x6:
            ZTRY(read_ext_unit(r));
            o.compression = HasCompression{};
            break;
        default:
            if (eh.mandatory) return std::unexpected(CodecError::malformed);
            ZTRY(skip_ext(r, eh.kind));
        }
        has_ext = eh.more;
    }
    return o;
}

} // namespace zenoh

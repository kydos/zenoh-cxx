module;

#include <cstddef>
#include <cstdint>
#include <expected>
#include <optional>
#include <span>
#include <string_view>

#include "zenoh/detail/try.hpp"

module zenoh.proto.network;

import zenoh.buffer;
import zenoh.util;
import zenoh.codec;
import zenoh.codec.ext;
import zenoh.proto.fields;
import zenoh.proto.exts;

// Implementation unit for zenoh.proto.network: the per-message encode/decode
// bodies. The body-dispatch wrappers and operator== stay inline in network.cppm.
namespace zenoh {

// --- Put ---

auto Put::encode_head(ByteWriter& w) const noexcept -> std::expected<void, CodecError> {
    bool const t = timestamp.has_value();
    bool const e = !(encoding == Encoding{});
    bool const has_si = sinfo.has_value();
    bool const has_at = attachment.has_value();
    bool const z = has_si || has_at;

    auto const h =
        static_cast<std::uint8_t>(id | (z ? flag_z : 0) | (e ? flag_x6 : 0) | (t ? flag_x5 : 0));
    ZTRY(w.write_byte(static_cast<std::byte>(h)));

    if (t) ZTRY(timestamp->encode(w));
    if (e) ZTRY(encoding.encode(w));

    if (has_si)
        ZTRY(put_ext_zstruct(w, 0x1, false, /*more=*/has_at, sinfo->body_len(),
                             [&](auto& ww) { return sinfo->encode_body(ww); }));
    if (has_at)
        ZTRY(put_ext_zstruct(w, 0x3, false, /*more=*/false, attachment->body_len(),
                             [&](auto& ww) { return attachment->encode_body(ww); }));

    // Payload length prefix only; bytes are appended by encode (or by a gather caller).
    return put_uint(w, payload.size());
}

auto Put::encode(ByteWriter& w) const noexcept -> std::expected<void, CodecError> {
    ZTRY(encode_head(w));
    return put_raw(w, payload);
}

auto Put::decode(ByteReader& r) noexcept -> std::expected<Put, CodecError> {
    std::uint8_t const h = std::to_integer<std::uint8_t>(ZTRY(r.read_byte()));
    if ((h & mid_mask) != id) return std::unexpected(CodecError::malformed);
    bool const t = (h & flag_x5) != 0;
    bool const e = (h & flag_x6) != 0;
    bool has_ext = (h & flag_z) != 0;

    Put p{};
    if (t) p.timestamp = ZTRY(Timestamp::decode(r));
    if (e) p.encoding = ZTRY(Encoding::decode(r));

    while (has_ext) {
        auto const eh = ZTRY(peek_ext_header(r));
        switch (eh.id) {
        case 0x1: {
            ByteReader sub{ZTRY(read_ext_zstruct(r))};
            p.sinfo = ZTRY(SourceInfo::decode_body(sub));
            break;
        }
        case 0x3:
            p.attachment = Attachment{ZTRY(read_ext_zstruct(r))};
            break;
        default:
            if (eh.mandatory) return std::unexpected(CodecError::malformed);
            ZTRY(skip_ext(r, eh.kind));
        }
        has_ext = eh.more;
    }

    p.payload = ZTRY(get_prefixed(r));
    return p;
}

// --- Del ---

auto Del::encode(ByteWriter& w) const noexcept -> std::expected<void, CodecError> {
    bool const t = timestamp.has_value();
    bool const has_si = sinfo.has_value();
    bool const has_at = attachment.has_value();
    bool const z = has_si || has_at;

    auto const h = static_cast<std::uint8_t>(id | (z ? flag_z : 0) | (t ? flag_x5 : 0));
    ZTRY(w.write_byte(static_cast<std::byte>(h)));
    if (t) ZTRY(timestamp->encode(w));
    if (has_si)
        ZTRY(put_ext_zstruct(w, 0x1, false, has_at, sinfo->body_len(),
                             [&](auto& ww) { return sinfo->encode_body(ww); }));
    if (has_at)
        ZTRY(put_ext_zstruct(w, 0x2, false, false, attachment->body_len(),
                             [&](auto& ww) { return attachment->encode_body(ww); }));
    return {};
}

auto Del::decode(ByteReader& r) noexcept -> std::expected<Del, CodecError> {
    std::uint8_t const h = std::to_integer<std::uint8_t>(ZTRY(r.read_byte()));
    if ((h & mid_mask) != id) return std::unexpected(CodecError::malformed);
    bool const t = (h & flag_x5) != 0;
    bool has_ext = (h & flag_z) != 0;

    Del d{};
    if (t) d.timestamp = ZTRY(Timestamp::decode(r));
    while (has_ext) {
        auto const eh = ZTRY(peek_ext_header(r));
        switch (eh.id) {
        case 0x1: {
            ByteReader sub{ZTRY(read_ext_zstruct(r))};
            d.sinfo = ZTRY(SourceInfo::decode_body(sub));
            break;
        }
        case 0x2:
            d.attachment = Attachment{ZTRY(read_ext_zstruct(r))};
            break;
        default:
            if (eh.mandatory) return std::unexpected(CodecError::malformed);
            ZTRY(skip_ext(r, eh.kind));
        }
        has_ext = eh.more;
    }
    return d;
}

// --- Push ---

auto Push::encode_head(ByteWriter& w) const noexcept -> std::expected<void, CodecError> {
    bool const n = wire_expr.has_suffix();
    bool const m = wire_expr.is_sender();
    bool const q = !(qos == QoS{});
    bool const ts = timestamp.has_value();
    bool const nid = !(nodeid == NodeId{});
    bool const z = q || ts || nid;

    auto const h =
        static_cast<std::uint8_t>(id | (z ? flag_z : 0) | (m ? flag_x6 : 0) | (n ? flag_x5 : 0));
    ZTRY(w.write_byte(static_cast<std::byte>(h)));
    ZTRY(wire_expr.encode_body(w));

    if (q) ZTRY(put_ext_u64(w, 0x1, false, /*more=*/ts || nid, qos.inner));
    if (ts)
        ZTRY(put_ext_zstruct(w, 0x2, false, /*more=*/nid, timestamp->encoded_len(),
                             [&](auto& ww) { return timestamp->encode(ww); }));
    if (nid) ZTRY(put_ext_u64(w, 0x3, /*mandatory=*/true, /*more=*/false, nodeid.node_id));

    return payload.encode_head(w);
}

auto Push::encode(ByteWriter& w) const noexcept -> std::expected<void, CodecError> {
    ZTRY(encode_head(w));
    return put_raw(w, payload.trailing_payload());
}

auto Push::decode(ByteReader& r) noexcept -> std::expected<Push, CodecError> {
    std::uint8_t const h = std::to_integer<std::uint8_t>(ZTRY(r.read_byte()));
    if ((h & mid_mask) != id) return std::unexpected(CodecError::malformed);
    bool const m = (h & flag_x6) != 0;
    bool const n = (h & flag_x5) != 0;
    bool has_ext = (h & flag_z) != 0;

    Push p{};
    p.wire_expr = ZTRY(WireExpr::decode_body(r, m, n));

    while (has_ext) {
        auto const eh = ZTRY(peek_ext_header(r));
        switch (eh.id) {
        case 0x1:
            p.qos.inner = ZTRY(read_ext_uint<std::uint8_t>(r));
            break;
        case 0x2: {
            ByteReader sub{ZTRY(read_ext_zstruct(r))};
            p.timestamp = ZTRY(Timestamp::decode(sub));
            break;
        }
        case 0x3:
            p.nodeid.node_id = ZTRY(read_ext_uint<std::uint16_t>(r));
            break;
        default:
            if (eh.mandatory) return std::unexpected(CodecError::malformed);
            ZTRY(skip_ext(r, eh.kind));
        }
        has_ext = eh.more;
    }

    p.payload = ZTRY(PushBody::decode(r));
    return p;
}

// --- Query ---

auto Query::encode(ByteWriter& w) const noexcept -> std::expected<void, CodecError> {
    bool const c = consolidation != ConsolidationMode::automatic;
    bool const p = !parameters.empty();
    bool const e_si = sinfo.has_value();
    bool const e_body = body.has_value();
    bool const e_att = attachment.has_value();
    bool const z = e_si || e_body || e_att;

    auto const h =
        static_cast<std::uint8_t>(id | (z ? flag_z : 0) | (p ? flag_x6 : 0) | (c ? flag_x5 : 0));
    ZTRY(w.write_byte(static_cast<std::byte>(h)));
    if (c) ZTRY(w.write_byte(static_cast<std::byte>(static_cast<std::uint8_t>(consolidation))));
    if (p) ZTRY(put_prefixed_str(w, parameters));

    if (e_si)
        ZTRY(put_ext_zstruct(w, 0x1, false, e_body || e_att, sinfo->body_len(),
                             [&](auto& ww) { return sinfo->encode_body(ww); }));
    if (e_body)
        ZTRY(put_ext_zstruct(w, 0x3, false, e_att, body->body_len(),
                             [&](auto& ww) { return body->encode_body(ww); }));
    if (e_att)
        ZTRY(put_ext_zstruct(w, 0x5, false, false, attachment->body_len(),
                             [&](auto& ww) { return attachment->encode_body(ww); }));
    return {};
}

auto Query::decode(ByteReader& r) noexcept -> std::expected<Query, CodecError> {
    std::uint8_t const h = std::to_integer<std::uint8_t>(ZTRY(r.read_byte()));
    if ((h & mid_mask) != id) return std::unexpected(CodecError::malformed);
    bool const c = (h & flag_x5) != 0;
    bool const p = (h & flag_x6) != 0;
    bool has_ext = (h & flag_z) != 0;

    Query q{};
    if (c) {
        auto const cv = std::to_integer<std::uint8_t>(ZTRY(r.read_byte()));
        if (cv > 3) return std::unexpected(CodecError::malformed);
        q.consolidation = static_cast<ConsolidationMode>(cv);
    }
    if (p) q.parameters = ZTRY(get_prefixed_str(r));

    while (has_ext) {
        auto const eh = ZTRY(peek_ext_header(r));
        switch (eh.id) {
        case 0x1: {
            ByteReader sub{ZTRY(read_ext_zstruct(r))};
            q.sinfo = ZTRY(SourceInfo::decode_body(sub));
            break;
        }
        case 0x3: {
            ByteReader sub{ZTRY(read_ext_zstruct(r))};
            q.body = ZTRY(Value::decode_body(sub));
            break;
        }
        case 0x5:
            q.attachment = Attachment{ZTRY(read_ext_zstruct(r))};
            break;
        default:
            if (eh.mandatory) return std::unexpected(CodecError::malformed);
            ZTRY(skip_ext(r, eh.kind));
        }
        has_ext = eh.more;
    }
    return q;
}

// --- Err ---

auto Err::encode(ByteWriter& w) const noexcept -> std::expected<void, CodecError> {
    bool const e = !(encoding == Encoding{});
    bool const e_si = sinfo.has_value();
    auto const h = static_cast<std::uint8_t>(id | (e_si ? flag_z : 0) | (e ? flag_x6 : 0));
    ZTRY(w.write_byte(static_cast<std::byte>(h)));
    if (e) ZTRY(encoding.encode(w));
    if (e_si)
        ZTRY(put_ext_zstruct(w, 0x1, false, false, sinfo->body_len(),
                             [&](auto& ww) { return sinfo->encode_body(ww); }));
    return put_prefixed(w, payload);
}

auto Err::decode(ByteReader& r) noexcept -> std::expected<Err, CodecError> {
    std::uint8_t const h = std::to_integer<std::uint8_t>(ZTRY(r.read_byte()));
    if ((h & mid_mask) != id) return std::unexpected(CodecError::malformed);
    bool const e = (h & flag_x6) != 0;
    bool has_ext = (h & flag_z) != 0;

    Err x{};
    if (e) x.encoding = ZTRY(Encoding::decode(r));
    while (has_ext) {
        auto const eh = ZTRY(peek_ext_header(r));
        switch (eh.id) {
        case 0x1: {
            ByteReader sub{ZTRY(read_ext_zstruct(r))};
            x.sinfo = ZTRY(SourceInfo::decode_body(sub));
            break;
        }
        default:
            if (eh.mandatory) return std::unexpected(CodecError::malformed);
            ZTRY(skip_ext(r, eh.kind));
        }
        has_ext = eh.more;
    }
    x.payload = ZTRY(get_prefixed(r));
    return x;
}

// --- Reply ---

auto Reply::encode(ByteWriter& w) const noexcept -> std::expected<void, CodecError> {
    bool const c = consolidation != ConsolidationMode::automatic;
    auto const h = static_cast<std::uint8_t>(id | (c ? flag_x5 : 0));
    ZTRY(w.write_byte(static_cast<std::byte>(h)));
    if (c) ZTRY(w.write_byte(static_cast<std::byte>(static_cast<std::uint8_t>(consolidation))));
    return payload.encode(w);
}

auto Reply::decode(ByteReader& r) noexcept -> std::expected<Reply, CodecError> {
    std::uint8_t const h = std::to_integer<std::uint8_t>(ZTRY(r.read_byte()));
    if ((h & mid_mask) != id) return std::unexpected(CodecError::malformed);
    bool const c = (h & flag_x5) != 0;
    bool has_ext = (h & flag_z) != 0;

    Reply rep{};
    if (c) {
        auto const cv = std::to_integer<std::uint8_t>(ZTRY(r.read_byte()));
        if (cv > 3) return std::unexpected(CodecError::malformed);
        rep.consolidation = static_cast<ConsolidationMode>(cv);
    }
    // Reply defines no extensions; tolerate (skip) any a peer may add.
    while (has_ext) {
        auto const eh = ZTRY(peek_ext_header(r));
        if (eh.mandatory) return std::unexpected(CodecError::malformed);
        ZTRY(skip_ext(r, eh.kind));
        has_ext = eh.more;
    }
    rep.payload = ZTRY(PushBody::decode(r));
    return rep;
}

// --- Request ---

auto Request::encode(ByteWriter& w) const noexcept -> std::expected<void, CodecError> {
    bool const m = wire_expr.is_sender();
    bool const n = wire_expr.has_suffix();
    bool const e_qos = !(qos == QoS{});
    bool const e_ts = timestamp.has_value();
    bool const e_nid = !(nodeid == NodeId{});
    bool const e_tgt = target != QueryTarget::best_matching;
    bool const e_bud = budget.has_value();
    bool const e_to = timeout.has_value();
    bool const z = e_qos || e_ts || e_nid || e_tgt || e_bud || e_to;

    auto const h =
        static_cast<std::uint8_t>(mid | (z ? flag_z : 0) | (m ? flag_x6 : 0) | (n ? flag_x5 : 0));
    ZTRY(w.write_byte(static_cast<std::byte>(h)));
    ZTRY(put_uint(w, id));
    ZTRY(wire_expr.encode_body(w));

    // The reference writes nodeid (id 3) LAST, after target/budget/timeout.
    if (e_qos) ZTRY(put_ext_u64(w, 0x1, false, e_ts || e_tgt || e_bud || e_to || e_nid, qos.inner));
    if (e_ts)
        ZTRY(put_ext_zstruct(w, 0x2, false, e_tgt || e_bud || e_to || e_nid,
                             timestamp->encoded_len(),
                             [&](auto& ww) { return timestamp->encode(ww); }));
    if (e_tgt)
        ZTRY(put_ext_u64(w, 0x4, true, e_bud || e_to || e_nid, static_cast<std::uint8_t>(target)));
    if (e_bud) ZTRY(put_ext_u64(w, 0x5, false, e_to || e_nid, *budget));
    if (e_to) ZTRY(put_ext_u64(w, 0x6, false, e_nid, timeout->millis()));
    if (e_nid) ZTRY(put_ext_u64(w, 0x3, true, false, nodeid.node_id));

    return payload.encode(w);
}

auto Request::decode(ByteReader& r) noexcept -> std::expected<Request, CodecError> {
    std::uint8_t const h = std::to_integer<std::uint8_t>(ZTRY(r.read_byte()));
    if ((h & mid_mask) != mid) return std::unexpected(CodecError::malformed);
    bool const m = (h & flag_x6) != 0;
    bool const n = (h & flag_x5) != 0;
    bool has_ext = (h & flag_z) != 0;

    Request req{};
    req.id = ZTRY(get_uint_as<std::uint32_t>(r));
    req.wire_expr = ZTRY(WireExpr::decode_body(r, m, n));

    while (has_ext) {
        auto const eh = ZTRY(peek_ext_header(r));
        switch (eh.id) {
        case 0x1:
            req.qos.inner = ZTRY(read_ext_uint<std::uint8_t>(r));
            break;
        case 0x2: {
            ByteReader sub{ZTRY(read_ext_zstruct(r))};
            req.timestamp = ZTRY(Timestamp::decode(sub));
            break;
        }
        case 0x3:
            req.nodeid.node_id = ZTRY(read_ext_uint<std::uint16_t>(r));
            break;
        case 0x4: {
            auto const tv = ZTRY(read_ext_u64(r));
            if (tv > 2) return std::unexpected(CodecError::malformed);
            req.target = static_cast<QueryTarget>(tv);
            break;
        }
        case 0x5:
            req.budget = ZTRY(read_ext_uint<std::uint32_t>(r));
            break;
        case 0x6:
            req.timeout = Duration::from_millis(ZTRY(read_ext_u64(r)));
            break;
        default:
            if (eh.mandatory) return std::unexpected(CodecError::malformed);
            ZTRY(skip_ext(r, eh.kind));
        }
        has_ext = eh.more;
    }

    req.payload = ZTRY(RequestBody::decode(r));
    return req;
}

// --- Response ---

auto Response::encode(ByteWriter& w) const noexcept -> std::expected<void, CodecError> {
    bool const m = wire_expr.is_sender();
    bool const n = wire_expr.has_suffix();
    bool const e_qos = !(qos == QoS{});
    bool const e_ts = timestamp.has_value();
    bool const e_rid = respid.has_value();
    bool const z = e_qos || e_ts || e_rid;

    auto const h =
        static_cast<std::uint8_t>(id | (z ? flag_z : 0) | (m ? flag_x6 : 0) | (n ? flag_x5 : 0));
    ZTRY(w.write_byte(static_cast<std::byte>(h)));
    ZTRY(put_uint(w, rid));
    ZTRY(wire_expr.encode_body(w));

    if (e_qos) ZTRY(put_ext_u64(w, 0x1, false, e_ts || e_rid, qos.inner));
    if (e_ts)
        ZTRY(put_ext_zstruct(w, 0x2, false, e_rid, timestamp->encoded_len(),
                             [&](auto& ww) { return timestamp->encode(ww); }));
    if (e_rid)
        ZTRY(put_ext_zstruct(w, 0x3, false, false, respid->body_len(),
                             [&](auto& ww) { return respid->encode_body(ww); }));

    return payload.encode(w);
}

auto Response::decode(ByteReader& r) noexcept -> std::expected<Response, CodecError> {
    std::uint8_t const h = std::to_integer<std::uint8_t>(ZTRY(r.read_byte()));
    if ((h & mid_mask) != id) return std::unexpected(CodecError::malformed);
    bool const m = (h & flag_x6) != 0;
    bool const n = (h & flag_x5) != 0;
    bool has_ext = (h & flag_z) != 0;

    Response rsp{};
    rsp.rid = ZTRY(get_uint_as<std::uint32_t>(r));
    rsp.wire_expr = ZTRY(WireExpr::decode_body(r, m, n));

    while (has_ext) {
        auto const eh = ZTRY(peek_ext_header(r));
        switch (eh.id) {
        case 0x1:
            rsp.qos.inner = ZTRY(read_ext_uint<std::uint8_t>(r));
            break;
        case 0x2: {
            ByteReader sub{ZTRY(read_ext_zstruct(r))};
            rsp.timestamp = ZTRY(Timestamp::decode(sub));
            break;
        }
        case 0x3: {
            ByteReader sub{ZTRY(read_ext_zstruct(r))};
            rsp.respid = ZTRY(EntityGlobalId::decode_body(sub));
            break;
        }
        default:
            if (eh.mandatory) return std::unexpected(CodecError::malformed);
            ZTRY(skip_ext(r, eh.kind));
        }
        has_ext = eh.more;
    }

    rsp.payload = ZTRY(ResponseBody::decode(r));
    return rsp;
}

// --- ResponseFinal ---

auto ResponseFinal::encode(ByteWriter& w) const noexcept -> std::expected<void, CodecError> {
    bool const e_qos = !(qos == QoS{});
    bool const e_ts = timestamp.has_value();
    bool const z = e_qos || e_ts;

    auto const h = static_cast<std::uint8_t>(id | (z ? flag_z : 0));
    ZTRY(w.write_byte(static_cast<std::byte>(h)));
    ZTRY(put_uint(w, rid));

    if (e_qos) ZTRY(put_ext_u64(w, 0x1, false, e_ts, qos.inner));
    if (e_ts)
        ZTRY(put_ext_zstruct(w, 0x2, false, false, timestamp->encoded_len(),
                             [&](auto& ww) { return timestamp->encode(ww); }));
    return {};
}

auto ResponseFinal::decode(ByteReader& r) noexcept -> std::expected<ResponseFinal, CodecError> {
    std::uint8_t const h = std::to_integer<std::uint8_t>(ZTRY(r.read_byte()));
    if ((h & mid_mask) != id) return std::unexpected(CodecError::malformed);
    bool has_ext = (h & flag_z) != 0;

    ResponseFinal rf{};
    rf.rid = ZTRY(get_uint_as<std::uint32_t>(r));
    while (has_ext) {
        auto const eh = ZTRY(peek_ext_header(r));
        switch (eh.id) {
        case 0x1:
            rf.qos.inner = ZTRY(read_ext_uint<std::uint8_t>(r));
            break;
        case 0x2: {
            ByteReader sub{ZTRY(read_ext_zstruct(r))};
            rf.timestamp = ZTRY(Timestamp::decode(sub));
            break;
        }
        default:
            if (eh.mandatory) return std::unexpected(CodecError::malformed);
            ZTRY(skip_ext(r, eh.kind));
        }
        has_ext = eh.more;
    }
    return rf;
}

} // namespace zenoh

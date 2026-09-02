module;

#include <cstddef>
#include <cstdint>
#include <expected>
#include <optional>
#include <utility>

#include "zenoh/detail/try.hpp"

module zenoh.proto.declare;

import zenoh.buffer;
import zenoh.util;
import zenoh.codec;
import zenoh.codec.ext;
import zenoh.proto.fields;
import zenoh.proto.exts;

// Implementation unit for zenoh.proto.declare: per-body and Declare codecs. The
// DeclareBody variant dispatcher stays inline in declare.cppm.
namespace zenoh {
namespace {

// Encode a body that carries an optional WireExpr as the mandatory ext 0x0f.
[[nodiscard]] auto put_optional_wire_expr_ext(ByteWriter& w, std::uint8_t msg_id, std::uint32_t id,
                                              const std::optional<WireExpr>& we) noexcept
    -> std::expected<void, CodecError> {
    auto const h =
        static_cast<std::uint8_t>(unsigned{msg_id} | flag_if(we.has_value(), declare_flag::z));
    ZTRY(w.write_byte(static_cast<std::byte>(h)));
    ZTRY(put_uint(w, id));
    if (we)
        ZTRY(put_ext_zstruct(w, 0x0f, /*mandatory=*/true, false, we->full_len(),
                             [&](auto& ww) { return we->encode_full(ww); }));
    return {};
}

// Decode id (u32) + optional WireExpr ext 0x0f.
[[nodiscard]] auto get_id_and_optional_wire_expr(ByteReader& r, std::uint8_t msg_id) noexcept
    -> std::expected<std::pair<std::uint32_t, std::optional<WireExpr>>, CodecError> {
    std::uint8_t const h = std::to_integer<std::uint8_t>(ZTRY(r.read_byte()));
    if ((h & mid_mask) != msg_id) return std::unexpected(CodecError::malformed);
    bool has_ext = (h & declare_flag::z) != 0;
    auto const id = ZTRY(get_uint_as<std::uint32_t>(r));
    std::optional<WireExpr> we{};
    while (has_ext) {
        auto const eh = ZTRY(peek_ext_header(r));
        if (eh.id == 0x0f) {
            ByteReader sub{ZTRY(read_ext_zstruct(r))};
            we = ZTRY(WireExpr::decode_full(sub));
        } else if (eh.mandatory) {
            return std::unexpected(CodecError::malformed);
        } else {
            ZTRY(skip_ext(r, eh.kind));
        }
        has_ext = eh.more;
    }
    return std::pair{id, we};
}

} // namespace

auto DeclareKeyExpr::encode(ByteWriter& w) const noexcept -> std::expected<void, CodecError> {
    auto const h =
        static_cast<std::uint8_t>(unsigned{mid} | flag_if(wire_expr.is_sender(), declare_flag::m) |
                                  flag_if(wire_expr.has_suffix(), declare_flag::n));
    ZTRY(w.write_byte(static_cast<std::byte>(h)));
    ZTRY(put_uint(w, id));
    return wire_expr.encode_body(w);
}

auto DeclareKeyExpr::decode(ByteReader& r) noexcept -> std::expected<DeclareKeyExpr, CodecError> {
    std::uint8_t const h = std::to_integer<std::uint8_t>(ZTRY(r.read_byte()));
    if ((h & mid_mask) != mid) return std::unexpected(CodecError::malformed);
    DeclareKeyExpr d{};
    d.id = ZTRY(get_uint_as<std::uint16_t>(r));
    d.wire_expr =
        ZTRY(WireExpr::decode_body(r, (h & declare_flag::m) != 0, (h & declare_flag::n) != 0));
    return d;
}

auto UndeclareKeyExpr::encode(ByteWriter& w) const noexcept -> std::expected<void, CodecError> {
    ZTRY(w.write_byte(static_cast<std::byte>(mid)));
    return put_uint(w, id);
}

auto UndeclareKeyExpr::decode(ByteReader& r) noexcept
    -> std::expected<UndeclareKeyExpr, CodecError> {
    std::uint8_t const h = std::to_integer<std::uint8_t>(ZTRY(r.read_byte()));
    if ((h & mid_mask) != mid) return std::unexpected(CodecError::malformed);
    UndeclareKeyExpr d{};
    d.id = ZTRY(get_uint_as<std::uint16_t>(r));
    return d;
}

auto DeclareSubscriber::encode(ByteWriter& w) const noexcept -> std::expected<void, CodecError> {
    auto const h =
        static_cast<std::uint8_t>(unsigned{mid} | flag_if(wire_expr.is_sender(), declare_flag::m) |
                                  flag_if(wire_expr.has_suffix(), declare_flag::n));
    ZTRY(w.write_byte(static_cast<std::byte>(h)));
    ZTRY(put_uint(w, id));
    return wire_expr.encode_body(w);
}

auto DeclareSubscriber::decode(ByteReader& r) noexcept
    -> std::expected<DeclareSubscriber, CodecError> {
    std::uint8_t const h = std::to_integer<std::uint8_t>(ZTRY(r.read_byte()));
    if ((h & mid_mask) != mid) return std::unexpected(CodecError::malformed);
    DeclareSubscriber d{};
    d.id = ZTRY(get_uint_as<std::uint32_t>(r));
    d.wire_expr =
        ZTRY(WireExpr::decode_body(r, (h & declare_flag::m) != 0, (h & declare_flag::n) != 0));
    return d;
}

auto UndeclareSubscriber::encode(ByteWriter& w) const noexcept -> std::expected<void, CodecError> {
    return put_optional_wire_expr_ext(w, mid, id, wire_expr);
}

auto UndeclareSubscriber::decode(ByteReader& r) noexcept
    -> std::expected<UndeclareSubscriber, CodecError> {
    auto const [id, we] = ZTRY(get_id_and_optional_wire_expr(r, mid));
    return UndeclareSubscriber{.id = id, .wire_expr = we};
}

auto DeclareQueryable::encode(ByteWriter& w) const noexcept -> std::expected<void, CodecError> {
    bool const e_qi = !(qinfo == QueryableInfo{});
    auto const h = static_cast<std::uint8_t>(unsigned{mid} | flag_if(e_qi, declare_flag::z) |
                                             flag_if(wire_expr.is_sender(), declare_flag::m) |
                                             flag_if(wire_expr.has_suffix(), declare_flag::n));
    ZTRY(w.write_byte(static_cast<std::byte>(h)));
    ZTRY(put_uint(w, id));
    ZTRY(wire_expr.encode_body(w));
    if (e_qi) ZTRY(put_ext_u64(w, 0x1, false, false, qinfo.as_u64()));
    return {};
}

auto DeclareQueryable::decode(ByteReader& r) noexcept
    -> std::expected<DeclareQueryable, CodecError> {
    std::uint8_t const h = std::to_integer<std::uint8_t>(ZTRY(r.read_byte()));
    if ((h & mid_mask) != mid) return std::unexpected(CodecError::malformed);
    bool has_ext = (h & declare_flag::z) != 0;
    DeclareQueryable d{};
    d.id = ZTRY(get_uint_as<std::uint32_t>(r));
    d.wire_expr =
        ZTRY(WireExpr::decode_body(r, (h & declare_flag::m) != 0, (h & declare_flag::n) != 0));
    while (has_ext) {
        auto const eh = ZTRY(peek_ext_header(r));
        if (eh.id == 0x1) {
            d.qinfo = QueryableInfo::from_u64(ZTRY(read_ext_u64(r)));
        } else if (eh.mandatory) {
            return std::unexpected(CodecError::malformed);
        } else {
            ZTRY(skip_ext(r, eh.kind));
        }
        has_ext = eh.more;
    }
    return d;
}

auto UndeclareQueryable::encode(ByteWriter& w) const noexcept -> std::expected<void, CodecError> {
    return put_optional_wire_expr_ext(w, mid, id, wire_expr);
}

auto UndeclareQueryable::decode(ByteReader& r) noexcept
    -> std::expected<UndeclareQueryable, CodecError> {
    auto const [id, we] = ZTRY(get_id_and_optional_wire_expr(r, mid));
    return UndeclareQueryable{.id = id, .wire_expr = we};
}

auto DeclareToken::encode(ByteWriter& w) const noexcept -> std::expected<void, CodecError> {
    auto const h =
        static_cast<std::uint8_t>(unsigned{mid} | flag_if(wire_expr.is_sender(), declare_flag::m) |
                                  flag_if(wire_expr.has_suffix(), declare_flag::n));
    ZTRY(w.write_byte(static_cast<std::byte>(h)));
    ZTRY(put_uint(w, id));
    return wire_expr.encode_body(w);
}

auto DeclareToken::decode(ByteReader& r) noexcept -> std::expected<DeclareToken, CodecError> {
    std::uint8_t const h = std::to_integer<std::uint8_t>(ZTRY(r.read_byte()));
    if ((h & mid_mask) != mid) return std::unexpected(CodecError::malformed);
    DeclareToken d{};
    d.id = ZTRY(get_uint_as<std::uint32_t>(r));
    d.wire_expr =
        ZTRY(WireExpr::decode_body(r, (h & declare_flag::m) != 0, (h & declare_flag::n) != 0));
    return d;
}

auto UndeclareToken::encode(ByteWriter& w) const noexcept -> std::expected<void, CodecError> {
    return put_optional_wire_expr_ext(w, mid, id, wire_expr);
}

auto UndeclareToken::decode(ByteReader& r) noexcept -> std::expected<UndeclareToken, CodecError> {
    auto const [id, we] = ZTRY(get_id_and_optional_wire_expr(r, mid));
    return UndeclareToken{.id = id, .wire_expr = we};
}

auto DeclareFinal::encode(ByteWriter& w) const noexcept -> std::expected<void, CodecError> {
    return w.write_byte(static_cast<std::byte>(mid));
}

auto DeclareFinal::decode(ByteReader& r) noexcept -> std::expected<DeclareFinal, CodecError> {
    std::uint8_t const h = std::to_integer<std::uint8_t>(ZTRY(r.read_byte()));
    if ((h & mid_mask) != mid) return std::unexpected(CodecError::malformed);
    return DeclareFinal{};
}

auto Declare::encode(ByteWriter& w) const noexcept -> std::expected<void, CodecError> {
    bool const has_id = id.has_value();
    bool const e_qos = !(qos == QoS{});
    bool const e_ts = timestamp.has_value();
    bool const e_nid = !(nodeid == NodeId{});
    bool const z = e_qos || e_ts || e_nid;

    auto const h = static_cast<std::uint8_t>(unsigned{mid} | flag_if(z, declare_flag::z) |
                                             flag_if(has_id, declare_flag::i));
    ZTRY(w.write_byte(static_cast<std::byte>(h)));
    if (has_id) ZTRY(put_uint(w, *id));

    if (e_qos) ZTRY(put_ext_u64(w, 0x1, false, e_ts || e_nid, qos.inner));
    if (e_ts)
        ZTRY(put_ext_zstruct(w, 0x2, false, e_nid, timestamp->encoded_len(),
                             [&](auto& ww) { return timestamp->encode(ww); }));
    if (e_nid) ZTRY(put_ext_u64(w, 0x3, true, false, nodeid.node_id));

    return body.encode(w);
}

auto Declare::decode(ByteReader& r) noexcept -> std::expected<Declare, CodecError> {
    std::uint8_t const h = std::to_integer<std::uint8_t>(ZTRY(r.read_byte()));
    if ((h & mid_mask) != mid) return std::unexpected(CodecError::malformed);
    bool const has_id = (h & declare_flag::i) != 0;
    bool has_ext = (h & declare_flag::z) != 0;

    Declare d{};
    if (has_id) d.id = ZTRY(get_uint_as<std::uint32_t>(r));
    while (has_ext) {
        auto const eh = ZTRY(peek_ext_header(r));
        switch (eh.id) {
        case 0x1:
            d.qos.inner = ZTRY(read_ext_uint<std::uint8_t>(r));
            break;
        case 0x2: {
            ByteReader sub{ZTRY(read_ext_zstruct(r))};
            d.timestamp = ZTRY(Timestamp::decode(sub));
            break;
        }
        case 0x3:
            d.nodeid.node_id = ZTRY(read_ext_uint<std::uint16_t>(r));
            break;
        default:
            if (eh.mandatory) return std::unexpected(CodecError::malformed);
            ZTRY(skip_ext(r, eh.kind));
        }
        has_ext = eh.more;
    }
    d.body = ZTRY(DeclareBody::decode(r));
    return d;
}

} // namespace zenoh

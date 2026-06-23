module;

#include <cstddef>
#include <cstdint>
#include <expected>
#include <optional>
#include <type_traits>
#include <variant>

#include "zenoh/detail/try.hpp"

export module zenoh.proto.declare;

import zenoh.buffer;
import zenoh.util;
import zenoh.codec;
import zenoh.codec.ext;
import zenoh.proto.fields;
import zenoh.proto.exts;

// The Declare message and its 9 declaration bodies. Per-body and Declare
// `encode`/`decode` bodies live in declare.cpp; the `DeclareBody` variant
// dispatcher and `operator==` stay inline here.
export namespace zenoh {

/// Header flag bits used by Declare and its bodies (bits 4:0 are the message id).
namespace declare_flag {
inline constexpr std::uint8_t z = 0x80; ///< extensions follow
inline constexpr std::uint8_t m = 0x40; ///< wire_expr mapping
inline constexpr std::uint8_t n = 0x20; ///< wire_expr suffix
inline constexpr std::uint8_t i = 0x20; ///< Declare: id present
} // namespace declare_flag

/// 0x00 — bind a numeric id to a key expression.
struct DeclareKeyExpr {
    std::uint16_t id = 0;
    WireExpr wire_expr{};
    static constexpr std::uint8_t mid = 0x00;
    auto operator==(const DeclareKeyExpr&) const -> bool = default;

    [[nodiscard]] auto encode(ByteWriter& w) const noexcept -> std::expected<void, CodecError>;
    [[nodiscard]] static auto decode(ByteReader& r) noexcept -> std::expected<DeclareKeyExpr, CodecError>;
};

/// 0x01 — release a previously declared key-expression id.
struct UndeclareKeyExpr {
    std::uint16_t id = 0;
    static constexpr std::uint8_t mid = 0x01;
    auto operator==(const UndeclareKeyExpr&) const -> bool = default;

    [[nodiscard]] auto encode(ByteWriter& w) const noexcept -> std::expected<void, CodecError>;
    [[nodiscard]] static auto decode(ByteReader& r) noexcept -> std::expected<UndeclareKeyExpr, CodecError>;
};

/// 0x02 — declare a subscriber on a key expression.
struct DeclareSubscriber {
    std::uint32_t id = 0;
    WireExpr wire_expr{};
    static constexpr std::uint8_t mid = 0x02;
    auto operator==(const DeclareSubscriber&) const -> bool = default;

    [[nodiscard]] auto encode(ByteWriter& w) const noexcept -> std::expected<void, CodecError>;
    [[nodiscard]] static auto decode(ByteReader& r) noexcept -> std::expected<DeclareSubscriber, CodecError>;
};

/// 0x03 — undeclare a subscriber (optionally echoing its key expression, ext 0x0f).
struct UndeclareSubscriber {
    std::uint32_t id = 0;
    std::optional<WireExpr> wire_expr{};
    static constexpr std::uint8_t mid = 0x03;
    auto operator==(const UndeclareSubscriber& o) const noexcept -> bool {
        return id == o.id && wire_expr_eq(wire_expr, o.wire_expr);
    }

    [[nodiscard]] auto encode(ByteWriter& w) const noexcept -> std::expected<void, CodecError>;
    [[nodiscard]] static auto decode(ByteReader& r) noexcept -> std::expected<UndeclareSubscriber, CodecError>;
};

/// 0x04 — declare a queryable on a key expression (with completeness/distance info).
struct DeclareQueryable {
    std::uint32_t id = 0;
    WireExpr wire_expr{};
    QueryableInfo qinfo{}; ///< ext 0x01 (U64), default-elided
    static constexpr std::uint8_t mid = 0x04;
    auto operator==(const DeclareQueryable&) const -> bool = default;

    [[nodiscard]] auto encode(ByteWriter& w) const noexcept -> std::expected<void, CodecError>;
    [[nodiscard]] static auto decode(ByteReader& r) noexcept -> std::expected<DeclareQueryable, CodecError>;
};

/// 0x05 — undeclare a queryable (optionally echoing its key expression, ext 0x0f).
struct UndeclareQueryable {
    std::uint32_t id = 0;
    std::optional<WireExpr> wire_expr{};
    static constexpr std::uint8_t mid = 0x05;
    auto operator==(const UndeclareQueryable& o) const noexcept -> bool {
        return id == o.id && wire_expr_eq(wire_expr, o.wire_expr);
    }

    [[nodiscard]] auto encode(ByteWriter& w) const noexcept -> std::expected<void, CodecError>;
    [[nodiscard]] static auto decode(ByteReader& r) noexcept -> std::expected<UndeclareQueryable, CodecError>;
};

/// 0x06 — declare a liveliness token on a key expression.
struct DeclareToken {
    std::uint32_t id = 0;
    WireExpr wire_expr{};
    static constexpr std::uint8_t mid = 0x06;
    auto operator==(const DeclareToken&) const -> bool = default;

    [[nodiscard]] auto encode(ByteWriter& w) const noexcept -> std::expected<void, CodecError>;
    [[nodiscard]] static auto decode(ByteReader& r) noexcept -> std::expected<DeclareToken, CodecError>;
};

/// 0x07 — undeclare a token (optionally echoing its key expression, ext 0x0f).
struct UndeclareToken {
    std::uint32_t id = 0;
    std::optional<WireExpr> wire_expr{};
    static constexpr std::uint8_t mid = 0x07;
    auto operator==(const UndeclareToken& o) const noexcept -> bool {
        return id == o.id && wire_expr_eq(wire_expr, o.wire_expr);
    }

    [[nodiscard]] auto encode(ByteWriter& w) const noexcept -> std::expected<void, CodecError>;
    [[nodiscard]] static auto decode(ByteReader& r) noexcept -> std::expected<UndeclareToken, CodecError>;
};

/// 0x1a — marks the end of a declare exchange.
struct DeclareFinal {
    static constexpr std::uint8_t mid = 0x1a;
    auto operator==(const DeclareFinal&) const -> bool = default;

    [[nodiscard]] auto encode(ByteWriter& w) const noexcept -> std::expected<void, CodecError>;
    [[nodiscard]] static auto decode(ByteReader& r) noexcept -> std::expected<DeclareFinal, CodecError>;
};

/// The declaration carried by a Declare, dispatched by the inner message id.
/// (Inline: the variant visitor is generic over alternatives.)
struct DeclareBody {
    std::variant<DeclareKeyExpr, UndeclareKeyExpr, DeclareSubscriber, UndeclareSubscriber,
                 DeclareQueryable, UndeclareQueryable, DeclareToken, UndeclareToken, DeclareFinal>
        body{DeclareFinal{}};
    auto operator==(const DeclareBody& o) const noexcept -> bool {
        if (body.index() != o.body.index()) return false;
        return std::visit(
            [&o](auto const& lhs) -> bool {
                using T = std::decay_t<decltype(lhs)>;
                return lhs == std::get<T>(o.body);
            },
            body);
    }

    [[nodiscard]] auto encode(ByteWriter& w) const noexcept -> std::expected<void, CodecError> {
        return std::visit([&](auto const& m) { return m.encode(w); }, body);
    }
    [[nodiscard]] static auto decode(ByteReader& r) noexcept -> std::expected<DeclareBody, CodecError> {
        std::uint8_t const mid = std::to_integer<std::uint8_t>(ZTRY(r.peek())) & mid_mask;
        DeclareBody b{};
        switch (mid) {
            case DeclareKeyExpr::mid: b.body = ZTRY(DeclareKeyExpr::decode(r)); break;
            case UndeclareKeyExpr::mid: b.body = ZTRY(UndeclareKeyExpr::decode(r)); break;
            case DeclareSubscriber::mid: b.body = ZTRY(DeclareSubscriber::decode(r)); break;
            case UndeclareSubscriber::mid: b.body = ZTRY(UndeclareSubscriber::decode(r)); break;
            case DeclareQueryable::mid: b.body = ZTRY(DeclareQueryable::decode(r)); break;
            case UndeclareQueryable::mid: b.body = ZTRY(UndeclareQueryable::decode(r)); break;
            case DeclareToken::mid: b.body = ZTRY(DeclareToken::decode(r)); break;
            case UndeclareToken::mid: b.body = ZTRY(UndeclareToken::decode(r)); break;
            case DeclareFinal::mid: b.body = ZTRY(DeclareFinal::decode(r)); break;
            default: return std::unexpected(CodecError::malformed);
        }
        return b;
    }
};

/// `Declare` (0x1e): wraps a declaration body with optional id + common exts.
/// Wire: `Z|_|I|ID:5=0x1e` ++ [id if I] ++ exts ++ DeclareBody.
struct Declare {
    std::optional<std::uint32_t> id{};
    QoS qos{};                            ///< ext 0x1 (U64), default-elided
    std::optional<Timestamp> timestamp{}; ///< ext 0x2 (ZStruct)
    NodeId nodeid{};                      ///< ext 0x3 (U64, mandatory), default-elided
    DeclareBody body{};

    static constexpr std::uint8_t mid = 0x1e;
    auto operator==(const Declare&) const -> bool = default;

    [[nodiscard]] auto encode(ByteWriter& w) const noexcept -> std::expected<void, CodecError>;
    [[nodiscard]] static auto decode(ByteReader& r) noexcept -> std::expected<Declare, CodecError>;
};

} // namespace zenoh

module;

#include <cstddef>
#include <cstdint>
#include <expected>
#include <optional>

export module zenoh.proto.interest;

import zenoh.buffer;
import zenoh.util;
import zenoh.codec;
import zenoh.codec.ext;
import zenoh.proto.fields;

// Interest / InterestFinal and their inner options. encode/decode bodies live in
// interest.cpp; operator== stays inline.
export namespace zenoh {

/// The body of an Interest. Its single header byte is the `options` bitfield
/// (K=0, S=1, Q=2, T=3, A=7), into which the optional wire_expr also folds: the R
/// bit (4) marks its presence and the M/N bits (6/5) carry its mapping/suffix.
struct InterestInner {
    std::uint8_t options = 0;
    std::optional<WireExpr> wire_expr{};

    /// Bits that belong to `options` proper (the rest are derived from wire_expr).
    static constexpr std::uint8_t opt_mask = 0b1000'1111; ///< K,S,Q,T + A
    static constexpr std::uint8_t flag_r = 0x10;          ///< wire_expr present
    static constexpr std::uint8_t flag_n = 0x20;          ///< wire_expr suffix
    static constexpr std::uint8_t flag_m = 0x40;          ///< wire_expr mapping

    auto operator==(const InterestInner& o) const noexcept -> bool {
        return (options & opt_mask) == (o.options & opt_mask) && wire_expr_eq(wire_expr, o.wire_expr);
    }

    [[nodiscard]] auto encode(ByteWriter& w) const noexcept -> std::expected<void, CodecError>;
    [[nodiscard]] static auto decode(ByteReader& r) noexcept -> std::expected<InterestInner, CodecError>;
};

/// `Interest` (0x19, MODE != 0): subscribe to declarations. Wire:
/// `Z|MODE:2|ID:5=0x19` ++ id ++ InterestInner ++ exts.
struct Interest {
    std::uint32_t id = 0;
    InterestMode mode = InterestMode::final_;
    InterestInner inner{};
    QoS qos{};                            ///< ext 0x1 (U64), default-elided
    std::optional<Timestamp> timestamp{}; ///< ext 0x2 (ZStruct)
    NodeId nodeid{};                      ///< ext 0x3 (U64, mandatory), default-elided

    static constexpr std::uint8_t mid = 0x19;
    static constexpr std::uint8_t flag_z = 0x80;
    auto operator==(const Interest&) const -> bool = default;

    [[nodiscard]] auto encode(ByteWriter& w) const noexcept -> std::expected<void, CodecError>;
    [[nodiscard]] static auto decode(ByteReader& r) noexcept -> std::expected<Interest, CodecError>;
};

/// `InterestFinal` (0x19, MODE == 0): closes an interest. No inner; just id + exts.
struct InterestFinal {
    std::uint32_t id = 0;
    QoS qos{};                            ///< ext 0x1 (U64), default-elided
    std::optional<Timestamp> timestamp{}; ///< ext 0x2 (ZStruct)
    NodeId nodeid{};                      ///< ext 0x3 (U64, mandatory), default-elided

    static constexpr std::uint8_t mid = 0x19;
    static constexpr std::uint8_t flag_z = 0x80;
    static constexpr std::uint8_t mode_mask = 0x60; ///< bits 6:5 must be 0
    auto operator==(const InterestFinal&) const -> bool = default;

    [[nodiscard]] auto encode(ByteWriter& w) const noexcept -> std::expected<void, CodecError>;
    [[nodiscard]] static auto decode(ByteReader& r) noexcept -> std::expected<InterestFinal, CodecError>;
};

} // namespace zenoh

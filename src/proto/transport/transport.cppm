module;

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <optional>
#include <ranges>
#include <span>

export module zenoh.proto.transport;

import zenoh.buffer;
import zenoh.util;
import zenoh.codec;
import zenoh.codec.ext;
import zenoh.proto.fields;

// Transport-handshake messages and their field/extension types. Message and
// identifier `encode`/`decode` bodies live in transport.cpp; data-only types and
// `operator==`/`encoded_len` stay inline here.
export namespace zenoh {

// --- transport-handshake field/extension types ---

/// Node role + Zenoh id. Own header byte: `ZID:4|_:2|W:2` (zid length is len-1 in
/// the high nibble, whatami in the low 2 bits).
struct InitIdentifier {
    WhatAmI whatami = WhatAmI::client;
    ZenohId zid{};
    auto operator==(const InitIdentifier&) const -> bool = default;

    /// Encoded length: header byte + zid bytes.
    [[nodiscard]] auto encoded_len() const noexcept -> std::size_t { return 1 + zid.len; }
    [[nodiscard]] auto encode(ByteWriter& w) const noexcept -> std::expected<void, CodecError>;
    [[nodiscard]] static auto decode(ByteReader& r) noexcept
        -> std::expected<InitIdentifier, CodecError>;
};

/// Negotiated SN/request-id resolution byte + little-endian batch size. The
/// defaults below match the reference (Resolution 0x0a, BatchSize u16::MAX).
struct InitResolution {
    std::uint8_t resolution = 0x0a;
    std::uint16_t batch_size = 0xffff;
    auto operator==(const InitResolution&) const -> bool = default;

    [[nodiscard]] auto encode(ByteWriter& w) const noexcept -> std::expected<void, CodecError>;
    [[nodiscard]] static auto decode(ByteReader& r) noexcept
        -> std::expected<InitResolution, CodecError>;
};

/// Marker (Unit) extension: QoS supported.
struct HasQoS {
    auto operator==(const HasQoS&) const -> bool = default;
};
/// Marker (Unit) extension: low-latency supported.
struct HasLowLatency {
    auto operator==(const HasLowLatency&) const -> bool = default;
};
/// Marker (Unit) extension: compression supported.
struct HasCompression {
    auto operator==(const HasCompression&) const -> bool = default;
};

/// Per-link QoS budget (U64 extension).
struct QoSLink {
    std::uint64_t qos = 0;
    auto operator==(const QoSLink&) const -> bool = default;
};

/// Protocol patch level (U64 extension); 0 == "none".
struct Patch {
    std::uint8_t value = 0;
    auto operator==(const Patch&) const -> bool = default;
};

/// Opaque authentication payload (ZStruct extension, body = slice).
struct Auth {
    std::span<const std::byte> payload{};
    auto operator==(const Auth& o) const noexcept -> bool {
        return std::ranges::equal(payload, o.payload);
    }
};
/// Multilink payload (ZStruct extension, body = slice).
struct MultiLink {
    std::span<const std::byte> payload{};
    auto operator==(const MultiLink& o) const noexcept -> bool {
        return std::ranges::equal(payload, o.payload);
    }
};
/// Multilink-syn payload (ZStruct extension, body = slice).
struct MultiLinkSyn {
    std::span<const std::byte> payload{};
    auto operator==(const MultiLinkSyn& o) const noexcept -> bool {
        return std::ranges::equal(payload, o.payload);
    }
};
/// Marker (Unit) extension: multilink ack.
struct HasMultiLinkAck {
    auto operator==(const HasMultiLinkAck&) const -> bool = default;
};

/// How a Close applies, carried in the S header bit.
enum class CloseBehaviour : std::uint8_t { link = 0, session = 1 };
// `Duration` is shared and lives in zenoh.proto.fields.

/// `InitAck` (0x01, A=1): the listener's reply in the transport handshake.
///
/// Wire: `Z|S|A=1|ID:5=0x1` ++ version ++ identifier ++ [resolution if S] ++
/// cookie(prefixed) ++ exts. Note exts `qos` (Unit) and `qos_link` (U64) share
/// id 0x1 and are disambiguated by kind.
struct InitAck {
    std::uint8_t version = 0;
    InitIdentifier identifier{};
    InitResolution resolution{};
    std::span<const std::byte> cookie{};

    std::optional<HasQoS> qos{};                 ///< ext 0x1, Unit
    std::optional<QoSLink> qos_link{};           ///< ext 0x1, U64
    std::optional<Auth> auth{};                  ///< ext 0x3, ZStruct
    std::optional<MultiLink> mlink{};            ///< ext 0x4, ZStruct
    std::optional<HasLowLatency> lowlatency{};   ///< ext 0x5, Unit
    std::optional<HasCompression> compression{}; ///< ext 0x6, Unit
    Patch patch{};                               ///< ext 0x7, U64, default-elided

    static constexpr std::uint8_t id = 0x01;
    static constexpr std::uint8_t flag_a = 0x20; ///< bit 5, fixed 1 for Ack
    static constexpr std::uint8_t flag_s = 0x40; ///< bit 6, resolution present
    static constexpr std::uint8_t flag_z = 0x80; ///< bit 7, extensions follow

    auto operator==(const InitAck& o) const noexcept -> bool {
        return version == o.version && identifier == o.identifier && resolution == o.resolution &&
               std::ranges::equal(cookie, o.cookie) && qos == o.qos && qos_link == o.qos_link &&
               auth == o.auth && mlink == o.mlink && lowlatency == o.lowlatency &&
               compression == o.compression && patch == o.patch;
    }

    [[nodiscard]] auto encode(ByteWriter& w) const noexcept -> std::expected<void, CodecError>;
    [[nodiscard]] static auto decode(ByteReader& r) noexcept -> std::expected<InitAck, CodecError>;
};

/// `InitSyn` (0x01, A=0): the initiator's first transport-handshake message.
/// Identical to `InitAck` minus the cookie (and with the Ack bit clear).
struct InitSyn {
    std::uint8_t version = 0;
    InitIdentifier identifier{};
    InitResolution resolution{};

    std::optional<HasQoS> qos{};
    std::optional<QoSLink> qos_link{};
    std::optional<Auth> auth{};
    std::optional<MultiLink> mlink{};
    std::optional<HasLowLatency> lowlatency{};
    std::optional<HasCompression> compression{};
    Patch patch{};

    static constexpr std::uint8_t id = 0x01;
    static constexpr std::uint8_t flag_a = 0x20; ///< bit 5, must be 0 for Syn
    static constexpr std::uint8_t flag_s = 0x40;
    static constexpr std::uint8_t flag_z = 0x80;

    auto operator==(const InitSyn&) const -> bool = default; // no cookie => no spans

    [[nodiscard]] auto encode(ByteWriter& w) const noexcept -> std::expected<void, CodecError>;
    [[nodiscard]] static auto decode(ByteReader& r) noexcept -> std::expected<InitSyn, CodecError>;
};

/// `Close` (0x03): tear down a link or session with a reason code. The close
/// behaviour (Link vs Session) rides in the S header bit; the reason is the body.
struct Close {
    std::uint8_t reason = 0;
    CloseBehaviour behaviour = CloseBehaviour::link;

    static constexpr std::uint8_t id = 0x03;
    static constexpr std::uint8_t flag_s = 0x20;
    auto operator==(const Close&) const -> bool = default;

    [[nodiscard]] auto encode(ByteWriter& w) const noexcept -> std::expected<void, CodecError>;
    [[nodiscard]] static auto decode(ByteReader& r) noexcept -> std::expected<Close, CodecError>;
};

/// `KeepAlive` (0x04): a single-byte heartbeat.
struct KeepAlive {
    static constexpr std::uint8_t id = 0x04;
    auto operator==(const KeepAlive&) const -> bool = default;

    [[nodiscard]] auto encode(ByteWriter& w) const noexcept -> std::expected<void, CodecError>;
    [[nodiscard]] static auto decode(ByteReader& r) noexcept
        -> std::expected<KeepAlive, CodecError>;
};

/// `FrameHeader` (0x05): precedes a batch of network messages. Reliability rides
/// in the R header bit; QoS is a (mandatory, default-elided) U64 extension.
struct FrameHeader {
    Reliability reliability = Reliability::reliable;
    std::uint32_t sn = 0;
    QoS qos{};

    static constexpr std::uint8_t id = 0x05;
    static constexpr std::uint8_t flag_r = 0x20;
    static constexpr std::uint8_t flag_z = 0x80;
    auto operator==(const FrameHeader&) const -> bool = default;

    [[nodiscard]] auto encode(ByteWriter& w) const noexcept -> std::expected<void, CodecError>;
    [[nodiscard]] static auto decode(ByteReader& r) noexcept
        -> std::expected<FrameHeader, CodecError>;
};

/// `OpenSyn` (0x02, A=0): completes the handshake from the initiator. Lease is a
/// flattened Duration (seconds-flag in the T header bit). exts `mlink_syn`
/// (ZStruct) and `mlink_ack` (Unit) share id 0x4, disambiguated by kind.
struct OpenSyn {
    Duration lease{};
    std::uint32_t sn = 0;
    std::span<const std::byte> cookie{};

    std::optional<HasQoS> qos{};
    std::optional<Auth> auth{};
    std::optional<MultiLinkSyn> mlink_syn{};
    std::optional<HasMultiLinkAck> mlink_ack{};
    std::optional<HasLowLatency> lowlatency{};
    std::optional<HasCompression> compression{};

    static constexpr std::uint8_t id = 0x02;
    static constexpr std::uint8_t flag_a = 0x20; ///< bit 5, must be 0 for Syn
    static constexpr std::uint8_t flag_t = 0x40; ///< lease in seconds
    static constexpr std::uint8_t flag_z = 0x80;

    auto operator==(const OpenSyn& o) const noexcept -> bool {
        return lease == o.lease && sn == o.sn && std::ranges::equal(cookie, o.cookie) &&
               qos == o.qos && auth == o.auth && mlink_syn == o.mlink_syn &&
               mlink_ack == o.mlink_ack && lowlatency == o.lowlatency &&
               compression == o.compression;
    }

    [[nodiscard]] auto encode(ByteWriter& w) const noexcept -> std::expected<void, CodecError>;
    [[nodiscard]] static auto decode(ByteReader& r) noexcept -> std::expected<OpenSyn, CodecError>;
};

/// `OpenAck` (0x02, A=1): the listener's reply to OpenSyn. Identical to OpenSyn
/// minus the cookie (and with the Ack bit set).
struct OpenAck {
    Duration lease{};
    std::uint32_t sn = 0;

    std::optional<HasQoS> qos{};
    std::optional<Auth> auth{};
    std::optional<MultiLinkSyn> mlink_syn{};
    std::optional<HasMultiLinkAck> mlink_ack{};
    std::optional<HasLowLatency> lowlatency{};
    std::optional<HasCompression> compression{};

    static constexpr std::uint8_t id = 0x02;
    static constexpr std::uint8_t flag_a = 0x20; ///< bit 5, must be 1 for Ack
    static constexpr std::uint8_t flag_t = 0x40;
    static constexpr std::uint8_t flag_z = 0x80;

    auto operator==(const OpenAck&) const -> bool = default; // no cookie => no spans

    [[nodiscard]] auto encode(ByteWriter& w) const noexcept -> std::expected<void, CodecError>;
    [[nodiscard]] static auto decode(ByteReader& r) noexcept -> std::expected<OpenAck, CodecError>;
};

} // namespace zenoh

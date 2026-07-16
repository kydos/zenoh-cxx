module;

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <ranges>
#include <span>

export module zenoh.proto.exts;

import zenoh.buffer;
import zenoh.util;
import zenoh.codec;
import zenoh.proto.fields;

// Structured (ZStruct) extension bodies. Dependency direction is exts -> fields,
// never the reverse (PLAN.md §5.7). `body_len`/`operator==` are inline here; the
// `encode_body`/`decode_body` bodies live in exts.cpp.
export namespace zenoh {

/// Globally-unique entity id: a Zenoh id (length carried in the high nibble of a
/// leading header byte) plus a 32-bit entity id.
struct EntityGlobalId {
    ZenohId zid{};
    std::uint32_t eid = 0;
    auto operator==(const EntityGlobalId&) const -> bool = default;

    /// Encoded body length: header byte + zid bytes + VLE(eid).
    [[nodiscard]] auto body_len() const noexcept -> std::size_t {
        return 1 + zid.len + len_uint(eid);
    }
    /// Encode: header nibble (`zid.len - 1`) ++ zid bytes ++ VLE(eid).
    [[nodiscard]] auto encode_body(ByteWriter& w) const noexcept -> std::expected<void, CodecError>;
    /// Decode the above (zid length = header nibble + 1).
    [[nodiscard]] static auto decode_body(ByteReader& r) noexcept
        -> std::expected<EntityGlobalId, CodecError>;
};

/// A zenoh-cxx-specific routing extension carried on `Push`/`Request`
/// (ZStruct, project-local — not present in upstream zenoh-rust): narrows
/// broker fan-out to the single peer whose zid matches, ANDed with normal
/// key-expression-declaration matching (a filter, never a bypass — the
/// target must still have a matching Subscriber/Queryable declared). Absent
/// = today's unfiltered fan-out; the extension id is non-mandatory, so an
/// older peer that doesn't know it simply skips it via `skip_ext`. Because
/// it doesn't exist upstream, it cannot be differentially tested against the
/// reference — round-trip/property tests carry that burden instead.
struct DestinationId {
    ZenohId zid{};
    auto operator==(const DestinationId&) const -> bool = default;

    /// Encoded body length: header byte + zid bytes (same shape as
    /// `EntityGlobalId` minus the trailing `eid`).
    [[nodiscard]] auto body_len() const noexcept -> std::size_t { return 1 + zid.len; }
    /// Encode: header nibble (`zid.len - 1`) ++ zid bytes.
    [[nodiscard]] auto encode_body(ByteWriter& w) const noexcept -> std::expected<void, CodecError>;
    /// Decode the above (zid length = header nibble + 1).
    [[nodiscard]] static auto decode_body(ByteReader& r) noexcept
        -> std::expected<DestinationId, CodecError>;
};

/// Originating entity + sequence number (ZStruct extension).
struct SourceInfo {
    EntityGlobalId id{};
    std::uint32_t sn = 0;
    auto operator==(const SourceInfo&) const -> bool = default;

    /// Encoded body length.
    [[nodiscard]] auto body_len() const noexcept -> std::size_t {
        return id.body_len() + len_uint(sn);
    }
    /// Encode: entity id ++ VLE(sn).
    [[nodiscard]] auto encode_body(ByteWriter& w) const noexcept -> std::expected<void, CodecError>;
    /// Decode the above.
    [[nodiscard]] static auto decode_body(ByteReader& r) noexcept
        -> std::expected<SourceInfo, CodecError>;
};

/// Opaque user attachment (ZStruct extension whose body is "rest of the slice").
struct Attachment {
    std::span<const std::byte> buffer{};
    auto operator==(const Attachment& o) const noexcept -> bool {
        return std::ranges::equal(buffer, o.buffer);
    }

    /// Encoded body length (raw bytes, no prefix — the ext is length-delimited).
    [[nodiscard]] auto body_len() const noexcept -> std::size_t { return buffer.size(); }
    /// Encode: the raw buffer.
    [[nodiscard]] auto encode_body(ByteWriter& w) const noexcept -> std::expected<void, CodecError>;
};

/// A query request body: an encoding followed by a "rest of the slice" payload
/// (ZStruct extension).
struct Value {
    Encoding encoding{};
    std::span<const std::byte> payload{};
    auto operator==(const Value& o) const noexcept -> bool {
        return encoding == o.encoding && std::ranges::equal(payload, o.payload);
    }

    /// Encoded body length.
    [[nodiscard]] auto body_len() const noexcept -> std::size_t {
        return encoding.encoded_len() + payload.size();
    }
    /// Encode: encoding ++ raw payload.
    [[nodiscard]] auto encode_body(ByteWriter& w) const noexcept -> std::expected<void, CodecError>;
    /// Decode the above (payload is the remainder of the ext slice).
    [[nodiscard]] static auto decode_body(ByteReader& r) noexcept
        -> std::expected<Value, CodecError>;
};

/// Queryable advertisement info (U64 extension): `complete` flag in bit 0,
/// `distance` in bits 8..23. (Kept inline: pure bit-packing arithmetic.)
struct QueryableInfo {
    bool complete = false;
    std::uint16_t distance = 0;
    auto operator==(const QueryableInfo&) const -> bool = default;

    /// Pack into the U64 ext value.
    [[nodiscard]] auto as_u64() const noexcept -> std::uint64_t {
        return (complete ? 1ull : 0ull) | (static_cast<std::uint64_t>(distance) << 8);
    }
    /// Unpack from the U64 ext value.
    static auto from_u64(std::uint64_t v) noexcept -> QueryableInfo {
        return QueryableInfo{.complete = (v & 1) != 0,
                             .distance = static_cast<std::uint16_t>((v >> 8) & 0xffff)};
    }
};

} // namespace zenoh

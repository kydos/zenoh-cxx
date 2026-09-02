module;

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <optional>
#include <ranges>
#include <span>
#include <string_view>

#include "zenoh/detail/try.hpp"

export module zenoh.proto.fields;

import zenoh.buffer;
import zenoh.util;
import zenoh.codec;

// Shared value types used across messages. Borrow-only: byte/text fields are views
// into the source buffer (D2). The hot `*_len`/accessor methods are inline here;
// the (larger) `encode`/`decode`/`*_body` bodies live in fields.cpp.
export namespace zenoh {

/// Whether a WireExpr mapping was declared by the sender or the receiver.
enum class Mapping : std::uint8_t { receiver = 0, sender = 1 };

/// The role a node plays, as carried in the low 2 bits of an InitIdentifier header.
enum class WhatAmI : std::uint8_t { router = 0, peer = 1, client = 2 };

/// Delivery guarantee, carried in a header flag bit on a Frame.
enum class Reliability : std::uint8_t { best_effort = 0, reliable = 1 };

/// Mask for the 5-bit message id in a header byte (bits 4:0); bits 7:5 are flags.
inline constexpr std::uint8_t mid_mask = 0x1f;

/// True when `mid` (already masked with `mid_mask`) names a *network* message.
///
/// The protocol keeps the network ids (0x19..0x1f: Interest, ResponseFinal, Response,
/// Request, Push, Declare, OAM) disjoint from the transport ids (0x00..0x07: OAM,
/// Init, Open, Close, KeepAlive, Frame, Fragment, Join) precisely so the two can be
/// told apart by inspection -- the reference implementation marks the constraint with
/// a "WARNING: it's crucial that these IDs do NOT collide" on both id tables.
///
/// That is what makes a batch decodable: a TCP batch is a *sequence* of transport
/// messages, and a Frame's body runs to the first byte that is not a network message,
/// not to the end of the batch. A receiver walks the batch, and when it is inside a
/// frame it ends that frame the moment this returns false, then resumes reading
/// transport messages from the same byte. (zenoh-rust reaches the same point from the
/// other side: `Frame::read` tries to decode a network message and rewinds the reader
/// when it fails.) See `docs/RUNTIME.md` and `docs/BROKER.md`.
[[nodiscard]] inline constexpr auto is_network_mid(std::uint8_t mid) noexcept -> bool {
    return mid >= 0x19 && mid <= 0x1f;
}

/// Query reply-consolidation strategy (1 wire byte when present).
enum class ConsolidationMode : std::uint8_t { automatic = 0, none = 1, monotonic = 2, latest = 3 };

/// Which matching queryables a request targets (U64 extension value).
enum class QueryTarget : std::uint8_t { best_matching = 0, all = 1, all_complete = 2 };

/// Interest temporal scope, carried in 2 header bits.
enum class InterestMode : std::uint8_t { final_ = 0, current = 1, future = 2, current_future = 3 };

/// A lease/timeout duration. Stored in its wire form (a value plus a "this value
/// is whole seconds" flag) so it round-trips losslessly, including large second
/// counts that would overflow a milliseconds-only representation. Two wire uses:
///  - flattened (Open* lease): the T parent-header bit carries `seconds`, the body
///    is VLE(wire_value());
///  - U64 extension (Request timeout): always VLE(millis()).
/// (Kept fully inline: a tiny value type used on the lease hot path.)
struct Duration {
    std::uint64_t value = 0;
    bool seconds = false;
    auto operator==(const Duration&) const -> bool = default;

    /// Canonicalize a millisecond count into the shorter wire form (matching how
    /// the reference elides whole seconds).
    [[nodiscard]] static auto from_millis(std::uint64_t ms) noexcept -> Duration {
        if (ms % 1000 == 0) return Duration{.value = ms / 1000, .seconds = true};
        return Duration{.value = ms, .seconds = false};
    }
    /// Whether the wire value is whole seconds (sets the parent T flag).
    [[nodiscard]] auto is_seconds() const noexcept -> bool { return seconds; }
    /// The VLE body value (seconds or milliseconds per `seconds`).
    [[nodiscard]] auto wire_value() const noexcept -> std::uint64_t { return value; }
    /// Milliseconds, truncated to u64 (matching the reference's `as_millis() as u64`).
    /// The seconds->millis scale is computed in 128-bit so a large second count
    /// truncates like the reference rather than wrapping mid-multiply in u64.
    [[nodiscard]] auto millis() const noexcept -> std::uint64_t {
        return seconds ? static_cast<std::uint64_t>(static_cast<unsigned __int128>(value) * 1000)
                       : value;
    }
    /// Decode a flattened lease body, with `secs` taken from the parent T flag.
    [[nodiscard]] static auto decode_value(ByteReader& r, bool secs) noexcept
        -> std::expected<Duration, CodecError> {
        return Duration{.value = ZTRY(get_uint(r)), .seconds = secs};
    }
};

/// Quality-of-service flags packed in one byte. Wire form is a U64 extension
/// carrying `inner` (priority in bits 2:0, D=block bit 3, E=express bit 4).
struct QoS {
    std::uint8_t inner = 5; ///< default: Priority::Data(5), Drop, not express
    auto operator==(const QoS&) const -> bool = default;
};

/// Routing node id. Wire form is a U64 extension carrying `node_id`.
struct NodeId {
    std::uint16_t node_id = 0;
    auto operator==(const NodeId&) const -> bool = default;
};

/// A Zenoh id: 1..16 little-endian bytes, zero-padded so equality is by value.
struct ZenohId {
    std::array<std::byte, 16> bytes{};
    std::uint8_t len = 0;
    /// The significant `len` bytes.
    [[nodiscard]] auto view() const noexcept -> std::span<const std::byte> {
        return {bytes.data(), len};
    }
    auto operator==(const ZenohId&) const -> bool = default;
};

/// uhlc timestamp: a 64-bit NTP time plus a 1..16-byte source id.
struct Timestamp {
    std::uint64_t time = 0;
    std::array<std::byte, 16> id{};
    std::uint8_t id_len = 0;
    auto operator==(const Timestamp&) const -> bool = default;

    /// Encoded length: VLE(time) + VLE(id_len) + id bytes.
    ///
    /// Meaningful only for an `id_len` in 1..16 — i.e. one `encode` would accept.
    [[nodiscard]] auto encoded_len() const noexcept -> std::size_t {
        return len_uint(time) + len_uint(id_len) + id_len;
    }
    /// Encode: VLE(time) ++ VLE(id_len) ++ id bytes.
    ///
    /// `malformed` if `id_len` is not in 1..16 — it indexes the fixed 16-byte `id`,
    /// so an out-of-range value would otherwise read past the array.
    [[nodiscard]] auto encode(ByteWriter& w) const noexcept -> std::expected<void, CodecError>;
    /// Decode the above; rejects an `id_len` outside 1..16 as malformed.
    [[nodiscard]] static auto decode(ByteReader& r) noexcept
        -> std::expected<Timestamp, CodecError>;
};

/// Payload encoding: a 13-bit-ish id plus an optional schema. On the wire the id
/// is `(id << 1) | has_schema`; the schema, if present, is length-prefixed.
struct Encoding {
    std::uint16_t id = 0;
    bool has_schema = false;
    std::span<const std::byte> schema{};

    auto operator==(const Encoding& o) const noexcept -> bool {
        return id == o.id && has_schema == o.has_schema &&
               (!has_schema || std::ranges::equal(schema, o.schema));
    }

    /// Encoded length (hot path: used to size the parent before writing).
    [[nodiscard]] auto encoded_len() const noexcept -> std::size_t {
        std::uint64_t const combined =
            (static_cast<std::uint64_t>(id) << 1) | (has_schema ? 1u : 0u);
        return len_uint(combined) + (has_schema ? len_prefixed(schema) : 0);
    }
    /// Encode the `(id<<1)|has_schema` word, then the length-prefixed schema if any.
    [[nodiscard]] auto encode(ByteWriter& w) const noexcept -> std::expected<void, CodecError>;
    /// Decode the above.
    [[nodiscard]] static auto decode(ByteReader& r) noexcept -> std::expected<Encoding, CodecError>;
};

/// A wire key expression: numeric scope + optional textual suffix. It is usually
/// "flattened" into a parent message header (mapping M and suffix-presence N flags
/// live in the parent header byte), but can also be a standalone extension body
/// (the `*_full` form, with its own flags byte and a raw — not length-prefixed —
/// suffix that fills the length-delimited ext body).
struct WireExpr {
    std::uint16_t scope = 0;
    Mapping mapping = Mapping::receiver;
    std::string_view suffix{};
    auto operator==(const WireExpr&) const -> bool = default;

    /// Whether a non-empty suffix is present (the N flag).
    [[nodiscard]] auto has_suffix() const noexcept -> bool { return !suffix.empty(); }
    /// Whether the mapping is sender-declared (the M flag).
    [[nodiscard]] auto is_sender() const noexcept -> bool { return mapping == Mapping::sender; }

    // --- flattened form (flags supplied by the parent header) ---

    /// Flattened-body encoded length.
    [[nodiscard]] auto body_len() const noexcept -> std::size_t {
        return len_uint(scope) + (has_suffix() ? len_prefixed_str(suffix) : 0);
    }
    /// Encode the flattened body: VLE(scope) ++ [length-prefixed suffix if present].
    [[nodiscard]] auto encode_body(ByteWriter& w) const noexcept -> std::expected<void, CodecError>;
    /// Decode the flattened body; `m`/`n` are the parent header's M/N flags.
    [[nodiscard]] static auto decode_body(ByteReader& r, bool m, bool n) noexcept
        -> std::expected<WireExpr, CodecError>;

    // --- standalone extension-body form ---

    /// Standalone-body encoded length: flags byte + VLE(scope) + raw suffix.
    [[nodiscard]] auto full_len() const noexcept -> std::size_t {
        return 1 + len_uint(scope) + (has_suffix() ? suffix.size() : 0);
    }
    /// Encode the standalone form (flags byte, scope, raw suffix).
    [[nodiscard]] auto encode_full(ByteWriter& w) const noexcept -> std::expected<void, CodecError>;
    /// Decode the standalone form. `r` must be bounded to exactly the ext body, so
    /// the suffix is "the rest of the reader" (UTF-8 validated).
    [[nodiscard]] static auto decode_full(ByteReader& r) noexcept
        -> std::expected<WireExpr, CodecError>;
};

/// Compare two optional WireExprs. (A direct `optional<WireExpr> ==` is ambiguous
/// under libc++ when the element type has a defaulted `operator==`.)
[[nodiscard]] inline auto wire_expr_eq(const std::optional<WireExpr>& a,
                                       const std::optional<WireExpr>& b) noexcept -> bool {
    return a.has_value() == b.has_value() && (!a.has_value() || *a == *b);
}

} // namespace zenoh

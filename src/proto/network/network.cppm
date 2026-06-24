module;

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <optional>
#include <ranges>
#include <span>
#include <string_view>
#include <type_traits>
#include <variant>

#include "zenoh/detail/try.hpp"

export module zenoh.proto.network;

import zenoh.buffer;
import zenoh.util;
import zenoh.codec;
import zenoh.codec.ext;
import zenoh.proto.fields;
import zenoh.proto.exts;

// Network messages and the data/reply/request/response bodies. Message `encode`/
// `decode` bodies live in network.cpp; the body-dispatch wrappers (PushBody,
// RequestBody, ResponseBody) and `operator==` stay inline here (the variant
// `std::visit` is generic over alternatives and must be visible).
export namespace zenoh {

/// Network message header flag bits (bits 7:5; bits 4:0 are the message id).
inline constexpr std::uint8_t flag_z = 0x80;  ///< extensions follow
inline constexpr std::uint8_t flag_x6 = 0x40; ///< bit 6 (M / E depending on message)
inline constexpr std::uint8_t flag_x5 = 0x20; ///< bit 5 (N / T / C depending on message)
// `mid_mask` comes from zenoh.proto.fields.

/// `Put` (0x01): the data payload carried by Push/Reply.
///
/// Wire: `Z|E|T|ID:5=0x1` ++ [timestamp if T] ++ [encoding if E] ++ exts ++ payload.
struct Put {
    std::optional<Timestamp> timestamp{};
    Encoding encoding{};
    std::optional<SourceInfo> sinfo{};      ///< ext 0x1
    std::optional<Attachment> attachment{}; ///< ext 0x3
    std::span<const std::byte> payload{};   ///< length-prefixed

    static constexpr std::uint8_t id = 0x1;

    auto operator==(const Put& o) const noexcept -> bool {
        return timestamp == o.timestamp && encoding == o.encoding && sinfo == o.sinfo &&
               attachment == o.attachment && std::ranges::equal(payload, o.payload);
    }

    /// Encode everything except the trailing payload *bytes* — i.e. the header,
    /// extensions, and the payload length prefix. Lets a caller append the (borrowed)
    /// payload separately (e.g. scatter-gather I/O) instead of copying it into the
    /// encode buffer. `encode` == `encode_head` followed by the raw payload.
    [[nodiscard]] auto encode_head(ByteWriter& w) const noexcept -> std::expected<void, CodecError>;
    [[nodiscard]] auto encode(ByteWriter& w) const noexcept -> std::expected<void, CodecError>;
    [[nodiscard]] static auto decode(ByteReader& r) noexcept -> std::expected<Put, CodecError>;
};

/// `Del` (0x02): a key-expression deletion, the other PushBody variant.
///
/// Wire: `Z|_|T|ID:5=0x2` ++ [timestamp if T] ++ exts (no payload).
struct Del {
    std::optional<Timestamp> timestamp{};
    std::optional<SourceInfo> sinfo{};      ///< ext 0x1
    std::optional<Attachment> attachment{}; ///< ext 0x2 (note: 0x3 in Put)

    static constexpr std::uint8_t id = 0x2;
    auto operator==(const Del&) const -> bool = default;

    [[nodiscard]] auto encode(ByteWriter& w) const noexcept -> std::expected<void, CodecError>;
    [[nodiscard]] static auto decode(ByteReader& r) noexcept -> std::expected<Del, CodecError>;
};

/// Body carried by a Push (and a Reply): a `Put` or a `Del`, dispatched by the
/// inner message id. (Inline: the variant visitor is generic over alternatives.)
struct PushBody {
    std::variant<Put, Del> body{Put{}};

    auto operator==(const PushBody& o) const noexcept -> bool {
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
    /// Encode the body up to (but not including) the trailing payload bytes; pair with
    /// `trailing_payload()` for scatter-gather. Only `Put` defers a payload — `Del`
    /// has none, so its `encode_head` is a full encode.
    [[nodiscard]] auto encode_head(ByteWriter& w) const noexcept -> std::expected<void, CodecError> {
        if (auto const* p = std::get_if<Put>(&body)) return p->encode_head(w);
        return std::visit([&](auto const& m) { return m.encode(w); }, body);
    }
    /// The deferred trailing payload bytes (empty for bodies that have none).
    [[nodiscard]] auto trailing_payload() const noexcept -> std::span<const std::byte> {
        if (auto const* p = std::get_if<Put>(&body)) return p->payload;
        return {};
    }
    [[nodiscard]] static auto decode(ByteReader& r) noexcept -> std::expected<PushBody, CodecError> {
        std::uint8_t const mid = std::to_integer<std::uint8_t>(ZTRY(r.peek())) & mid_mask;
        PushBody b{};
        if (mid == Put::id) {
            b.body = ZTRY(Put::decode(r));
        } else if (mid == Del::id) {
            b.body = ZTRY(Del::decode(r));
        } else {
            return std::unexpected(CodecError::malformed);
        }
        return b;
    }
};

/// `Push` (0x1d): unidirectional data delivery.
///
/// Wire: `Z|M|N|ID:5=0x1d` ++ wire_expr body ++ exts ++ PushBody.
struct Push {
    WireExpr wire_expr{};
    QoS qos{};                            ///< ext 0x1 (U64), default-elided
    std::optional<Timestamp> timestamp{}; ///< ext 0x2 (ZStruct)
    NodeId nodeid{};                      ///< ext 0x3 (U64, mandatory), default-elided
    PushBody payload{};

    static constexpr std::uint8_t id = 0x1d;
    auto operator==(const Push&) const -> bool = default;

    /// Encode everything except the trailing PushBody payload bytes (see
    /// `Put::encode_head`); the payload is `payload.trailing_payload()`.
    [[nodiscard]] auto encode_head(ByteWriter& w) const noexcept -> std::expected<void, CodecError>;
    [[nodiscard]] auto encode(ByteWriter& w) const noexcept -> std::expected<void, CodecError>;
    [[nodiscard]] static auto decode(ByteReader& r) noexcept -> std::expected<Push, CodecError>;
};

/// `Query` (0x03): the request body. Wire: `Z|P|C|ID:5=0x3` ++ [consolidation if C]
/// ++ [parameters if P] ++ exts.
struct Query {
    ConsolidationMode consolidation = ConsolidationMode::automatic; ///< header C
    std::string_view parameters{};                                  ///< header P
    std::optional<SourceInfo> sinfo{};                              ///< ext 0x1
    std::optional<Value> body{};                                    ///< ext 0x3
    std::optional<Attachment> attachment{};                         ///< ext 0x5

    static constexpr std::uint8_t id = 0x3;
    auto operator==(const Query&) const -> bool = default;

    [[nodiscard]] auto encode(ByteWriter& w) const noexcept -> std::expected<void, CodecError>;
    [[nodiscard]] static auto decode(ByteReader& r) noexcept -> std::expected<Query, CodecError>;
};

/// `Err` (0x05): an error response body. Wire: `Z|E|_|ID:5=0x5` ++ [encoding if E]
/// ++ exts ++ payload(prefixed).
struct Err {
    Encoding encoding{};               ///< header E
    std::optional<SourceInfo> sinfo{}; ///< ext 0x1
    std::span<const std::byte> payload{};

    static constexpr std::uint8_t id = 0x5;
    auto operator==(const Err& o) const noexcept -> bool {
        return encoding == o.encoding && sinfo == o.sinfo && std::ranges::equal(payload, o.payload);
    }

    [[nodiscard]] auto encode(ByteWriter& w) const noexcept -> std::expected<void, CodecError>;
    [[nodiscard]] static auto decode(ByteReader& r) noexcept -> std::expected<Err, CodecError>;
};

/// `Reply` (0x04): a reply response body. Wire: `Z|_|C|ID:5=0x4` ++ [consolidation
/// if C] ++ PushBody.
struct Reply {
    ConsolidationMode consolidation = ConsolidationMode::automatic; ///< header C
    PushBody payload{};

    static constexpr std::uint8_t id = 0x4;
    auto operator==(const Reply&) const -> bool = default;

    [[nodiscard]] auto encode(ByteWriter& w) const noexcept -> std::expected<void, CodecError>;
    [[nodiscard]] static auto decode(ByteReader& r) noexcept -> std::expected<Reply, CodecError>;
};

/// The body of a Request: today only a Query. (Inline: thin delegate.)
struct RequestBody {
    Query query{};
    auto operator==(const RequestBody&) const -> bool = default;

    [[nodiscard]] auto encode(ByteWriter& w) const noexcept -> std::expected<void, CodecError> {
        return query.encode(w);
    }
    [[nodiscard]] static auto decode(ByteReader& r) noexcept -> std::expected<RequestBody, CodecError> {
        RequestBody b{};
        b.query = ZTRY(Query::decode(r));
        return b;
    }
};

/// The body of a Response: an Err or a Reply, dispatched by the inner message id.
/// (Inline: the variant visitor is generic over alternatives.)
struct ResponseBody {
    std::variant<Err, Reply> body{Err{}};
    auto operator==(const ResponseBody&) const -> bool = default;

    [[nodiscard]] auto encode(ByteWriter& w) const noexcept -> std::expected<void, CodecError> {
        return std::visit([&](auto const& m) { return m.encode(w); }, body);
    }
    [[nodiscard]] static auto decode(ByteReader& r) noexcept -> std::expected<ResponseBody, CodecError> {
        std::uint8_t const mid = std::to_integer<std::uint8_t>(ZTRY(r.peek())) & mid_mask;
        ResponseBody b{};
        if (mid == Err::id) {
            b.body = ZTRY(Err::decode(r));
        } else if (mid == Reply::id) {
            b.body = ZTRY(Reply::decode(r));
        } else {
            return std::unexpected(CodecError::malformed);
        }
        return b;
    }
};

/// `Request` (0x1c): issues a query. Wire: `Z|M|N|ID:5=0x1c` ++ id ++ wire_expr
/// body ++ exts ++ RequestBody.
struct Request {
    std::uint32_t id = 0;
    WireExpr wire_expr{};
    QoS qos{};                            ///< ext 0x1 (U64), default-elided
    std::optional<Timestamp> timestamp{}; ///< ext 0x2 (ZStruct)
    NodeId nodeid{};                      ///< ext 0x3 (U64, mandatory), default-elided
    QueryTarget target = QueryTarget::best_matching; ///< ext 0x4 (U64, mandatory), default-elided
    std::optional<std::uint32_t> budget{};           ///< ext 0x5 (U64)
    std::optional<Duration> timeout{};               ///< ext 0x6 (U64, millis)
    RequestBody payload{};

    static constexpr std::uint8_t mid = 0x1c;
    auto operator==(const Request&) const -> bool = default;

    [[nodiscard]] auto encode(ByteWriter& w) const noexcept -> std::expected<void, CodecError>;
    [[nodiscard]] static auto decode(ByteReader& r) noexcept -> std::expected<Request, CodecError>;
};

/// `Response` (0x1b): a query response. Wire: `Z|M|N|ID:5=0x1b` ++ rid ++ wire_expr
/// body ++ exts ++ ResponseBody.
struct Response {
    std::uint32_t rid = 0;
    WireExpr wire_expr{};
    QoS qos{};                              ///< ext 0x1 (U64), default-elided
    std::optional<Timestamp> timestamp{};   ///< ext 0x2 (ZStruct)
    std::optional<EntityGlobalId> respid{}; ///< ext 0x3 (ZStruct)
    ResponseBody payload{};

    static constexpr std::uint8_t id = 0x1b;
    auto operator==(const Response&) const -> bool = default;

    [[nodiscard]] auto encode(ByteWriter& w) const noexcept -> std::expected<void, CodecError>;
    [[nodiscard]] static auto decode(ByteReader& r) noexcept -> std::expected<Response, CodecError>;
};

/// `ResponseFinal` (0x1a): marks the end of a response stream. Wire:
/// `Z|_:2|ID:5=0x1a` ++ rid ++ exts.
struct ResponseFinal {
    std::uint32_t rid = 0;
    QoS qos{};                            ///< ext 0x1 (U64), default-elided
    std::optional<Timestamp> timestamp{}; ///< ext 0x2 (ZStruct)

    static constexpr std::uint8_t id = 0x1a;
    auto operator==(const ResponseFinal&) const -> bool = default;

    [[nodiscard]] auto encode(ByteWriter& w) const noexcept -> std::expected<void, CodecError>;
    [[nodiscard]] static auto decode(ByteReader& r) noexcept
        -> std::expected<ResponseFinal, CodecError>;
};

} // namespace zenoh

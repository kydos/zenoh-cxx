#pragma once
//
// Helpers shared by the example programs: the `ZError` -> name mapping every example
// needs on its error paths, and the conversions between the runtime API's byte spans
// and ordinary strings.
//
// Include this *after* `import zenoh;` -- it names types exported by that module.
//
#include <cstddef>
#include <span>
#include <string_view>

namespace zexample {

/// Human-readable name for a `ZError`, for the examples' diagnostics.
inline auto error_name(zenoh::ZError e) -> const char* {
    switch (e) {
    case zenoh::ZError::would_block:
        return "would_block";
    case zenoh::ZError::connection_closed:
        return "connection_closed";
    case zenoh::ZError::io_error:
        return "io_error";
    case zenoh::ZError::protocol_error:
        return "protocol_error";
    case zenoh::ZError::encode_error:
        return "encode_error";
    case zenoh::ZError::bad_endpoint:
        return "bad_endpoint";
    case zenoh::ZError::already_subscribed:
        return "already_subscribed";
    case zenoh::ZError::already_queryable:
        return "already_queryable";
    case zenoh::ZError::query_timeout:
        return "query_timeout";
    }
    return "unknown";
}

/// Borrow a string's bytes as the payload span `put`/`reply` take.
inline auto as_bytes(std::string_view s) -> std::span<const std::byte> {
    return {reinterpret_cast<const std::byte*>(s.data()), s.size()};
}

/// Borrow a payload span as text. Payloads are arbitrary bytes -- the examples only
/// ever print ones they know to be strings, exactly as the reference examples do.
inline auto as_str(std::span<const std::byte> b) -> std::string_view {
    return {reinterpret_cast<const char*>(b.data()), b.size()};
}

} // namespace zexample

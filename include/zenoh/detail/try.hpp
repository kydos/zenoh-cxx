#pragma once
//
// ZTRY: short-circuit helper for std::expected, in the spirit of Rust's `?`.
//
// Relies on the GNU statement-expression extension (`({ ... })`), which clang and
// gcc both support under -std=gnu++23. This is a deliberate, documented dependency
// (see PLAN.md D1): it lets the codec read top-to-bottom without nesting.
//
//   auto value = ZTRY(decode_something(reader));   // binds the success value
//   ZTRY(write_byte(writer, b));                    // discards (expected<void>)
//
// On failure it returns std::unexpected(error) from the *enclosing* function, so
// ZTRY may only be used inside a function whose return type is a std::expected
// with a compatible error type.
//
#include <expected>
#include <utility>

#define ZTRY(expr)                                                       \
    ({                                                                   \
        auto&& _ztry_result = (expr);                                    \
        if (!_ztry_result) [[unlikely]]                                  \
            return ::std::unexpected(::std::move(_ztry_result).error()); \
        ::std::move(_ztry_result).value();                              \
    })

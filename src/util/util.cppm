module;

#include <bit>
#include <concepts>
#include <cstddef>
#include <cstdint>

export module zenoh.util;

// Foundational, dependency-free utilities: the codec error type, single-byte
// header bit-field access, and little-endian load/store helpers.
export namespace zenoh {

/// Error codes shared across the buffer and codec layers.
///
/// A flat, payload-free enum (see PLAN.md D1): keeps `std::expected` small and the
/// success path zero-cost. Diagnostic context, when wanted, lives behind a
/// release-compiled-out logging hook rather than in this type.
enum class CodecError : std::uint8_t {
    // Buffer-level (cursor under/overflow).
    src_exhausted = 10, ///< Tried to read past the end of the source buffer.
    dst_full = 11,      ///< Tried to write past the end of the destination buffer.

    // Codec-level (well-formed bytes, ill-formed message).
    malformed = 20,     ///< Bytes do not form a valid value for the target type.
    invalid_field = 21, ///< A field failed validation (e.g. non-UTF-8 text).
};

/// A named bit-field within a single header byte: bits [Shift .. Shift+Bits).
///
/// Gives protocol header flags/ids names instead of open-coded bit math. Bit 0 is
/// the least-significant bit, matching the Zenoh wire diagrams.
template <std::uint8_t Shift, std::uint8_t Bits>
    requires(Shift + Bits <= 8)
struct ByteField {
    static constexpr std::uint8_t mask = static_cast<std::uint8_t>((1u << Bits) - 1u);
    static constexpr std::uint8_t shifted_mask = static_cast<std::uint8_t>(mask << Shift);

    /// Extract this field's value from header byte `b`.
    [[nodiscard]] static constexpr auto get(std::byte b) noexcept -> std::uint8_t {
        return static_cast<std::uint8_t>((std::to_integer<std::uint8_t>(b) >> Shift) & mask);
    }

    /// Set this field's value in header byte `b`, leaving other bits untouched.
    static constexpr auto set(std::byte& b, std::uint8_t v) noexcept -> void {
        b = static_cast<std::byte>(
            (std::to_integer<std::uint8_t>(b) & static_cast<std::uint8_t>(~shifted_mask)) |
            static_cast<std::uint8_t>((v & mask) << Shift));
    }
};

/// Load an unsigned integer stored little-endian at `p` (sizeof(T) bytes).
///
/// Uses memcpy (folded to a single load) rather than a reinterpret_cast, which
/// would be UB and alignment-unsafe. Linux/macOS targets are little-endian, so the
/// byteswap branch is a correctness guard that compiles away.
template <std::unsigned_integral T>
[[nodiscard]] inline auto load_le(const std::byte* p) noexcept -> T {
    T v{};
    __builtin_memcpy(&v, p, sizeof(T));
    if constexpr (std::endian::native == std::endian::big) {
        v = std::byteswap(v);
    }
    return v;
}

/// Store an unsigned integer little-endian at `p` (sizeof(T) bytes).
template <std::unsigned_integral T> inline auto store_le(std::byte* p, T v) noexcept -> void {
    if constexpr (std::endian::native == std::endian::big) {
        v = std::byteswap(v);
    }
    __builtin_memcpy(p, &v, sizeof(T));
}

} // namespace zenoh

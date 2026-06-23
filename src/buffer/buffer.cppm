module;

#include <cstddef>
#include <expected>
#include <span>

export module zenoh.buffer;

import zenoh.util;

// The byte cursors the codec reads from and writes to. The codec operates on these
// two concrete types (not a template) because the protocol decodes a *contiguous*
// reassembled frame (PLAN.md D3); the `Readable`/`Writable` concepts remain as a
// documented contract the concrete types satisfy.
export namespace zenoh {

/// Contract for a byte reader: a cursor over a contiguous source buffer.
template <class R>
concept Readable = requires(R r, std::size_t n) {
    { r.remaining() } -> std::convertible_to<std::size_t>;
    { r.peek() } -> std::same_as<std::expected<std::byte, CodecError>>;
    { r.read_byte() } -> std::same_as<std::expected<std::byte, CodecError>>;
    { r.read_slice(n) } -> std::same_as<std::expected<std::span<const std::byte>, CodecError>>;
};

/// Contract for a byte writer: a cursor over a contiguous destination buffer.
template <class W>
concept Writable = requires(W w, std::byte b, std::span<const std::byte> src) {
    { w.remaining() } -> std::convertible_to<std::size_t>;
    { w.write_byte(b) } -> std::same_as<std::expected<void, CodecError>>;
    { w.write(src) } -> std::same_as<std::expected<void, CodecError>>;
};

/// Zero-copy reader over a borrowed `std::span<const std::byte>`. `read_slice`
/// returns a contiguous view into the source (no copy); decoded message fields
/// borrow from it, so the source must outlive them.
class ByteReader {
public:
    explicit ByteReader(std::span<const std::byte> data) noexcept
        : cur_(data.data()), end_(data.data() + data.size()) {}

    /// Bytes left to read.
    [[nodiscard]] auto remaining() const noexcept -> std::size_t {
        return static_cast<std::size_t>(end_ - cur_);
    }

    /// Return the next byte without consuming it.
    [[nodiscard]] auto peek() const noexcept -> std::expected<std::byte, CodecError> {
        if (cur_ == end_) [[unlikely]] {
            return std::unexpected(CodecError::src_exhausted);
        }
        return *cur_;
    }

    /// Consume and return the next byte.
    [[nodiscard]] auto read_byte() noexcept -> std::expected<std::byte, CodecError> {
        if (cur_ == end_) [[unlikely]] {
            return std::unexpected(CodecError::src_exhausted);
        }
        return *cur_++;
    }

    /// Borrow the next `n` bytes (no copy) and advance past them.
    [[nodiscard]] auto read_slice(std::size_t n) noexcept
        -> std::expected<std::span<const std::byte>, CodecError> {
        if (remaining() < n) [[unlikely]] {
            return std::unexpected(CodecError::src_exhausted);
        }
        std::span<const std::byte> s{cur_, n};
        cur_ += n;
        return s;
    }

    /// Copy exactly `out.size()` bytes into `out`.
    [[nodiscard]] auto read_exact(std::span<std::byte> out) noexcept
        -> std::expected<void, CodecError> {
        if (remaining() < out.size()) [[unlikely]] {
            return std::unexpected(CodecError::src_exhausted);
        }
        __builtin_memcpy(out.data(), cur_, out.size());
        cur_ += out.size();
        return {};
    }

private:
    const std::byte* cur_;
    const std::byte* end_;
};

/// Writer over a borrowed `std::span<std::byte>` destination buffer.
class ByteWriter {
public:
    explicit ByteWriter(std::span<std::byte> buf) noexcept
        : begin_(buf.data()), cur_(buf.data()), end_(buf.data() + buf.size()) {}

    /// Space left to write.
    [[nodiscard]] auto remaining() const noexcept -> std::size_t {
        return static_cast<std::size_t>(end_ - cur_);
    }

    /// Number of bytes written so far.
    [[nodiscard]] auto written() const noexcept -> std::size_t {
        return static_cast<std::size_t>(cur_ - begin_);
    }

    /// Write one byte.
    [[nodiscard]] auto write_byte(std::byte b) noexcept -> std::expected<void, CodecError> {
        if (cur_ == end_) [[unlikely]] {
            return std::unexpected(CodecError::dst_full);
        }
        *cur_++ = b;
        return {};
    }

    /// Write a span of bytes.
    [[nodiscard]] auto write(std::span<const std::byte> src) noexcept
        -> std::expected<void, CodecError> {
        if (remaining() < src.size()) [[unlikely]] {
            return std::unexpected(CodecError::dst_full);
        }
        __builtin_memcpy(cur_, src.data(), src.size());
        cur_ += src.size();
        return {};
    }

private:
    std::byte* begin_;
    std::byte* cur_;
    std::byte* end_;
};

static_assert(Readable<ByteReader>);
static_assert(Writable<ByteWriter>);

} // namespace zenoh

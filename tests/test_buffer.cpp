import zenoh.buffer;
import zenoh.util;

#include "ztest.hpp"

#include <array>
#include <cstddef>
#include <span>

using namespace zenoh;

TEST("ByteReader reads bytes and zero-copy slices in order") {
    std::array<std::byte, 4> data{std::byte{1}, std::byte{2}, std::byte{3}, std::byte{4}};
    ByteReader r{data};

    CHECK(r.remaining() == 4);

    auto b = r.read_byte();
    CHECK(b.has_value());
    CHECK(std::to_integer<int>(*b) == 1);

    auto s = r.read_slice(2);
    CHECK(s.has_value());
    CHECK(s->size() == 2);
    CHECK(s->data() == data.data() + 1); // borrows, no copy
    CHECK(std::to_integer<int>((*s)[0]) == 2);

    CHECK(r.remaining() == 1);
}

TEST("ByteReader peek does not consume") {
    std::array<std::byte, 1> data{std::byte{7}};
    ByteReader r{data};
    auto p = r.peek();
    CHECK(p.has_value() && std::to_integer<int>(*p) == 7);
    CHECK(r.remaining() == 1);
}

TEST("ByteReader reports exhaustion") {
    std::array<std::byte, 1> data{std::byte{9}};
    ByteReader r{data};
    CHECK(r.read_byte().has_value());

    auto e = r.read_byte();
    CHECK(!e.has_value());
    CHECK(e.error() == CodecError::src_exhausted);

    ByteReader r2{data};
    auto s = r2.read_slice(5);
    CHECK(!s.has_value());
    CHECK(s.error() == CodecError::src_exhausted);
}

TEST("ByteWriter writes then reports a full buffer") {
    std::array<std::byte, 2> buf{};
    ByteWriter w{buf};

    CHECK(w.write_byte(std::byte{0xAB}).has_value());
    CHECK(w.write_byte(std::byte{0xCD}).has_value());
    CHECK(w.written() == 2);
    CHECK(w.remaining() == 0);

    auto e = w.write_byte(std::byte{0x00});
    CHECK(!e.has_value());
    CHECK(e.error() == CodecError::dst_full);

    CHECK(std::to_integer<unsigned>(buf[0]) == 0xAB);
    CHECK(std::to_integer<unsigned>(buf[1]) == 0xCD);
}

TEST("ByteWriter::write copies a span and advances") {
    std::array<std::byte, 4> buf{};
    std::array<std::byte, 3> src{std::byte{1}, std::byte{2}, std::byte{3}};
    ByteWriter w{buf};

    CHECK(w.write(src).has_value());
    CHECK(w.written() == 3);

    std::array<std::byte, 2> too_big{};
    CHECK(!w.write(too_big).has_value()); // only 1 byte left
}

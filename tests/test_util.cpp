import zenoh.util;

#include "ztest.hpp"

#include <cstddef>
#include <cstdint>

using namespace zenoh;

TEST("ByteField packs message id and flags into one header byte") {
    using Mid = ByteField<0, 5>;   // bits [4:0]
    using Flags = ByteField<5, 3>; // bits [7:5]

    std::byte h{};
    Mid::set(h, 0x1d);
    Flags::set(h, 0b101);

    CHECK(Mid::get(h) == 0x1d);
    CHECK(Flags::get(h) == 0b101);
    CHECK(std::to_integer<unsigned>(h) == ((0b101u << 5) | 0x1du));
}

TEST("ByteField set overwrites only its own bits") {
    using Lo = ByteField<0, 4>;
    using Hi = ByteField<4, 4>;
    std::byte h{static_cast<std::byte>(0xFF)};
    Lo::set(h, 0x3);
    CHECK(Lo::get(h) == 0x3);
    CHECK(Hi::get(h) == 0xF); // untouched
}

TEST("store_le / load_le round-trip and byte order") {
    std::byte buf[8]{};

    store_le<std::uint16_t>(buf, 0x1234);
    CHECK(std::to_integer<unsigned>(buf[0]) == 0x34);
    CHECK(std::to_integer<unsigned>(buf[1]) == 0x12);
    CHECK(load_le<std::uint16_t>(buf) == 0x1234);

    store_le<std::uint32_t>(buf, 0xDEADBEEFu);
    CHECK(load_le<std::uint32_t>(buf) == 0xDEADBEEFu);

    store_le<std::uint64_t>(buf, 0x0102030405060708ull);
    CHECK(load_le<std::uint64_t>(buf) == 0x0102030405060708ull);
}

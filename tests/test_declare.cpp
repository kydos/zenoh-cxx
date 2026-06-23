import zenoh.proto.declare;
import zenoh.proto.interest;
import zenoh.proto.fields;
import zenoh.proto.exts;
import zenoh.buffer;
import zenoh.util;

#include "ztest.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string_view>

using namespace zenoh;

namespace {

template <class T>
auto roundtrip(const T& value) -> void {
    std::array<std::byte, 512> buf{};
    ByteWriter w{buf};
    CHECK(value.encode(w).has_value());

    ByteReader r{std::span<const std::byte>{buf.data(), w.written()}};
    auto decoded = T::decode(r);
    CHECK(decoded.has_value());
    if (decoded) {
        CHECK(*decoded == value);
        CHECK(r.remaining() == 0);
    }
}

auto we(std::string_view suffix, bool sender) -> WireExpr {
    return WireExpr{.scope = 0, .mapping = sender ? Mapping::sender : Mapping::receiver,
                    .suffix = suffix};
}

template <class Body>
auto declare_with(Body b) -> Declare {
    Declare d{};
    d.body.body = b;
    return d;
}

} // namespace

TEST("Declare bodies round-trip via the Declare wrapper") {
    roundtrip(declare_with(DeclareKeyExpr{.id = 5, .wire_expr = we("foo", true)}));
    roundtrip(declare_with(UndeclareKeyExpr{.id = 5}));
    roundtrip(declare_with(DeclareSubscriber{.id = 10, .wire_expr = we("a/b", false)}));
    roundtrip(declare_with(UndeclareSubscriber{.id = 10, .wire_expr = we("x", true)}));
    roundtrip(declare_with(
        DeclareQueryable{.id = 7, .wire_expr = we("q", false),
                         .qinfo = QueryableInfo{.complete = true, .distance = 5}}));
    roundtrip(declare_with(UndeclareQueryable{.id = 7, .wire_expr = std::nullopt}));
    roundtrip(declare_with(DeclareToken{.id = 3, .wire_expr = we("t", true)}));
    roundtrip(declare_with(UndeclareToken{.id = 3, .wire_expr = we("t", false)}));
    roundtrip(declare_with(DeclareFinal{}));
}

TEST("Declare with id and extensions round-trips") {
    Declare d{};
    d.id = 3u;
    d.qos.inner = 0x15;
    d.nodeid.node_id = 2;
    roundtrip(d);
}

TEST("Interest round-trips (minimal and with folded wire_expr)") {
    Interest a{};
    a.id = 1;
    a.mode = InterestMode::current;
    a.inner.options = 0b0000'0011; // KEYEXPRS | SUBSCRIBERS
    roundtrip(a);

    Interest b{};
    b.id = 2;
    b.mode = InterestMode::current_future;
    b.inner.options = 0b1000'0100; // QUERYABLES | AGGREGATE
    b.inner.wire_expr = we("k", true);
    b.nodeid.node_id = 1;
    roundtrip(b);
}

TEST("InterestFinal round-trips (minimal and with QoS)") {
    InterestFinal a{};
    a.id = 9;
    roundtrip(a);

    InterestFinal c{};
    c.id = 1;
    c.qos.inner = 0x15;
    roundtrip(c);
}

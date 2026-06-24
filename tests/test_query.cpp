import zenoh.proto.network;
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

template <class T> auto roundtrip(const T& value) -> void {
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

auto make_zid(std::initializer_list<int> bytes) -> ZenohId {
    ZenohId z{};
    z.len = static_cast<std::uint8_t>(bytes.size());
    std::size_t i = 0;
    for (int b : bytes) z.bytes[i++] = static_cast<std::byte>(b);
    return z;
}

auto make_ts(std::uint64_t time, std::initializer_list<int> id) -> Timestamp {
    Timestamp t{};
    t.time = time;
    t.id_len = static_cast<std::uint8_t>(id.size());
    std::size_t i = 0;
    for (int b : id) t.id[i++] = static_cast<std::byte>(b);
    return t;
}

} // namespace

TEST("Query round-trips (default and full)") {
    roundtrip(Query{});

    static constexpr std::array<std::byte, 2> vp{std::byte{0xAA}, std::byte{0xBB}};
    static constexpr std::array<std::byte, 1> at{std::byte{0xCC}};
    Query q{};
    q.consolidation = ConsolidationMode::latest;
    q.parameters = std::string_view{"key=val"};
    q.sinfo = SourceInfo{.id = {.zid = make_zid({1, 2}), .eid = 3}, .sn = 4};
    q.body = Value{.encoding = Encoding{.id = 7}, .payload = vp};
    q.attachment = Attachment{at};
    roundtrip(q);
}

TEST("Err round-trips (payload only and encoding+sinfo)") {
    static constexpr std::array<std::byte, 3> pl{std::byte{1}, std::byte{2}, std::byte{3}};
    Err e{};
    e.payload = pl;
    roundtrip(e);

    static constexpr std::array<std::byte, 1> pl2{std::byte{0xFF}};
    Err e2{};
    e2.encoding = Encoding{.id = 5};
    e2.sinfo = SourceInfo{.id = {.zid = make_zid({9}), .eid = 1}, .sn = 2};
    e2.payload = pl2;
    roundtrip(e2);
}

TEST("Reply round-trips (default and with consolidation + payload)") {
    roundtrip(Reply{});

    static constexpr std::array<std::byte, 2> pl{std::byte{1}, std::byte{2}};
    Reply rep{};
    rep.consolidation = ConsolidationMode::monotonic;
    Put put{};
    put.payload = pl;
    rep.payload.body = put;
    roundtrip(rep);
}

TEST("Request round-trips (minimal and with every extension)") {
    Request req{};
    req.id = 7;
    req.wire_expr.suffix = std::string_view{"a"};
    roundtrip(req);

    Request req2{};
    req2.id = 1;
    req2.wire_expr.scope = 2;
    req2.wire_expr.mapping = Mapping::sender;
    req2.wire_expr.suffix = std::string_view{"x"};
    req2.qos.inner = 0x15;
    req2.timestamp = make_ts(99, {0xAB});
    req2.nodeid.node_id = 3;
    req2.target = QueryTarget::all;
    req2.budget = 100u;
    req2.timeout = Duration::from_millis(5000);
    roundtrip(req2);
}

TEST("Response round-trips (Reply body and Err body)") {
    Response rsp{};
    rsp.rid = 9;
    rsp.wire_expr.suffix = std::string_view{"r"};
    rsp.payload.body = Reply{};
    roundtrip(rsp);

    static constexpr std::array<std::byte, 1> pl{std::byte{0xAB}};
    Err e{};
    e.payload = pl;
    Response rsp2{};
    rsp2.rid = 2;
    rsp2.wire_expr.mapping = Mapping::sender;
    rsp2.wire_expr.suffix = std::string_view{"e"};
    rsp2.respid = EntityGlobalId{.zid = make_zid({9}), .eid = 1};
    rsp2.payload.body = e;
    roundtrip(rsp2);
}

TEST("ResponseFinal round-trips (minimal and with timestamp)") {
    ResponseFinal rf{};
    rf.rid = 5;
    roundtrip(rf);

    ResponseFinal rf2{};
    rf2.rid = 1;
    rf2.timestamp = make_ts(123, {0x01, 0x02});
    roundtrip(rf2);
}

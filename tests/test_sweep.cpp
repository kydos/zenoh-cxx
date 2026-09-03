// Systematic per-message sweeps, applied to *fully populated* values.
//
// The per-message tests elsewhere check that a message round-trips; these check the
// paths around that. For every message type, with every optional field present:
//
//   - `encode_pressure` encodes into a buffer of every size smaller than the message
//     needs. Each must fail cleanly, which walks every `ZTRY(w.write_*)` in the
//     encoder — the error arm of a write is otherwise only taken when a peer sends
//     something pathological.
//   - `truncation_sweep` decodes every prefix of the encoded bytes, walking the
//     matching read arms. Any result is fine; the point is that none crash or read
//     out of bounds (ASan/UBSan enforce that).
//   - `roundtrip` asserts the value survives encode+decode intact and consumes
//     exactly its own bytes.
//
// Fully populated matters: an optional field that is never set means its encoder,
// its decoder, and the `z = a || b || ...` chain that decides whether an extension
// block follows are all only ever exercised on one side.
import zenoh.proto.transport;
import zenoh.proto.network;
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
#include <span>
#include <string_view>
#include <vector>

using namespace zenoh;

namespace {

auto bytes_of(std::string_view s) -> std::span<const std::byte> {
    return {reinterpret_cast<const std::byte*>(s.data()), s.size()};
}

// Scratch large enough for anything built below.
constexpr std::size_t scratch_size = 2048;

template <class T> auto encoded(const T& value) -> std::vector<std::byte> {
    std::array<std::byte, scratch_size> buf{};
    ByteWriter w{buf};
    auto const ok = value.encode(w).has_value();
    CHECK(ok);
    return {buf.data(), buf.data() + w.written()};
}

// Encode into every buffer too small to hold the message: each attempt must report
// failure rather than writing past the end.
template <class T> auto encode_pressure(const T& value, std::size_t full_len) -> void {
    std::vector<std::byte> buf(full_len);
    for (std::size_t n = 0; n < full_len; ++n) {
        ByteWriter w{std::span(buf).first(n)};
        auto const r = value.encode(w);
        CHECK(!r.has_value());
        if (r) return; // don't spam one CHECK per size once it is wrong
    }
}

template <class T> auto truncation_sweep(std::span<const std::byte> bytes) -> void {
    for (std::size_t n = 0; n < bytes.size(); ++n) {
        ByteReader r{bytes.subspan(0, n)};
        auto result = T::decode(r);
        (void)result;
    }
}

// The three sweeps plus the round-trip, for one value.
template <class T> auto sweep(const T& value) -> void {
    auto const bytes = encoded(value);
    CHECK(!bytes.empty());

    ByteReader r{bytes};
    auto decoded = T::decode(r);
    CHECK(decoded.has_value());
    if (decoded) {
        CHECK(*decoded == value);
        CHECK(r.remaining() == 0);
    }

    encode_pressure(value, bytes.size());
    truncation_sweep<T>(bytes);
}

auto make_zid(int fill, std::uint8_t len) -> ZenohId {
    ZenohId z{};
    z.len = len;
    for (std::uint8_t i = 0; i < len; ++i) z.bytes[i] = static_cast<std::byte>(fill + i);
    return z;
}

auto make_timestamp() -> Timestamp {
    Timestamp t{};
    t.time = 0x0123456789abcdefULL;
    t.id_len = 8;
    for (std::uint8_t i = 0; i < t.id_len; ++i) t.id[i] = static_cast<std::byte>(0xa0 + i);
    return t;
}

auto make_entity() -> EntityGlobalId {
    EntityGlobalId e{};
    e.zid = make_zid(0x31, 16); // the maximum-length zid, the widest nibble encoding
    e.eid = 0xdeadbeef;
    return e;
}

auto make_source_info() -> SourceInfo { return SourceInfo{.id = make_entity(), .sn = 0x7fffffff}; }

auto make_wire_expr(std::string_view suffix, std::uint16_t scope) -> WireExpr {
    return WireExpr{.scope = scope, .mapping = Mapping::sender, .suffix = suffix};
}

} // namespace

TEST("sweep: transport messages, every extension present") {
    InitSyn isyn{};
    isyn.version = 9;
    isyn.identifier.whatami = WhatAmI::router;
    isyn.identifier.zid = make_zid(0x11, 16);
    isyn.resolution = InitResolution{.resolution = 0x0a, .batch_size = 65000};
    isyn.qos = HasQoS{};
    isyn.qos_link = QoSLink{.qos = 0x1234};
    isyn.auth = Auth{bytes_of("auth-token")};
    isyn.mlink = MultiLink{bytes_of("multi-link")};
    isyn.lowlatency = HasLowLatency{};
    isyn.compression = HasCompression{};
    isyn.patch = Patch{7};
    sweep(isyn);

    InitAck iack{};
    iack.version = 9;
    iack.identifier.whatami = WhatAmI::peer;
    iack.identifier.zid = make_zid(0x21, 4);
    iack.resolution = InitResolution{.resolution = 0x0a, .batch_size = 8192};
    auto const cookie =
        std::array<std::byte, 4>{std::byte{9}, std::byte{8}, std::byte{7}, std::byte{6}};
    iack.cookie = cookie;
    iack.qos = HasQoS{};
    iack.qos_link = QoSLink{.qos = 0x99};
    iack.auth = Auth{bytes_of("ack-auth")};
    iack.mlink = MultiLink{bytes_of("ack-mlink")};
    iack.lowlatency = HasLowLatency{};
    iack.compression = HasCompression{};
    iack.patch = Patch{3};
    sweep(iack);

    OpenSyn osyn{};
    osyn.lease = Duration::from_millis(12345);
    osyn.sn = 42;
    osyn.cookie = cookie;
    osyn.qos = HasQoS{};
    osyn.auth = Auth{bytes_of("open-auth")};
    osyn.mlink_syn = MultiLinkSyn{bytes_of("open-mlink")};
    osyn.mlink_ack = HasMultiLinkAck{};
    osyn.lowlatency = HasLowLatency{};
    osyn.compression = HasCompression{};
    sweep(osyn);

    OpenAck oack{};
    oack.lease = Duration::from_millis(30000); // the T-flag (seconds) encoding
    oack.sn = 7;
    oack.qos = HasQoS{};
    oack.auth = Auth{bytes_of("oack-auth")};
    oack.mlink_syn = MultiLinkSyn{bytes_of("oack-mlink")};
    oack.mlink_ack = HasMultiLinkAck{};
    oack.lowlatency = HasLowLatency{};
    oack.compression = HasCompression{};
    sweep(oack);

    sweep(Close{.reason = 3, .behaviour = CloseBehaviour::session});
    sweep(Close{.reason = 0, .behaviour = CloseBehaviour::link});
    sweep(KeepAlive{});

    FrameHeader fh{};
    fh.reliability = Reliability::best_effort;
    fh.sn = 0x0fffffff; // the widest SN the resolution mask allows
    fh.qos = QoS{.inner = 0x2a};
    sweep(fh);
}

TEST("sweep: network messages, every extension present") {
    Put put{};
    put.timestamp = make_timestamp();
    put.encoding = Encoding{.id = 0x7ff, .has_schema = true, .schema = bytes_of("schema")};
    put.sinfo = make_source_info();
    put.attachment = Attachment{bytes_of("attach")};
    put.payload = bytes_of("payload bytes");
    sweep(put);

    Del del{};
    del.timestamp = make_timestamp();
    del.sinfo = make_source_info();
    del.attachment = Attachment{bytes_of("del-attach")};
    sweep(del);

    Push push{};
    push.wire_expr = make_wire_expr("demo/key", 3);
    push.qos = QoS{.inner = 0x0d};
    push.timestamp = make_timestamp();
    push.dest = DestinationId{.zid = make_zid(0x41, 16)};
    push.payload = PushBody{.body = put};
    sweep(push);

    Push push_del{};
    push_del.wire_expr = make_wire_expr("demo/gone", 0);
    push_del.payload = PushBody{.body = del};
    sweep(push_del);

    Query query{};
    query.parameters = "a=1;b=2";
    query.consolidation = ConsolidationMode::latest;
    query.sinfo = make_source_info();
    query.body = Value{.encoding = Encoding{.id = 5, .has_schema = false, .schema = {}},
                       .payload = bytes_of("query body")};
    query.attachment = Attachment{bytes_of("q-attach")};
    sweep(query);

    Err err{};
    err.encoding = Encoding{.id = 9, .has_schema = true, .schema = bytes_of("err-schema")};
    err.sinfo = make_source_info();
    err.payload = bytes_of("error payload");
    sweep(err);

    Request request{};
    request.id = 0x1234;
    request.wire_expr = make_wire_expr("demo/**", 0);
    request.qos = QoS{.inner = 0x0d};
    request.timestamp = make_timestamp();
    request.budget = 4096;
    request.timeout = Duration::from_millis(2500);
    request.dest = DestinationId{.zid = make_zid(0x51, 8)};
    request.target = QueryTarget::all_complete;
    request.payload = RequestBody{.query = query};
    sweep(request);

    Response response{};
    response.rid = 0x99;
    response.wire_expr = make_wire_expr("demo/reply", 1);
    response.qos = QoS{.inner = 0x0d};
    response.timestamp = make_timestamp();
    response.respid = make_entity();
    response.payload = ResponseBody{.body = Reply{.payload = PushBody{.body = put}}};
    sweep(response);

    Response err_response = response;
    err_response.payload = ResponseBody{.body = err};
    sweep(err_response);

    ResponseFinal final{};
    final.rid = 0x77;
    final.qos = QoS{.inner = 0x0d};
    final.timestamp = make_timestamp();
    sweep(final);
}

TEST("sweep: declare bodies, every extension present") {
    auto const we = make_wire_expr("demo/declared", 2);

    Declare dec{};
    dec.id = 0x4242;
    dec.timestamp = make_timestamp();
    dec.qos = QoS{.inner = 0x0d};
    dec.body = DeclareBody{.body = DeclareKeyExpr{.id = 5, .wire_expr = we}};
    sweep(dec);

    dec.body = DeclareBody{.body = UndeclareKeyExpr{.id = 5}};
    sweep(dec);
    dec.body = DeclareBody{.body = DeclareSubscriber{.id = 6, .wire_expr = we}};
    sweep(dec);
    dec.body = DeclareBody{.body = UndeclareSubscriber{.id = 6, .wire_expr = we}};
    sweep(dec);
    dec.body = DeclareBody{.body = UndeclareSubscriber{.id = 6, .wire_expr = std::nullopt}};
    sweep(dec);
    dec.body = DeclareBody{
        .body = DeclareQueryable{
            .id = 7, .wire_expr = we, .qinfo = QueryableInfo{.complete = true, .distance = 4}}};
    sweep(dec);
    dec.body = DeclareBody{.body = UndeclareQueryable{.id = 7, .wire_expr = we}};
    sweep(dec);
    dec.body = DeclareBody{.body = DeclareToken{.id = 8, .wire_expr = we}};
    sweep(dec);
    dec.body = DeclareBody{.body = UndeclareToken{.id = 8, .wire_expr = we}};
    sweep(dec);
    dec.body = DeclareBody{.body = DeclareFinal{}};
    sweep(dec);
}

TEST("sweep: interest, every extension present") {
    Interest it{};
    it.mode = InterestMode::current_future;
    it.id = 0x321;
    it.qos = QoS{.inner = 0x0d};
    it.timestamp = make_timestamp();
    it.nodeid = NodeId{.node_id = 0x1234};
    it.inner.options = 0x1f;
    it.inner.wire_expr = make_wire_expr("demo/interest", 4);
    sweep(it);

    Interest bare{};
    bare.mode = InterestMode::current;
    bare.id = 1;
    sweep(bare);

    InterestFinal fin{};
    fin.id = 0x555;
    fin.qos = QoS{.inner = 0x0d};
    fin.timestamp = make_timestamp();
    fin.nodeid = NodeId{.node_id = 0x99};
    sweep(fin);
}

// --- inequality ---
//
// `operator==` for a message with borrowed spans is hand-written as a chain of `&&`,
// and a test that only ever compares *equal* values takes the same side of every one
// of those. Mutating one field at a time makes each link in the chain the first to
// differ, which is also the only way a bug in a later link would ever be noticed.
namespace {

template <class T, class F> auto differs(const T& base, F&& mutate) -> void {
    T other = base;
    mutate(other);
    CHECK(!(other == base)); // both orders: the chain is not symmetric by construction
    CHECK(!(base == other));
}

} // namespace

TEST("transport messages compare unequal on every field") {
    InitSyn s{};
    s.version = 9;
    s.identifier.zid = make_zid(0x11, 4);
    s.resolution = InitResolution{.resolution = 0x0a, .batch_size = 4096};
    s.qos = HasQoS{};
    s.qos_link = QoSLink{.qos = 1};
    s.auth = Auth{bytes_of("a")};
    s.mlink = MultiLink{bytes_of("m")};
    s.lowlatency = HasLowLatency{};
    s.compression = HasCompression{};
    s.patch = Patch{1};
    differs(s, [](InitSyn& v) { v.version = 8; });
    differs(s, [](InitSyn& v) { v.identifier.zid = make_zid(0x99, 4); });
    differs(s, [](InitSyn& v) { v.resolution.batch_size = 1024; });
    differs(s, [](InitSyn& v) { v.qos.reset(); });
    differs(s, [](InitSyn& v) { v.qos_link = QoSLink{.qos = 2}; });
    differs(s, [](InitSyn& v) { v.auth = Auth{bytes_of("b")}; });
    differs(s, [](InitSyn& v) { v.mlink = MultiLink{bytes_of("n")}; });
    differs(s, [](InitSyn& v) { v.lowlatency.reset(); });
    differs(s, [](InitSyn& v) { v.compression.reset(); });
    differs(s, [](InitSyn& v) { v.patch = Patch{2}; });

    InitAck a{};
    a.version = 9;
    a.identifier.zid = make_zid(0x21, 4);
    auto const cookie = std::array<std::byte, 2>{std::byte{1}, std::byte{2}};
    a.cookie = cookie;
    a.qos = HasQoS{};
    a.qos_link = QoSLink{.qos = 1};
    a.auth = Auth{bytes_of("a")};
    a.mlink = MultiLink{bytes_of("m")};
    a.lowlatency = HasLowLatency{};
    a.compression = HasCompression{};
    a.patch = Patch{1};
    auto const other_cookie = std::array<std::byte, 2>{std::byte{3}, std::byte{4}};
    differs(a, [](InitAck& v) { v.version = 8; });
    differs(a, [](InitAck& v) { v.identifier.zid = make_zid(0x99, 4); });
    differs(a, [](InitAck& v) { v.resolution.resolution = 0x0b; });
    differs(a, [&](InitAck& v) { v.cookie = other_cookie; });
    differs(a, [](InitAck& v) { v.qos.reset(); });
    differs(a, [](InitAck& v) { v.qos_link.reset(); });
    differs(a, [](InitAck& v) { v.auth = Auth{bytes_of("b")}; });
    differs(a, [](InitAck& v) { v.mlink.reset(); });
    differs(a, [](InitAck& v) { v.lowlatency.reset(); });
    differs(a, [](InitAck& v) { v.compression.reset(); });
    differs(a, [](InitAck& v) { v.patch = Patch{2}; });

    OpenSyn o{};
    o.lease = Duration::from_millis(1000);
    o.sn = 1;
    o.cookie = cookie;
    o.qos = HasQoS{};
    o.auth = Auth{bytes_of("a")};
    o.mlink_syn = MultiLinkSyn{bytes_of("m")};
    o.mlink_ack = HasMultiLinkAck{};
    o.lowlatency = HasLowLatency{};
    o.compression = HasCompression{};
    differs(o, [](OpenSyn& v) { v.lease = Duration::from_millis(2000); });
    differs(o, [](OpenSyn& v) { v.sn = 2; });
    differs(o, [&](OpenSyn& v) { v.cookie = other_cookie; });
    differs(o, [](OpenSyn& v) { v.qos.reset(); });
    differs(o, [](OpenSyn& v) { v.auth.reset(); });
    differs(o, [](OpenSyn& v) { v.mlink_syn.reset(); });
    differs(o, [](OpenSyn& v) { v.mlink_ack.reset(); });
    differs(o, [](OpenSyn& v) { v.lowlatency.reset(); });
    differs(o, [](OpenSyn& v) { v.compression.reset(); });
}

TEST("network messages compare unequal on every field") {
    Put put{};
    put.timestamp = make_timestamp();
    put.encoding = Encoding{.id = 3, .has_schema = true, .schema = bytes_of("s")};
    put.sinfo = make_source_info();
    put.attachment = Attachment{bytes_of("a")};
    put.payload = bytes_of("p");
    differs(put, [](Put& v) { v.timestamp.reset(); });
    differs(put, [](Put& v) { v.encoding.id = 4; });
    differs(put, [](Put& v) { v.sinfo->sn = 1; });
    differs(put, [](Put& v) { v.attachment = Attachment{bytes_of("b")}; });
    differs(put, [](Put& v) { v.payload = bytes_of("q"); });

    Del del{};
    del.timestamp = make_timestamp();
    del.sinfo = make_source_info();
    del.attachment = Attachment{bytes_of("a")};
    differs(del, [](Del& v) { v.timestamp.reset(); });
    differs(del, [](Del& v) { v.sinfo.reset(); });
    differs(del, [](Del& v) { v.attachment = Attachment{bytes_of("b")}; });

    Push push{};
    push.wire_expr = make_wire_expr("k", 1);
    push.timestamp = make_timestamp();
    push.dest = DestinationId{.zid = make_zid(0x41, 4)};
    push.payload = PushBody{.body = put};
    differs(push, [](Push& v) { v.wire_expr = make_wire_expr("k2", 1); });
    differs(push, [](Push& v) { v.wire_expr = make_wire_expr("k", 2); });
    differs(push, [](Push& v) { v.qos = QoS{.inner = 1}; });
    differs(push, [](Push& v) { v.timestamp.reset(); });
    differs(push, [](Push& v) { v.dest.reset(); });
    differs(push, [](Push& v) { v.payload = PushBody{.body = Del{}}; });

    Query q{};
    q.parameters = "a=1";
    q.sinfo = make_source_info();
    q.body = Value{.encoding = Encoding{}, .payload = bytes_of("b")};
    q.attachment = Attachment{bytes_of("a")};
    differs(q, [](Query& v) { v.parameters = "a=2"; });
    differs(q, [](Query& v) { v.consolidation = ConsolidationMode::none; });
    differs(q, [](Query& v) { v.sinfo.reset(); });
    differs(q, [](Query& v) { v.body.reset(); });
    differs(q, [](Query& v) { v.attachment.reset(); });

    Err err{};
    err.encoding = Encoding{.id = 1, .has_schema = false, .schema = {}};
    err.sinfo = make_source_info();
    err.payload = bytes_of("e");
    differs(err, [](Err& v) { v.encoding.id = 2; });
    differs(err, [](Err& v) { v.sinfo.reset(); });
    differs(err, [](Err& v) { v.payload = bytes_of("f"); });

    Request req{};
    req.id = 1;
    req.wire_expr = make_wire_expr("k", 0);
    req.timestamp = make_timestamp();
    req.budget = 10;
    req.timeout = Duration::from_millis(1000);
    req.dest = DestinationId{.zid = make_zid(0x51, 4)};
    req.payload = RequestBody{.query = q};
    differs(req, [](Request& v) { v.id = 2; });
    differs(req, [](Request& v) { v.wire_expr = make_wire_expr("k2", 0); });
    differs(req, [](Request& v) { v.qos = QoS{.inner = 1}; });
    differs(req, [](Request& v) { v.timestamp.reset(); });
    differs(req, [](Request& v) { v.budget = 11; });
    differs(req, [](Request& v) { v.timeout = Duration::from_millis(2000); });
    differs(req, [](Request& v) { v.dest.reset(); });
    differs(req, [](Request& v) { v.target = QueryTarget::all; });

    Response rsp{};
    rsp.rid = 1;
    rsp.wire_expr = make_wire_expr("k", 0);
    rsp.timestamp = make_timestamp();
    rsp.respid = make_entity();
    rsp.payload = ResponseBody{.body = Reply{.payload = PushBody{.body = put}}};
    differs(rsp, [](Response& v) { v.rid = 2; });
    differs(rsp, [](Response& v) { v.wire_expr = make_wire_expr("k2", 0); });
    differs(rsp, [](Response& v) { v.qos = QoS{.inner = 1}; });
    differs(rsp, [](Response& v) { v.timestamp.reset(); });
    differs(rsp, [](Response& v) { v.respid.reset(); });
    differs(rsp, [&](Response& v) { v.payload = ResponseBody{.body = err}; });

    ResponseFinal fin{};
    fin.rid = 1;
    fin.timestamp = make_timestamp();
    differs(fin, [](ResponseFinal& v) { v.rid = 2; });
    differs(fin, [](ResponseFinal& v) { v.qos = QoS{.inner = 1}; });
    differs(fin, [](ResponseFinal& v) { v.timestamp.reset(); });
}

TEST("declare and interest compare unequal on every field") {
    auto const we = make_wire_expr("k", 1);

    Declare dec{};
    dec.id = 1;
    dec.timestamp = make_timestamp();
    dec.body = DeclareBody{.body = DeclareSubscriber{.id = 2, .wire_expr = we}};
    differs(dec, [](Declare& v) { v.id = 2; });
    differs(dec, [](Declare& v) { v.qos = QoS{.inner = 1}; });
    differs(dec, [](Declare& v) { v.timestamp.reset(); });
    differs(dec, [](Declare& v) { v.body = DeclareBody{.body = DeclareFinal{}}; });

    DeclareSubscriber ds{.id = 2, .wire_expr = we};
    differs(ds, [](DeclareSubscriber& v) { v.id = 3; });
    differs(ds, [](DeclareSubscriber& v) { v.wire_expr = make_wire_expr("k2", 1); });

    UndeclareSubscriber us{.id = 2, .wire_expr = we};
    differs(us, [](UndeclareSubscriber& v) { v.id = 3; });
    differs(us, [](UndeclareSubscriber& v) { v.wire_expr.reset(); });

    DeclareQueryable dq{
        .id = 2, .wire_expr = we, .qinfo = QueryableInfo{.complete = true, .distance = 1}};
    differs(dq, [](DeclareQueryable& v) { v.id = 3; });
    differs(dq, [](DeclareQueryable& v) { v.wire_expr = make_wire_expr("k2", 1); });
    differs(dq, [](DeclareQueryable& v) { v.qinfo.complete = false; });

    Interest it{};
    it.mode = InterestMode::current;
    it.id = 1;
    it.timestamp = make_timestamp();
    it.nodeid = NodeId{.node_id = 1};
    it.inner.wire_expr = we;
    differs(it, [](Interest& v) { v.mode = InterestMode::future; });
    differs(it, [](Interest& v) { v.id = 2; });
    differs(it, [](Interest& v) { v.qos = QoS{.inner = 1}; });
    differs(it, [](Interest& v) { v.timestamp.reset(); });
    differs(it, [](Interest& v) { v.nodeid = NodeId{.node_id = 2}; });
    differs(it, [](Interest& v) { v.inner.wire_expr.reset(); });
    differs(it, [](Interest& v) { v.inner.options = 0x1f; });

    InterestFinal fin{};
    fin.id = 1;
    fin.timestamp = make_timestamp();
    fin.nodeid = NodeId{.node_id = 1};
    differs(fin, [](InterestFinal& v) { v.id = 2; });
    differs(fin, [](InterestFinal& v) { v.qos = QoS{.inner = 1}; });
    differs(fin, [](InterestFinal& v) { v.timestamp.reset(); });
    differs(fin, [](InterestFinal& v) { v.nodeid = NodeId{.node_id = 2}; });
}

TEST("field and extension types compare unequal on every field") {
    auto ts = make_timestamp();
    differs(ts, [](Timestamp& v) { v.time = 1; });
    differs(ts, [](Timestamp& v) { v.id_len = 4; });
    differs(ts, [](Timestamp& v) { v.id[0] = std::byte{0xff}; });

    Encoding enc{.id = 1, .has_schema = true, .schema = bytes_of("s")};
    differs(enc, [](Encoding& v) { v.id = 2; });
    differs(enc, [](Encoding& v) { v.has_schema = false; });
    differs(enc, [](Encoding& v) { v.schema = bytes_of("t"); });

    auto we = make_wire_expr("k", 1);
    differs(we, [](WireExpr& v) { v.scope = 2; });
    differs(we, [](WireExpr& v) { v.mapping = Mapping::receiver; });
    differs(we, [](WireExpr& v) { v.suffix = "k2"; });

    auto eid = make_entity();
    differs(eid, [](EntityGlobalId& v) { v.zid = make_zid(0x99, 16); });
    differs(eid, [](EntityGlobalId& v) { v.eid = 1; });

    auto si = make_source_info();
    differs(si, [](SourceInfo& v) { v.sn = 1; });
    differs(si, [](SourceInfo& v) { v.id.eid = 1; });

    Attachment att{bytes_of("a")};
    differs(att, [](Attachment& v) { v.buffer = bytes_of("b"); });

    Value val{.encoding = Encoding{}, .payload = bytes_of("p")};
    differs(val, [](Value& v) { v.encoding.id = 1; });
    differs(val, [](Value& v) { v.payload = bytes_of("q"); });

    DestinationId dest{.zid = make_zid(0x41, 4)};
    differs(dest, [](DestinationId& v) { v.zid = make_zid(0x99, 4); });
}

// --- header validation ---
//
// Every decoder starts by checking the header byte, and those checks are `||` chains:
// `(h & mid_mask) != id || (h & flag_a) == 0`. Feeding only the wrong *mid* takes the
// first arm every time and leaves the rest untested — so each arm gets its own byte
// sequence here, with the mid correct and one flag wrong.
TEST("transport decoders reject a correct mid carrying the wrong ack bit") {
    // InitAck requires A=1; InitSyn requires A=0. Each is the other's header.
    auto const init_no_ack = std::array<std::byte, 1>{std::byte{0x01}};   // mid 1, A=0
    auto const init_with_ack = std::array<std::byte, 1>{std::byte{0x21}}; // mid 1, A=1
    ByteReader r1{init_no_ack};
    CHECK(!InitAck::decode(r1).has_value());
    ByteReader r2{init_with_ack};
    CHECK(!InitSyn::decode(r2).has_value());

    // Same shape for Open.
    auto const open_no_ack = std::array<std::byte, 1>{std::byte{0x02}};
    auto const open_with_ack = std::array<std::byte, 1>{std::byte{0x22}};
    ByteReader r3{open_no_ack};
    CHECK(!OpenAck::decode(r3).has_value());
    ByteReader r4{open_with_ack};
    CHECK(!OpenSyn::decode(r4).has_value());
}

TEST("zid lengths outside 1..16 are rejected on encode") {
    // InitIdentifier stores len-1 in a nibble, so 0 would underflow to 0xf (decoding
    // back as 16) and >16 would not fit at all. Both must be refused, not clamped.
    std::array<std::byte, 64> buf{};
    for (std::uint8_t len : {std::uint8_t{0}, std::uint8_t{17}, std::uint8_t{200}}) {
        InitIdentifier id{};
        id.whatami = WhatAmI::client;
        id.zid.len = len;
        ByteWriter w{buf};
        CHECK(!id.encode(w).has_value());

        // The same nibble encoding, in the two extension bodies that also use it.
        EntityGlobalId e{};
        e.zid.len = len;
        e.eid = 1;
        ByteWriter w2{buf};
        CHECK(!e.encode_body(w2).has_value());

        DestinationId d{};
        d.zid.len = len;
        ByteWriter w3{buf};
        CHECK(!d.encode_body(w3).has_value());
    }

    // 1 and 16 are the boundaries that must work.
    for (std::uint8_t len : {std::uint8_t{1}, std::uint8_t{16}}) {
        InitIdentifier id{};
        id.whatami = WhatAmI::router;
        id.zid = make_zid(0x60, len);
        ByteWriter w{buf};
        CHECK(id.encode(w).has_value());
    }
}

TEST("an unknown non-mandatory extension is skipped, a mandatory one is rejected") {
    // Ext id 0xd is unused by every message here. Non-mandatory (M=0) it must be
    // skipped in each of the three encodings; mandatory (M=1) it must be refused.
    struct Case {
        int header; ///< extension header byte
        int extra;  ///< trailing body bytes the encoding implies
    };
    auto const skipped = std::array<Case, 3>{Case{0x0d, 0}, Case{0x2d, 1}, Case{0x4d, 1}};

    for (auto const& c : skipped) {
        // Push(Del) with Z set, then the unknown extension.
        std::vector<std::byte> bytes{std::byte{0x82}, static_cast<std::byte>(c.header)};
        for (int i = 0; i < c.extra; ++i) bytes.push_back(std::byte{0x00});
        ByteReader r{bytes};
        auto del = Del::decode(r);
        CHECK(del.has_value());
        CHECK(r.remaining() == 0);

        // The mandatory form of the same extension is a hard error.
        std::vector<std::byte> mandatory{std::byte{0x82}, static_cast<std::byte>(c.header | 0x10)};
        for (int i = 0; i < c.extra; ++i) mandatory.push_back(std::byte{0x00});
        ByteReader mr{mandatory};
        CHECK(!Del::decode(mr).has_value());
    }
}

// Every decoder's first act is `if ((h & mid_mask) != id) return malformed`. The
// suite reaches that line constantly but almost never takes the *true* side, because
// nothing else feeds a decoder a header belonging to a different message.
TEST("every decoder rejects a header carrying another message's id") {
    auto const foreign = std::array<std::byte, 1>{std::byte{0x00}}; // mid 0: no message
    auto reject = [&](auto decode) {
        ByteReader r{foreign};
        CHECK(!decode(r).has_value());
    };

    reject([](ByteReader& r) { return Put::decode(r); });
    reject([](ByteReader& r) { return Del::decode(r); });
    reject([](ByteReader& r) { return Push::decode(r); });
    reject([](ByteReader& r) { return Query::decode(r); });
    reject([](ByteReader& r) { return Err::decode(r); });
    reject([](ByteReader& r) { return Reply::decode(r); });
    reject([](ByteReader& r) { return Request::decode(r); });
    reject([](ByteReader& r) { return Response::decode(r); });
    reject([](ByteReader& r) { return ResponseFinal::decode(r); });
    reject([](ByteReader& r) { return Declare::decode(r); });
    reject([](ByteReader& r) { return Interest::decode(r); });
    reject([](ByteReader& r) { return InterestFinal::decode(r); });
    reject([](ByteReader& r) { return Close::decode(r); });
    reject([](ByteReader& r) { return KeepAlive::decode(r); });
    reject([](ByteReader& r) { return FrameHeader::decode(r); });
    reject([](ByteReader& r) { return InitSyn::decode(r); });
    reject([](ByteReader& r) { return InitAck::decode(r); });
    reject([](ByteReader& r) { return OpenSyn::decode(r); });
    reject([](ByteReader& r) { return OpenAck::decode(r); });

    // Declare sub-bodies use their own small id space (0x00..0x07), so a foreign id
    // there is one outside it.
    auto const foreign_body = std::array<std::byte, 1>{std::byte{0x0a}};
    ByteReader br{foreign_body};
    CHECK(!DeclareBody::decode(br).has_value());
    ByteReader br2{foreign_body};
    CHECK(!DeclareKeyExpr::decode(br2).has_value());
    ByteReader br3{foreign_body};
    CHECK(!DeclareSubscriber::decode(br3).has_value());
    ByteReader br4{foreign_body};
    CHECK(!DeclareQueryable::decode(br4).has_value());
}

TEST("out-of-range enum values on the wire are malformed") {
    // Query's consolidation byte is a 2-bit enum; 4..255 have no meaning.
    // Header: mid 0x1c-ish body — Query is decoded via its own id with the C flag set.
    Query q{};
    q.parameters = "";
    q.consolidation = ConsolidationMode::latest;
    auto bytes = encoded(q);
    // The consolidation byte is the last one written for this shape; make it invalid.
    CHECK(!bytes.empty());
    bytes.back() = std::byte{0x04};
    ByteReader r{bytes};
    auto decoded = Query::decode(r);
    CHECK(!decoded.has_value() || r.remaining() != 0);
}

TEST("a Reply tolerates an extension it does not define") {
    // Reply itself defines no extensions, but a peer may add one: non-mandatory must
    // be skipped, mandatory must be refused.
    Reply reply{};
    Put put{};
    put.payload = bytes_of("v");
    reply.payload = PushBody{.body = put};
    auto const base = encoded(reply);
    CHECK(!base.empty());

    // The extension block sits between the header and the payload, so the extension
    // byte is spliced in at index 1 rather than appended.
    auto splice = [&](int ext_byte) {
        std::vector<std::byte> out;
        out.push_back(static_cast<std::byte>(std::to_integer<std::uint8_t>(base[0]) | 0x80));
        out.push_back(static_cast<std::byte>(ext_byte));
        out.insert(out.end(), base.begin() + 1, base.end());
        return out;
    };

    auto const with_ext = splice(0x0d); // ext id 13, Unit, non-mandatory: skipped
    ByteReader r{with_ext};
    auto decoded = Reply::decode(r);
    CHECK(decoded.has_value());
    CHECK(r.remaining() == 0);

    auto const mandatory = splice(0x1d); // same id with M set: refused
    ByteReader mr{mandatory};
    CHECK(!Reply::decode(mr).has_value());
}

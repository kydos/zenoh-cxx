// Branch-coverage tests: round-trip every message type in a *fully populated* form
// (all optional fields and extensions present) and a *minimal* form (all defaults),
// then truncation-sweep the populated encoding. This exercises both arms of every
// optional-field decision (present vs absent) and every ZTRY error arm (a truncated
// read at each field). Complements the differential tests, which cover correctness
// but not every branch.
import zenoh.proto;

#include "ztest.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string_view>
#include <vector>

using namespace zenoh;

namespace {

template <class T> auto enc(const T& v) -> std::vector<std::byte> {
    std::array<std::byte, 4096> buf{};
    ByteWriter w{buf};
    CHECK(v.encode(w).has_value());
    return {buf.data(), buf.data() + w.written()};
}

// Encode, decode, assert decode consumed every byte and re-encodes byte-identically
// (decode∘encode stability — stronger than operator== and avoids the libc++
// optional<>-comparison landmine), then decode every truncated prefix (must not
// crash / read OOB under ASan/UBSan).
template <class T> auto roundtrip(const T& v) -> void {
    auto const bytes = enc(v);
    ByteReader r{bytes};
    auto d = T::decode(r);
    CHECK(d.has_value());
    if (d) {
        CHECK(r.remaining() == 0);
        CHECK(enc(*d) == bytes);
    }
    for (std::size_t n = 0; n < bytes.size(); ++n) {
        ByteReader rt{std::span<const std::byte>(bytes).subspan(0, n)};
        (void)T::decode(rt);
    }
}

auto zid(std::uint8_t len) -> ZenohId {
    ZenohId z{};
    z.len = len;
    for (std::uint8_t i = 0; i < len; ++i) z.bytes[i] = static_cast<std::byte>(0xA0 + i);
    return z;
}

auto stamp() -> Timestamp {
    Timestamp t{};
    t.time = 0x0123456789ABCDEFull;
    t.id_len = 8;
    for (int i = 0; i < 8; ++i) t.id[i] = static_cast<std::byte>(i + 1);
    return t;
}

// Stable backing storage for the borrowed span/string fields.
constexpr std::array<std::byte, 4> g_schema{std::byte{'c'}, std::byte{'b'}, std::byte{'o'},
                                            std::byte{'r'}};
constexpr std::array<std::byte, 3> g_att{std::byte{1}, std::byte{2}, std::byte{3}};
constexpr std::array<std::byte, 5> g_payload{std::byte{9}, std::byte{8}, std::byte{7}, std::byte{6},
                                             std::byte{5}};
constexpr std::array<std::byte, 4> g_cookie{std::byte{0xC0}, std::byte{0xC1}, std::byte{0xC2},
                                            std::byte{0xC3}};
constexpr std::array<std::byte, 2> g_blob{std::byte{0xEE}, std::byte{0xFF}};

auto full_encoding() -> Encoding {
    return Encoding{.id = 1234, .has_schema = true, .schema = g_schema};
}
auto full_sinfo() -> SourceInfo { return SourceInfo{.id = {.zid = zid(4), .eid = 7}, .sn = 99}; }
auto sender_we(std::string_view s) -> WireExpr {
    return WireExpr{.scope = 5, .mapping = Mapping::sender, .suffix = s};
}

auto bytes_of(std::initializer_list<int> v) -> std::vector<std::byte> {
    std::vector<std::byte> o;
    o.reserve(v.size());
    for (int b : v) o.push_back(static_cast<std::byte>(b));
    return o;
}

template <class T> auto decodes(std::span<const std::byte> b) -> bool {
    ByteReader r{b};
    return T::decode(r).has_value();
}

// `reach` must be a byte prefix that lands the decoder in T's extension loop with the
// Z flag set. Appending an unknown extension id then exercises the loop's `default`
// arm: a non-mandatory unknown ext is skipped (0x0d = unit, id 13, more=0), a
// mandatory one is rejected (0x1d = unit|mandatory, id 13).
template <class T> auto unknown_ext(std::vector<std::byte> reach) -> void {
    auto skip = reach;
    skip.push_back(std::byte{0x0d});
    {
        ByteReader r{skip};
        (void)T::decode(r); // executes skip_ext; may fail later on a missing trailing field
    }
    auto reject = reach;
    reject.push_back(std::byte{0x1d});
    CHECK(!decodes<T>(reject));
}

} // namespace

TEST("Put/Del round-trip full + minimal + truncation") {
    Put full{};
    full.timestamp = stamp();
    full.encoding = full_encoding();
    full.sinfo = full_sinfo();
    full.attachment = Attachment{g_att};
    full.payload = g_payload;
    roundtrip(full);
    roundtrip(Put{}); // minimal: every optional absent

    Del dfull{};
    dfull.timestamp = stamp();
    dfull.sinfo = full_sinfo();
    dfull.attachment = Attachment{g_att};
    roundtrip(dfull);
    roundtrip(Del{});
}

TEST("Push round-trip (Put body, Del body, minimal) + truncation") {
    Push p{};
    p.wire_expr = sender_we("demo/example/**");
    p.qos = QoS{.inner = 6};
    p.timestamp = stamp();
    p.nodeid = NodeId{.node_id = 42};
    Put pb{};
    pb.payload = g_payload;
    p.payload = PushBody{.body = pb};
    roundtrip(p);

    Push pd = p;
    pd.payload = PushBody{.body = Del{}};
    roundtrip(pd);

    roundtrip(Push{});
}

TEST("Query/Err/Reply round-trip full + minimal + truncation") {
    Query q{};
    q.consolidation = ConsolidationMode::latest;
    q.parameters = std::string_view{"arg=1;k=v"};
    q.sinfo = full_sinfo();
    q.body = Value{.encoding = full_encoding(), .payload = g_payload};
    q.attachment = Attachment{g_att};
    roundtrip(q);
    roundtrip(Query{});

    Err e{};
    e.encoding = full_encoding();
    e.sinfo = full_sinfo();
    e.payload = g_payload;
    roundtrip(e);
    roundtrip(Err{});

    Reply rep{};
    rep.consolidation = ConsolidationMode::monotonic;
    Put rpb{};
    rpb.payload = g_payload;
    rep.payload = PushBody{.body = rpb};
    roundtrip(rep);
    roundtrip(Reply{});
}

TEST("Request/Response/ResponseFinal round-trip full + minimal + truncation") {
    Request req{};
    req.id = 0x0BADCAFE;
    req.wire_expr = sender_we("q/key");
    req.qos = QoS{.inner = 6};
    req.timestamp = stamp();
    req.nodeid = NodeId{.node_id = 7};
    req.target = QueryTarget::all_complete;
    req.budget = 0x1234u;
    req.timeout = Duration::from_millis(5000);
    Query inner{};
    inner.parameters = std::string_view{"x"};
    req.payload = RequestBody{.query = inner};
    roundtrip(req);
    roundtrip(Request{});

    Response resp{};
    resp.rid = 0xFEED;
    resp.wire_expr = sender_we("r/key");
    resp.qos = QoS{.inner = 6};
    resp.timestamp = stamp();
    resp.respid = EntityGlobalId{.zid = zid(6), .eid = 11};
    Err rerr{};
    rerr.payload = g_payload;
    resp.payload = ResponseBody{.body = rerr};
    roundtrip(resp);

    Response resp2 = resp;
    Reply rrep{};
    Put rrpb{};
    rrpb.payload = g_payload;
    rrep.payload = PushBody{.body = rrpb};
    resp2.payload = ResponseBody{.body = rrep};
    roundtrip(resp2);
    roundtrip(Response{});

    ResponseFinal rf{};
    rf.rid = 0x99;
    rf.qos = QoS{.inner = 6};
    rf.timestamp = stamp();
    roundtrip(rf);
    roundtrip(ResponseFinal{});
}

TEST("Transport handshake messages round-trip full + minimal + truncation") {
    InitSyn isyn{};
    isyn.version = 9;
    isyn.identifier = InitIdentifier{.whatami = WhatAmI::client, .zid = zid(16)};
    isyn.resolution = InitResolution{.resolution = 0x05, .batch_size = 4096};
    isyn.qos = HasQoS{};
    isyn.qos_link = QoSLink{0x55};
    isyn.auth = Auth{g_blob};
    isyn.mlink = MultiLink{g_blob};
    isyn.lowlatency = HasLowLatency{};
    isyn.compression = HasCompression{};
    isyn.patch = Patch{.value = 1};
    roundtrip(isyn);
    roundtrip(InitSyn{.version = 9, .identifier = {.zid = zid(1)}});

    InitAck iack{};
    iack.version = 9;
    iack.identifier = InitIdentifier{.whatami = WhatAmI::router, .zid = zid(16)};
    iack.resolution = InitResolution{.resolution = 0x05, .batch_size = 4096};
    iack.cookie = g_cookie;
    iack.qos = HasQoS{};
    iack.auth = Auth{g_blob};
    iack.mlink = MultiLink{g_blob};
    iack.lowlatency = HasLowLatency{};
    iack.compression = HasCompression{};
    iack.patch = Patch{.value = 1};
    roundtrip(iack);

    OpenSyn osyn{};
    osyn.lease = Duration::from_millis(10000); // seconds form -> T flag
    osyn.sn = 12345;
    osyn.cookie = g_cookie;
    osyn.qos = HasQoS{};
    osyn.auth = Auth{g_blob};
    osyn.mlink_syn = MultiLinkSyn{g_blob};
    osyn.mlink_ack = HasMultiLinkAck{};
    osyn.lowlatency = HasLowLatency{};
    osyn.compression = HasCompression{};
    roundtrip(osyn);
    // millis (non-seconds) lease form exercises the other Duration branch.
    OpenSyn osyn_ms = osyn;
    osyn_ms.lease = Duration::from_millis(1500);
    roundtrip(osyn_ms);

    OpenAck oack{};
    oack.lease = Duration::from_millis(10000);
    oack.sn = 999;
    oack.qos = HasQoS{};
    oack.auth = Auth{g_blob};
    oack.mlink_syn = MultiLinkSyn{g_blob};
    oack.mlink_ack = HasMultiLinkAck{};
    oack.lowlatency = HasLowLatency{};
    oack.compression = HasCompression{};
    roundtrip(oack);

    roundtrip(Close{.reason = 3, .behaviour = CloseBehaviour::session});
    roundtrip(Close{.reason = 0, .behaviour = CloseBehaviour::link});
    roundtrip(KeepAlive{});

    roundtrip(FrameHeader{.reliability = Reliability::reliable, .sn = 7, .qos = QoS{.inner = 6}});
    roundtrip(FrameHeader{.reliability = Reliability::best_effort, .sn = 0});
}

TEST("Declare bodies round-trip full + minimal + truncation") {
    auto wrap = [](DeclareBody body, bool exts) {
        Declare d{};
        if (exts) {
            d.id = 77u;
            d.qos = QoS{.inner = 6};
            d.timestamp = stamp();
            d.nodeid = NodeId{.node_id = 5};
        }
        d.body = std::move(body);
        roundtrip(d);
    };

    wrap(DeclareBody{.body = DeclareKeyExpr{.id = 3, .wire_expr = sender_we("k/e")}}, true);
    wrap(DeclareBody{.body = UndeclareKeyExpr{.id = 4}}, true);
    wrap(DeclareBody{.body = DeclareSubscriber{.id = 5, .wire_expr = sender_we("s/e")}}, true);
    wrap(DeclareBody{.body = UndeclareSubscriber{.id = 6, .wire_expr = sender_we("u/s")}}, true);
    wrap(DeclareBody{.body = UndeclareSubscriber{.id = 6}}, false); // optional wire_expr absent
    wrap(DeclareBody{.body = DeclareQueryable{.id = 7,
                                              .wire_expr = sender_we("q/e"),
                                              .qinfo = {.complete = true, .distance = 9}}},
         true);
    wrap(DeclareBody{.body = UndeclareQueryable{.id = 8, .wire_expr = sender_we("u/q")}}, true);
    wrap(DeclareBody{.body = DeclareToken{.id = 9, .wire_expr = sender_we("t/e")}}, true);
    wrap(DeclareBody{.body = UndeclareToken{.id = 10, .wire_expr = sender_we("u/t")}}, true);
    wrap(DeclareBody{.body = UndeclareToken{.id = 10}}, false);
    wrap(DeclareBody{.body = DeclareFinal{}}, false);
}

TEST("Interest/InterestFinal round-trip full + minimal + truncation") {
    Interest it{};
    it.id = 1;
    it.mode = InterestMode::current_future;
    it.inner.options = 0b1000'1111; // K,S,Q,T + A
    it.inner.wire_expr = sender_we("i/key");
    it.qos = QoS{.inner = 6};
    it.timestamp = stamp();
    it.nodeid = NodeId{.node_id = 3};
    roundtrip(it);

    // Interest with no wire_expr (R bit clear) and a different mode.
    Interest it2{};
    it2.id = 2;
    it2.mode = InterestMode::future;
    it2.inner.options = 0b0000'0101;
    roundtrip(it2);

    InterestFinal f{};
    f.id = 5;
    f.qos = QoS{.inner = 6};
    f.timestamp = stamp();
    f.nodeid = NodeId{.node_id = 1};
    roundtrip(f);
    roundtrip(InterestFinal{.id = 6});
}

TEST("unknown extension: non-mandatory skipped, mandatory rejected (all messages)") {
    unknown_ext<Put>(bytes_of({0x81}));
    unknown_ext<Del>(bytes_of({0x82}));
    unknown_ext<Push>(bytes_of({0x9d, 0x00}));
    unknown_ext<Query>(bytes_of({0x83}));
    unknown_ext<Err>(bytes_of({0x85}));
    unknown_ext<Request>(bytes_of({0x9c, 0x00, 0x00}));
    unknown_ext<Response>(bytes_of({0x9b, 0x00, 0x00}));
    unknown_ext<ResponseFinal>(bytes_of({0x9a, 0x00}));
    unknown_ext<InitSyn>(bytes_of({0x81, 0x09, 0x00, 0x00}));
    unknown_ext<InitAck>(bytes_of({0xA1, 0x09, 0x00, 0x00, 0x00}));
    unknown_ext<OpenSyn>(bytes_of({0x82, 0x00, 0x00, 0x00}));
    unknown_ext<OpenAck>(bytes_of({0xA2, 0x00, 0x00}));
    unknown_ext<FrameHeader>(bytes_of({0x85, 0x00}));
    unknown_ext<Declare>(bytes_of({0x9e}));
    unknown_ext<Interest>(bytes_of({0xB9, 0x00, 0x00}));
    unknown_ext<InterestFinal>(bytes_of({0x99, 0x00}));
}

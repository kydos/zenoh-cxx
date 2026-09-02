// Robustness: decoders must reject malformed input with a CodecError and never
// read out of bounds (run under ASan/UBSan). Includes truncation sweeps that
// decode every prefix of a valid message.
import zenoh.proto.network;
import zenoh.proto.transport;
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
#include <vector>

using namespace zenoh;

namespace {

template <class T> auto rejects(std::span<const std::byte> bytes) -> bool {
    ByteReader r{bytes};
    return !T::decode(r).has_value();
}

// Encode a value into an owned byte vector (oversized scratch, trimmed to written).
template <class T> auto encoded(const T& value) -> std::vector<std::byte> {
    std::array<std::byte, 512> buf{};
    ByteWriter w{buf};
    auto const ok = value.encode(w).has_value();
    CHECK(ok);
    return {buf.data(), buf.data() + w.written()};
}

// Decode every prefix of `bytes` as T. Any result is fine; the point is that none
// of them crash or read past the end (ASan/UBSan enforce that).
template <class T> auto truncation_sweep(std::span<const std::byte> bytes) -> void {
    for (std::size_t n = 0; n <= bytes.size(); ++n) {
        ByteReader r{bytes.subspan(0, n)};
        auto result = T::decode(r);
        (void)result;
    }
}

auto byteseq(std::initializer_list<int> v) -> std::vector<std::byte> {
    std::vector<std::byte> out;
    out.reserve(v.size());
    for (int b : v) out.push_back(static_cast<std::byte>(b));
    return out;
}

} // namespace

TEST("decoders reject empty input") {
    std::span<const std::byte> empty{};
    CHECK(rejects<Put>(empty));
    CHECK(rejects<Push>(empty));
    CHECK(rejects<InitAck>(empty));
    CHECK(rejects<Declare>(empty));
    CHECK(rejects<Interest>(empty));
    CHECK(rejects<Request>(empty));
    CHECK(rejects<Response>(empty));
}

TEST("decoders reject the wrong message id") {
    auto b = byteseq({0x1f, 0x00, 0x00});
    CHECK(rejects<Put>(b));
    CHECK(rejects<Push>(b));
    CHECK(rejects<Declare>(b));
}

TEST("Put rejects a payload length that overruns the buffer") {
    // header 0x01 (no flags), payload prefix says 10 bytes, only 2 follow.
    auto b = byteseq({0x01, 0x0a, 0x01, 0x02});
    CHECK(rejects<Put>(b));
}

TEST("Put rejects an unknown mandatory extension") {
    // header Z; ext header = mandatory|U64|id=7 (unknown to Put), more=0; then a value.
    auto b = byteseq({0x81, 0x37, 0x00, 0x00});
    CHECK(rejects<Put>(b));
}

TEST("decode rejects a ZStruct extension whose length overruns") {
    // Put header Z; ext header = ZStruct|id=1 (sinfo); length says 20, buffer ends.
    auto b = byteseq({0x81, 0x41, 0x14, 0x00});
    CHECK(rejects<Put>(b));
}

TEST("InitAck rejects a header missing the Ack bit") {
    auto b = byteseq({0x01, 0x09, 0x00, 0x00});
    CHECK(rejects<InitAck>(b));
}

TEST("decoders reject out-of-range narrowed integers") {
    // Push wire_expr.scope is u16; encode 65536 as a varint (0x80 0x80 0x04).
    CHECK(rejects<Push>(byteseq({0x1d, 0x80, 0x80, 0x04})));
    // Request id is u32; 0x1_0000_0000 as a varint overflows u32.
    CHECK(rejects<Request>(byteseq({0x1c, 0x80, 0x80, 0x80, 0x80, 0x10})));
}

TEST("decoders reject out-of-range enum values") {
    // Query with the C flag set and a consolidation byte of 7 (valid range 0..3).
    CHECK(rejects<Query>(byteseq({0x23, 0x07})));
    // Request with a QueryTarget extension (id 4, U64, mandatory) of 5 (range 0..2).
    CHECK(rejects<Request>(byteseq({0x9c, 0x00, 0x00, 0x34, 0x05})));
    // InitAck identifier with WhatAmI = 0b11 (only 0..2 are defined).
    CHECK(rejects<InitAck>(byteseq({0x21, 0x09, 0x03, 0x00, 0x00})));
}

TEST("decoders reject non-UTF-8 text fields") {
    // Push with the N flag, scope 0, suffix = one byte 0xFF (not valid UTF-8).
    CHECK(rejects<Push>(byteseq({0x3d, 0x00, 0x01, 0xFF})));
}

TEST("truncation sweeps never crash or read out of bounds") {
    // A Put carrying timestamp, encoding, source-info, attachment and payload.
    static constexpr std::array<std::byte, 4> schema{std::byte{'j'}, std::byte{'s'}, std::byte{'o'},
                                                     std::byte{'n'}};
    static constexpr std::array<std::byte, 3> att{std::byte{1}, std::byte{2}, std::byte{3}};
    static constexpr std::array<std::byte, 2> pl{std::byte{0xAA}, std::byte{0xBB}};
    Put put{};
    Timestamp ts{};
    ts.time = 0x0123456789ABCDEFull;
    ts.id_len = 3;
    put.timestamp = ts;
    put.encoding = Encoding{.id = 1234, .has_schema = true, .schema = schema};
    put.sinfo = SourceInfo{.id = {.zid = {}, .eid = 7}, .sn = 99};
    put.sinfo->id.zid.len = 4;
    put.attachment = Attachment{att};
    put.payload = pl;
    truncation_sweep<Put>(encoded(put));

    Push push{};
    push.wire_expr.suffix = std::string_view{"demo/example"};
    push.nodeid.node_id = 42;
    push.timestamp = ts;
    truncation_sweep<Push>(encoded(push));

    InitAck ack{};
    ack.version = 9;
    ack.identifier.zid.len = 16;
    static constexpr std::array<std::byte, 4> cookie{std::byte{1}, std::byte{2}, std::byte{3},
                                                     std::byte{4}};
    ack.cookie = cookie;
    ack.qos_link = QoSLink{0x1234};
    ack.lowlatency = HasLowLatency{};
    truncation_sweep<InitAck>(encoded(ack));

    Declare dec{};
    dec.id = 7u;
    dec.body.body = DeclareSubscriber{.id = 3, .wire_expr = {.suffix = std::string_view{"a/b"}}};
    truncation_sweep<Declare>(encoded(dec));

    Interest it{};
    it.id = 1;
    it.mode = InterestMode::current;
    it.inner.options = 0b1000'0101;
    it.inner.wire_expr = WireExpr{.suffix = std::string_view{"k"}};
    truncation_sweep<Interest>(encoded(it));

    CHECK(true); // reaching here without an ASan/UBSan abort is the assertion
}

TEST("encode rejects an empty zid (would underflow the length nibble)") {
    std::array<std::byte, 64> buf{};
    // InitIdentifier and EntityGlobalId both pack zid length as len-1 in a nibble; a
    // default (len 0) id must be rejected, not silently encoded as length 16.
    {
        ByteWriter w{buf};
        InitIdentifier id{}; // zid.len == 0
        CHECK(!id.encode(w).has_value());
    }
    {
        ByteWriter w{buf};
        EntityGlobalId g{}; // zid.len == 0
        CHECK(!g.encode_body(w).has_value());
    }
    // A valid 16-byte id still encodes.
    {
        ByteWriter w{buf};
        InitIdentifier id{};
        id.zid.len = 16;
        CHECK(id.encode(w).has_value());
    }
}

// `Timestamp::id_len` indexes a fixed 16-byte array but is a plain public member, so
// `encode` has to bound it like every other nibble-encoded zid length in the codec
// (EntityGlobalId, DestinationId, InitIdentifier). It used to trust it: encoding a
// Timestamp with id_len > 16 read past the array -- an ASan stack-buffer-overflow,
// and `encoded_len()` reported a size large enough for the whole overread to fit.
TEST("Timestamp::encode rejects an id_len past the end of the id array") {
    std::array<std::byte, 512> buf{};

    Timestamp over{};
    over.time = 1;
    over.id_len = 200; // way past the 16-byte array
    ByteWriter w1{buf};
    CHECK(!over.encode(w1).has_value());
    CHECK(w1.written() == 0);

    Timestamp just_over{};
    just_over.time = 1;
    just_over.id_len = 17;
    ByteWriter w2{buf};
    CHECK(!just_over.encode(w2).has_value());

    // An empty id is not encodable either: uhlc source ids are 1..16 bytes.
    Timestamp empty{};
    empty.time = 1;
    empty.id_len = 0;
    ByteWriter w3{buf};
    CHECK(!empty.encode(w3).has_value());

    // The boundary value still works, and round-trips.
    Timestamp ok{};
    ok.time = 42;
    ok.id_len = 16;
    ok.id.fill(std::byte{0xab});
    ByteWriter w4{buf};
    CHECK(ok.encode(w4).has_value());
    ByteReader r{std::span(buf).first(w4.written())};
    auto back = Timestamp::decode(r);
    CHECK(back.has_value());
    if (back) CHECK(*back == ok);
}

// The reference rejects an empty timestamp id too (`uhlc::ID::try_from` holds a
// NonZeroU128), so accepting one here would round-trip a value no reference peer
// considers valid.
TEST("Timestamp::decode rejects an empty id") {
    std::array<std::byte, 2> const empty_id{std::byte{0x2a}, std::byte{0x00}};
    CHECK(rejects<Timestamp>(empty_id));

    std::array<std::byte, 3> const too_long{std::byte{0x2a}, std::byte{0x11}, std::byte{0x00}};
    CHECK(rejects<Timestamp>(too_long)); // 17 > 16
}

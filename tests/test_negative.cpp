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

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <span>
#include <variant>
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

// An extension's KIND decides how many bytes it occupies, so a decoder that
// dispatches on the id alone and then reads the shape it expects consumes the wrong
// number of bytes when a peer sends a known id with a different kind: a Unit read as
// a ZStruct swallows a length byte plus that many bytes of what follows; a ZStruct
// read as a U64 leaves its body to be re-parsed as the next extension header. Nothing
// reads out of bounds, but the rest of the batch is desynchronized -- and a broker
// re-encodes and forwards the mangled result. The reference rejects this structurally
// (it compares the whole header byte bar the Z flag against a constant that bakes in
// the encoding), so accepting it was also an interop divergence.
TEST("a known extension id sent with the wrong KIND is rejected") {
    // Del + ext id 2 as *Unit* (0x02) where the decoder expects a ZStruct attachment.
    // Pre-fix this consumed 0x03 as a length and swallowed aa bb cc.
    std::array<std::byte, 6> const unit_for_zstruct{std::byte{0x82}, std::byte{0x02},
                                                    std::byte{0x03}, std::byte{0xaa},
                                                    std::byte{0xbb}, std::byte{0xcc}};
    CHECK(rejects<Del>(unit_for_zstruct));

    // Push + ext id 1 as Unit (0x11, mandatory) where a U64 QoS is expected: the
    // reader used to take the following byte as the VLE value.
    std::array<std::byte, 4> const unit_for_u64{std::byte{0x9d}, std::byte{0x00}, std::byte{0x11},
                                                std::byte{0x05}};
    CHECK(rejects<Push>(unit_for_u64));
}

// A Declare body that defines no extensions of its own still has to *read* the chain
// the Z bit announces. Five of them (DeclareKeyExpr, UndeclareKeyExpr,
// DeclareSubscriber, DeclareToken, DeclareFinal) ignored it, leaving the extension
// bytes in the reader to be misparsed as the next declaration -- the reference calls
// extension::skip_all for exactly these.
TEST("a Declare body honours the Z bit even with no extensions of its own") {
    // DeclareSubscriber (mid 2 | Z), id 1, scope 0, then a non-mandatory Unit ext,
    // then a DeclareFinal. The whole thing must decode as two bodies, in order.
    std::array<std::byte, 5> const bytes{std::byte{0x82}, std::byte{0x01}, std::byte{0x00},
                                         std::byte{0x0e}, std::byte{0x1a}};
    ByteReader r{bytes};
    auto first = DeclareBody::decode(r);
    CHECK(first.has_value());
    if (first) CHECK(std::holds_alternative<DeclareSubscriber>(first->body));
    CHECK(r.remaining() == 1); // pre-fix: 2, the ext byte was left behind
    auto second = DeclareBody::decode(r);
    CHECK(second.has_value());
    if (second) CHECK(std::holds_alternative<DeclareFinal>(second->body));

    // A *mandatory* unknown extension is rejected, as everywhere else in the codec.
    std::array<std::byte, 4> const mandatory{std::byte{0x82}, std::byte{0x01}, std::byte{0x00},
                                             std::byte{0x1e}};
    CHECK(rejects<DeclareSubscriber>(mandatory));
}

// Interest and InterestFinal share mid 0x19 and are told apart by the 2-bit MODE
// field. InterestFinal enforced its half (MODE == 0); Interest did not, so it decoded
// an InterestFinal's bytes and then read the *next* message's first byte as its own
// InterestInner options.
TEST("Interest rejects MODE=0, which is InterestFinal") {
    std::array<std::byte, 2> const final_bytes{std::byte{0x19}, std::byte{0x05}};
    CHECK(rejects<Interest>(final_bytes));
    // The same bytes are a perfectly good InterestFinal.
    ByteReader r{final_bytes};
    CHECK(InterestFinal::decode(r).has_value());

    // And encode is symmetric: a final_ Interest would emit bytes a correct
    // dispatcher hands to the other decoder.
    std::array<std::byte, 64> buf{};
    ByteWriter w{buf};
    Interest it{};
    it.mode = InterestMode::final_;
    CHECK(!it.encode(w).has_value());
}

// Every other narrowed field in this codec rejects a wire value that does not fit
// (get_uint_as<T>/read_ext_uint<T>); Encoding silently truncated to u16. That decodes
// to a different encoding than the wire carried and re-encodes to different bytes --
// and because Put/Err elide the E flag for a default encoding, an id truncating to 0
// drops the field entirely when a broker relays the message.
TEST("Encoding rejects an id wider than u16") {
    // combined = 0x20000 -> id 65536, no schema.
    std::array<std::byte, 3> const too_wide{std::byte{0x80}, std::byte{0x80}, std::byte{0x08}};
    ByteReader r{too_wide};
    CHECK(!Encoding::decode(r).has_value());

    // The boundary still decodes, and round-trips byte-identically.
    std::array<std::byte, 3> const max_id{std::byte{0xfe}, std::byte{0xff}, std::byte{0x07}};
    ByteReader r2{max_id};
    auto e = Encoding::decode(r2);
    CHECK(e.has_value());
    if (e) {
        CHECK(e->id == 0xffff && !e->has_schema);
        std::array<std::byte, 16> buf{};
        ByteWriter w{buf};
        CHECK(e->encode(w).has_value());
        CHECK(w.written() == max_id.size());
        CHECK(std::equal(max_id.begin(), max_id.end(), buf.begin()));
    }
}

// A ZStruct extension declares its body length, and the sub-reader is bounded to it,
// so trailing bytes inside the body cannot desynchronize the outer stream. They are
// still wrong: the message decodes as if they were absent and re-encodes shorter than
// it arrived, so a broker relaying it silently rewrites the peer's bytes. Fixed-shape
// bodies now require exact consumption (Attachment and Value's payload are defined as
// "the rest of the slice" and are meant to consume it).
TEST("a fixed-shape extension body with trailing bytes is rejected") {
    // A SourceInfo body (Del's ext id 1, a ZStruct), and the same body declared one
    // byte longer with a junk byte appended.
    SourceInfo si{};
    si.id.zid.len = 1;
    si.id.zid.bytes[0] = std::byte{0x11};
    si.id.eid = 3;
    si.sn = 9;
    std::array<std::byte, 64> body_buf{};
    ByteWriter bw{body_buf};
    CHECK(si.encode_body(bw).has_value());
    auto const body = std::span(body_buf).first(bw.written());

    auto build = [&](std::size_t declared_len, bool trailing) {
        std::vector<std::byte> out;
        out.push_back(std::byte{0x82});                      // Del, Z set
        out.push_back(std::byte{0x41});                      // ext id 1, ZStruct kind
        out.push_back(static_cast<std::byte>(declared_len)); // body length
        out.insert(out.end(), body.begin(), body.end());
        if (trailing) out.push_back(std::byte{0x0c});
        return out;
    };

    CHECK(rejects<Del>(build(body.size() + 1, /*trailing=*/true)));

    // The honest encoding still decodes, and carries the SourceInfo.
    auto const good = build(body.size(), /*trailing=*/false);
    ByteReader r{good};
    auto del = Del::decode(r);
    CHECK(del.has_value());
    if (del) CHECK(del->sinfo.has_value());
}

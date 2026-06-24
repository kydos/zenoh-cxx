// Differential tests against the authoritative zenoh-rust codec.
//
// `tools/vector-gen` emits golden wire vectors by encoding random instances of
// every message type with zenoh-rust's own codec (tests/diff_vectors.hpp). Here we
// decode each vector with our C++ codec and assert it re-encodes byte-identically
// and consumes the whole buffer — i.e. our wire format matches zenoh-rust exactly.
import zenoh.proto.network;
import zenoh.proto.transport;
import zenoh.proto.declare;
import zenoh.proto.interest;
import zenoh.proto.fields;
import zenoh.proto.exts;
import zenoh.buffer;
import zenoh.util;

#include "diff_vectors.hpp"
#include "ztest.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdio>
#include <span>
#include <string_view>
#include <vector>

using namespace zenoh;

namespace {

auto dump(const char* label, std::span<const std::byte> s) -> void {
    std::fprintf(stderr, "  %s [%zu]:", label, s.size());
    for (auto b : s) std::fprintf(stderr, " %02x", std::to_integer<unsigned>(b));
    std::fprintf(stderr, "\n");
}

// Decode `g` as T and assert it re-encodes byte-identically (and consumed it all).
template <class T> auto check(std::string_view name, std::span<const std::byte> g) -> bool {
    ByteReader r{g};
    auto decoded = T::decode(r);
    if (!decoded) {
        std::fprintf(stderr, "decode FAILED for \"%.*s\"\n", static_cast<int>(name.size()),
                     name.data());
        dump("input", g);
        return false;
    }
    if (r.remaining() != 0) {
        std::fprintf(stderr, "decode left %zu bytes for \"%.*s\"\n", r.remaining(),
                     static_cast<int>(name.size()), name.data());
        return false;
    }
    std::vector<std::byte> buf(g.size() + 16);
    ByteWriter w{buf};
    if (!decoded->encode(w)) {
        std::fprintf(stderr, "re-encode FAILED for \"%.*s\"\n", static_cast<int>(name.size()),
                     name.data());
        return false;
    }
    std::span<const std::byte> got{buf.data(), w.written()};
    if (!std::ranges::equal(got, g)) {
        std::fprintf(stderr, "re-encode MISMATCH for \"%.*s\"\n", static_cast<int>(name.size()),
                     name.data());
        dump("expected", g);
        dump("got     ", got);
        return false;
    }
    return true;
}

// Dispatch a vector to the matching message decoder by its name prefix. Longer
// (more specific) prefixes are tested first.
auto dispatch(std::string_view name, std::span<const std::byte> g) -> bool {
    using std::string_view;
    auto is = [&](string_view p) { return name.starts_with(p); };

    if (is("rand_put_")) return check<Put>(name, g);
    if (is("rand_del_")) return check<Del>(name, g);
    if (is("rand_query_")) return check<Query>(name, g);
    if (is("rand_err_")) return check<Err>(name, g);
    if (is("rand_reply_")) return check<Reply>(name, g);
    if (is("rand_push_")) return check<Push>(name, g);
    if (is("rand_request_")) return check<Request>(name, g);
    if (is("rand_responsefinal_")) return check<ResponseFinal>(name, g);
    if (is("rand_response_")) return check<Response>(name, g);
    if (is("rand_declare_")) return check<Declare>(name, g);
    if (is("rand_interestfinal_")) return check<InterestFinal>(name, g);
    if (is("rand_interest_")) return check<Interest>(name, g);
    if (is("rand_initsyn_")) return check<InitSyn>(name, g);
    if (is("rand_initack_")) return check<InitAck>(name, g);
    if (is("rand_opensyn_")) return check<OpenSyn>(name, g);
    if (is("rand_openack_")) return check<OpenAck>(name, g);
    if (is("rand_close_")) return check<Close>(name, g);
    if (is("rand_keepalive_")) return check<KeepAlive>(name, g);
    if (is("rand_frameheader_")) return check<FrameHeader>(name, g);

    std::fprintf(stderr, "unhandled vector \"%.*s\"\n", static_cast<int>(name.size()), name.data());
    return false;
}

} // namespace

TEST("differential: every zenoh-rust vector decodes and re-encodes identically") {
    std::size_t handled = 0;
    for (const auto& v : diffvec::all) {
        std::span<const std::byte> g{reinterpret_cast<const std::byte*>(v.data), v.size};
        CHECK(dispatch(v.name, g));
        ++handled;
    }
    CHECK(handled == std::size(diffvec::all));
    CHECK(handled > 0);
}

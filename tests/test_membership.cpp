// Unit tests for zenoh.broker.membership: the clique's member table, the endpoint
// validation that keeps a misconfigured peer from poisoning it, the dial tie-break
// that collapses simultaneous mutual dials, and the gossip payload codec.
//
// Pure -- no broker, no sockets, no strand. This module was factored out of the
// broker precisely so these decisions could be pinned down here rather than only
// observed indirectly through a live mesh (test_clique.cpp does that part).
import zenoh.broker.membership;
import zenoh.proto;

#include "ztest.hpp"

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

using namespace zenoh::broker;
using zenoh::ZenohId;

namespace {

// A zid whose significant bytes are `first` followed by zeroes.
auto zid(std::uint8_t first, std::uint8_t len = 16) -> ZenohId {
    ZenohId z{};
    z.len = len;
    z.bytes[0] = static_cast<std::byte>(first);
    return z;
}

auto member(std::uint8_t first, std::vector<std::string> eps = {}) -> MemberInfo {
    return MemberInfo{.zid = zid(first), .endpoints = std::move(eps)};
}

} // namespace

TEST("is_internal_key reserves the @/ prefix and nothing else") {
    CHECK(is_internal_key(gossip_key));
    CHECK(is_internal_key("@/router/gossip"));
    CHECK(is_internal_key("@/anything"));
    CHECK(!is_internal_key("demo/example"));
    CHECK(!is_internal_key("@"));     // no separator: an ordinary one-chunk key
    CHECK(!is_internal_key("a/@/b")); // only the prefix is reserved
    CHECK(!is_internal_key(""));
}

TEST("zid_less is a strict total order over Zenoh ids") {
    auto const a = zid(1);
    auto const b = zid(2);
    CHECK(zid_less(a, b));
    CHECK(!zid_less(b, a));
    CHECK(!zid_less(a, a)); // irreflexive

    // Length dominates, so the zero padding a ZenohId carries never affects the
    // comparison of ids of different lengths.
    auto const shortid = zid(9, 4);
    auto const longid = zid(1, 16);
    CHECK(zid_less(shortid, longid));
    CHECK(!zid_less(longid, shortid));
}

TEST("keep_outbound_link makes both ends of a mutual dial agree on one survivor") {
    auto const lo = zid(1);
    auto const hi = zid(2);

    // Each end evaluates it with its own zid first; exactly one gets true, so
    // exactly one outbound link is kept and the other end keeps the matching
    // inbound one. This is the whole correctness argument for the tie-break.
    CHECK(keep_outbound_link(lo, hi));
    CHECK(!keep_outbound_link(hi, lo));

    // A broker that somehow dialled itself keeps nothing -- the caller closes it.
    CHECK(!keep_outbound_link(lo, lo));
}

TEST("is_dialable_endpoint accepts real endpoints and rejects unusable ones") {
    CHECK(is_dialable_endpoint("tcp/127.0.0.1:7447"));
    CHECK(is_dialable_endpoint("127.0.0.1:7447"));
    CHECK(is_dialable_endpoint("example.com:7447"));
    CHECK(is_dialable_endpoint("tcp/[::1]:7447"));
    CHECK(is_dialable_endpoint("[2001:db8::1]:1"));

    CHECK(!is_dialable_endpoint(""));
    CHECK(!is_dialable_endpoint("no-port"));
    CHECK(!is_dialable_endpoint("tcp/127.0.0.1:"));
    CHECK(!is_dialable_endpoint(":7447"));
    CHECK(!is_dialable_endpoint("tcp/127.0.0.1:0"));               // port 0 is not a target
    CHECK(!is_dialable_endpoint("tcp/127.0.0.1:70000"));           // out of range
    CHECK(!is_dialable_endpoint("tcp/127.0.0.1:74a7"));            // not a number
    CHECK(!is_dialable_endpoint("tcp/[::1]"));                     // bracketed, no port
    CHECK(!is_dialable_endpoint("tcp/[]:7447"));                   // empty literal
    CHECK(!is_dialable_endpoint(std::string(400, 'a') + ":7447")); // over the length cap
}

TEST("learn reports only genuinely new information") {
    Membership m;
    m.set_self(zid(1), {"tcp/127.0.0.1:7447"});

    CHECK(m.learn(member(2, {"tcp/10.0.0.2:7447"})));  // new broker
    CHECK(!m.learn(member(2, {"tcp/10.0.0.2:7447"}))); // nothing new
    CHECK(m.learn(member(2, {"tcp/10.0.0.9:7447"})));  // an additional endpoint
    CHECK(m.peer_count() == 1);

    auto const eps = m.endpoints_of(zid(2));
    CHECK(eps.size() == 2);

    // A broker's own announcement coming back around the clique teaches it nothing.
    CHECK(!m.learn(member(1, {"tcp/127.0.0.1:7447"})));
    CHECK(m.peer_count() == 1);
}

TEST("learn drops unusable zids and unusable endpoints") {
    Membership m;
    m.set_self(zid(1), {});

    MemberInfo bad_zid{};
    bad_zid.zid.len = 0;
    CHECK(!m.learn(bad_zid));
    CHECK(m.peer_count() == 0);

    // A member with only junk endpoints is still worth recording -- it exists, it
    // simply cannot be dialled -- but the junk itself is never stored.
    CHECK(m.learn(member(3, {"garbage", "tcp/1.2.3.4:0"})));
    CHECK(m.peer_count() == 1);
    CHECK(m.endpoints_of(zid(3)).empty());

    CHECK(m.learn(member(3, {"tcp/1.2.3.4:7447", "still-garbage"})));
    CHECK(m.endpoints_of(zid(3)).size() == 1);
}

TEST("the member table is bounded so gossip cannot grow it without limit") {
    Membership m;
    m.set_self(zid(0), {});
    for (std::size_t i = 0; i < Membership::max_members + 50; ++i) {
        MemberInfo mi{};
        mi.zid.len = 16;
        mi.zid.bytes[0] = static_cast<std::byte>(i & 0xff);
        mi.zid.bytes[1] = static_cast<std::byte>((i >> 8) & 0xff);
        (void)m.learn(mi);
    }
    CHECK(m.peer_count() <= Membership::max_members);

    // And per-member endpoints are capped too.
    Membership m2;
    m2.set_self(zid(1), {});
    std::vector<std::string> many;
    for (std::size_t i = 0; i < Membership::max_endpoints_per_member + 10; ++i) {
        many.push_back("tcp/10.0.0.1:" + std::to_string(1000 + i));
    }
    (void)m2.learn(MemberInfo{.zid = zid(2), .endpoints = many});
    CHECK(m2.endpoints_of(zid(2)).size() == Membership::max_endpoints_per_member);
}

TEST("snapshot carries this broker's own entry alongside everything it has learned") {
    Membership m;
    m.set_self(zid(1), {"tcp/127.0.0.1:7447"});
    CHECK(m.learn(member(2, {"tcp/10.0.0.2:7447"})));
    CHECK(m.learn(member(3, {})));

    auto const snap = m.snapshot();
    CHECK(snap.size() == 3);
    CHECK(snap[0].zid == zid(1)); // self first
    CHECK(snap[0].endpoints.size() == 1);
}

TEST("a gossip payload round-trips through encode/decode unchanged") {
    std::vector<MemberInfo> members;
    members.push_back(member(1, {"tcp/127.0.0.1:7447"}));
    members.push_back(member(2, {"tcp/10.0.0.2:7447", "tcp/[::1]:7447"}));
    members.push_back(member(3, {})); // a broker that advertises nothing is still announced

    auto const bytes = Membership::encode(members);
    CHECK(!bytes.empty());
    auto const back = Membership::decode(bytes);
    CHECK(back.has_value());
    if (back) CHECK(*back == members);
}

TEST("an empty member list round-trips") {
    auto const bytes = Membership::encode({});
    CHECK(!bytes.empty()); // a zero count is still a byte
    auto const back = Membership::decode(bytes);
    CHECK(back.has_value());
    if (back) CHECK(back->empty());
}

TEST("decode rejects malformed gossip rather than misparsing it") {
    auto const good = Membership::encode({member(1, {"tcp/127.0.0.1:7447"})});
    CHECK(!good.empty());

    // Every truncation must be rejected, never partially accepted.
    for (std::size_t n = 0; n < good.size(); ++n) {
        auto const truncated = Membership::decode(std::span(good).first(n));
        CHECK(!truncated.has_value());
    }

    // Trailing bytes mean the peer and this decoder disagree about the format.
    std::vector<std::byte> extra = good;
    extra.push_back(std::byte{0xff});
    CHECK(!Membership::decode(extra).has_value());

    // A zid length of 0 or >16 is not representable and must not be accepted.
    std::vector<std::byte> bad_len = good;
    bad_len[1] = std::byte{0};
    CHECK(!Membership::decode(bad_len).has_value());
    bad_len[1] = std::byte{17};
    CHECK(!Membership::decode(bad_len).has_value());

    // A member count far beyond the cap must be rejected before anything is
    // allocated for it.
    std::vector<std::byte> huge{std::byte{0xff}, std::byte{0xff}, std::byte{0xff}, std::byte{0x7f}};
    CHECK(!Membership::decode(huge).has_value());

    CHECK(!Membership::decode(std::span<const std::byte>{}).has_value());
}

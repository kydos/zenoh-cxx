module;

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

module zenoh.broker.membership;

import zenoh.buffer;
import zenoh.codec;
import zenoh.util;

// Implementation unit for zenoh.broker.membership.
namespace zenoh::broker {

namespace {

// Upper bound on one advertised endpoint string. Long enough for a bracketed IPv6
// literal plus a port, short enough that the member table stays bounded in bytes and
// not just in entries.
constexpr std::size_t max_endpoint_len = 256;

// Generous ceiling on the whole encoded payload, so a hostile peer cannot make us
// allocate for a member list it never actually sends.
constexpr std::size_t max_gossip_bytes = std::size_t{64} * 1024;

[[nodiscard]] auto find_member(std::vector<MemberInfo>& members, const ZenohId& zid)
    -> MemberInfo* {
    for (auto& m : members) {
        if (m.zid == zid) return &m;
    }
    return nullptr;
}

} // namespace

auto is_internal_key(std::string_view key) noexcept -> bool { return key.starts_with("@/"); }

auto zid_less(const ZenohId& a, const ZenohId& b) noexcept -> bool {
    if (a.len != b.len) return a.len < b.len;
    return std::lexicographical_compare(a.bytes.begin(), a.bytes.begin() + a.len, b.bytes.begin(),
                                        b.bytes.begin() + b.len);
}

auto keep_outbound_link(const ZenohId& self, const ZenohId& peer) noexcept -> bool {
    // "The link dialled by the lower zid survives." The peer evaluates
    // keep_outbound_link(peer, self), which is the negation of this whenever the two
    // ids differ -- so exactly one side keeps its outbound link and the other keeps
    // the matching inbound one. Equal zids mean a broker dialled itself; keeping
    // nothing is the right answer, and the caller closes the link.
    return zid_less(self, peer);
}

auto is_dialable_endpoint(std::string_view endpoint) noexcept -> bool {
    if (endpoint.empty() || endpoint.size() > max_endpoint_len) return false;
    if (endpoint.starts_with("tcp/")) endpoint.remove_prefix(4);

    std::string_view port;
    if (endpoint.starts_with('[')) {
        auto const close = endpoint.find(']');
        if (close == std::string_view::npos || close == 1) return false;
        if (close + 2 >= endpoint.size() || endpoint[close + 1] != ':') return false;
        port = endpoint.substr(close + 2);
    } else {
        auto const colon = endpoint.rfind(':');
        if (colon == std::string_view::npos || colon == 0 || colon + 1 == endpoint.size()) {
            return false;
        }
        port = endpoint.substr(colon + 1);
    }

    std::uint32_t value = 0;
    for (char const c : port) {
        if (c < '0' || c > '9') return false;
        value = value * 10 + static_cast<std::uint32_t>(c - '0');
        if (value > 0xffff) return false;
    }
    return value != 0;
}

auto Membership::set_self(ZenohId zid, std::vector<std::string> endpoints) -> void {
    std::erase_if(endpoints, [](const std::string& e) { return !is_dialable_endpoint(e); });
    if (endpoints.size() > max_endpoints_per_member) endpoints.resize(max_endpoints_per_member);
    self_ = MemberInfo{.zid = zid, .endpoints = std::move(endpoints)};
}

auto Membership::learn(const MemberInfo& m) -> bool {
    if (m.zid.len == 0) return false;
    if (m.zid == self_.zid) return false; // our own announcement, echoed back to us

    auto* existing = find_member(peers_, m.zid);
    if (existing == nullptr) {
        if (peers_.size() >= max_members) return false;
        MemberInfo entry{.zid = m.zid, .endpoints = {}};
        for (auto const& e : m.endpoints) {
            if (entry.endpoints.size() >= max_endpoints_per_member) break;
            if (is_dialable_endpoint(e)) entry.endpoints.push_back(e);
        }
        peers_.push_back(std::move(entry));
        return true;
    }

    // Known broker: merge in any endpoint we hadn't heard before. Endpoints are only
    // ever added, never replaced -- a broker that moves keeps its old address in the
    // table (harmless, it simply never connects) rather than risking a stale
    // announcement erasing the address that currently works.
    bool changed = false;
    for (auto const& e : m.endpoints) {
        if (!is_dialable_endpoint(e)) continue;
        if (existing->endpoints.size() >= max_endpoints_per_member) break;
        if (std::find(existing->endpoints.begin(), existing->endpoints.end(), e) !=
            existing->endpoints.end()) {
            continue;
        }
        existing->endpoints.push_back(e);
        changed = true;
    }
    return changed;
}

auto Membership::snapshot() const -> std::vector<MemberInfo> {
    std::vector<MemberInfo> out;
    out.reserve(peers_.size() + 1);
    if (self_.zid.len != 0) out.push_back(self_);
    out.insert(out.end(), peers_.begin(), peers_.end());
    return out;
}

auto Membership::endpoints_of(const ZenohId& zid) const -> std::vector<std::string> {
    for (auto const& m : peers_) {
        if (m.zid == zid) return m.endpoints;
    }
    return {};
}

// Wire form (all integers VLE):
//   count
//   per member: zid_len:u8, zid bytes, endpoint_count, then per endpoint a
//               length-prefixed string.
auto Membership::encode(const std::vector<MemberInfo>& members) -> std::vector<std::byte> {
    std::size_t bound = 16;
    for (auto const& m : members) {
        bound += 32 + m.zid.len;
        for (auto const& e : m.endpoints) bound += 16 + e.size();
    }
    if (bound > max_gossip_bytes) return {};

    std::vector<std::byte> buf(bound);
    ByteWriter w{buf};
    if (!put_uint(w, members.size())) return {};
    for (auto const& m : members) {
        if (!w.write_byte(static_cast<std::byte>(m.zid.len))) return {};
        if (!put_raw(w, m.zid.view())) return {};
        if (!put_uint(w, m.endpoints.size())) return {};
        for (auto const& e : m.endpoints) {
            if (!put_prefixed_str(w, e)) return {};
        }
    }
    buf.resize(w.written());
    return buf;
}

auto Membership::decode(std::span<const std::byte> payload)
    -> std::optional<std::vector<MemberInfo>> {
    if (payload.size() > max_gossip_bytes) return std::nullopt;
    ByteReader r{payload};

    auto count = get_uint(r);
    if (!count || *count > max_members) return std::nullopt;

    std::vector<MemberInfo> out;
    out.reserve(static_cast<std::size_t>(*count));
    for (std::uint64_t i = 0; i < *count; ++i) {
        auto len = r.read_byte();
        if (!len) return std::nullopt;
        auto const zid_len = std::to_integer<std::uint8_t>(*len);
        if (zid_len == 0 || zid_len > 16) return std::nullopt;
        auto zid_bytes = r.read_slice(zid_len);
        if (!zid_bytes) return std::nullopt;

        MemberInfo m{};
        m.zid.len = zid_len;
        __builtin_memcpy(m.zid.bytes.data(), zid_bytes->data(), zid_len);

        auto ep_count = get_uint(r);
        if (!ep_count || *ep_count > max_endpoints_per_member) return std::nullopt;
        m.endpoints.reserve(static_cast<std::size_t>(*ep_count));
        for (std::uint64_t j = 0; j < *ep_count; ++j) {
            auto e = get_prefixed_str(r);
            if (!e || e->size() > max_endpoint_len) return std::nullopt;
            m.endpoints.emplace_back(*e);
        }
        out.push_back(std::move(m));
    }
    // Trailing bytes mean the sender and this decoder disagree about the format;
    // reject rather than silently accepting a prefix of something else.
    if (r.remaining() != 0) return std::nullopt;
    return out;
}

} // namespace zenoh::broker

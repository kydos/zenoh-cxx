module;

#include <asio/strand.hpp>

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

module zenoh.broker.tables;

import zenoh.proto;
import zenoh.ke;
import zenoh.broker.resource;

// Implementation unit for zenoh.broker.tables: face registry maintenance and the
// Push/Request/Response/ResponseFinal routing algorithms.
namespace zenoh::broker {

namespace {

// A publish/query key straight off the wire is meant to be a literal (wildcard-free)
// key; a `*`/`**` in a *published* key isn't a wildcard, it's malformed input from a
// buggy or malicious peer, and treating it as one via ke::intersects would let it
// match far more subscriptions than intended. Reject rather than silently
// mismatching.
[[nodiscard]] auto is_literal_key(std::string_view key) noexcept -> bool {
    return key.find('*') == std::string_view::npos;
}

// Encode `m` into a freshly allocated block of `capacity` bytes (a generous upper
// bound on the encoded size). Null on a pathological encode failure; callers skip
// that delivery rather than faulting the whole routing step.
template <class Msg>
[[nodiscard]] auto encode_shared(const Msg& m, std::size_t capacity) -> SharedBuf {
    auto out = SharedBuf::allocate(capacity);
    if (!out) return {};
    ByteWriter w{out.storage()};
    if (!m.encode(w)) return {};
    out.finish(w.written());
    return out;
}

[[nodiscard]] auto encode_request(std::uint32_t rid, std::string_view key,
                                  std::string_view parameters,
                                  const std::optional<std::vector<std::byte>>& payload,
                                  QueryTarget target, const std::optional<ZenohId>& dest, QoS qos)
    -> SharedBuf {
    Request req{};
    req.id = rid;
    req.qos = qos;
    req.wire_expr = WireExpr{.scope = 0, .mapping = Mapping::sender, .suffix = key};
    req.target = target;
    // Carried through rather than dropped: this request may be on its way to a peer
    // broker, and the zid it targets is very likely one of *that* broker's clients --
    // the filter is applied at the terminal hop, so the extension has to survive the
    // hop to get there. Forwarding it to a local client queryable as well is
    // harmless (a Session does not re-filter on it; the broker already did).
    if (dest) req.dest = DestinationId{.zid = *dest};
    Query q{};
    q.parameters = parameters;
    if (payload) q.body = Value{.encoding = Encoding{}, .payload = *payload};
    req.payload = RequestBody{.query = q};

    std::size_t const payload_size = payload ? payload->size() : 0;
    return encode_shared(req, 96 + key.size() + parameters.size() + payload_size);
}

[[nodiscard]] auto encode_response(std::uint32_t rid, bool ok, std::string_view key,
                                   std::span<const std::byte> payload, QoS qos) -> SharedBuf {
    Response rsp{};
    rsp.rid = rid;
    rsp.qos = qos;
    if (ok) {
        rsp.wire_expr = WireExpr{.scope = 0, .mapping = Mapping::sender, .suffix = key};
        Put put{};
        put.payload = payload;
        rsp.payload.body = Reply{.payload = PushBody{.body = put}};
    } else {
        Err e{};
        e.payload = payload;
        rsp.payload.body = e;
    }
    return encode_shared(rsp, 96 + key.size() + payload.size());
}

[[nodiscard]] auto encode_response_final(std::uint32_t rid, QoS qos = QoS{}) -> SharedBuf {
    ResponseFinal rf{};
    rf.rid = rid;
    rf.qos = qos;
    return encode_shared(rf, 32);
}

/// The standard Zenoh congestion-control flag: QoS bit 3 set means `Block`.
[[nodiscard]] auto is_block(QoS qos) noexcept -> bool { return (qos.inner & 0x08) != 0; }

/// A QoS byte marked `Block`. Used for every `ResponseFinal`: it is the message that
/// *terminates* a query, so dropping it under congestion would not lose data, it
/// would leave the requester waiting for something that is never coming (until its
/// own client-side timeout). Terminators are a handful of bytes, so queuing them
/// past the watermark costs nothing worth measuring.
[[nodiscard]] auto qos_block() noexcept -> QoS {
    QoS q{};
    q.inner |= 0x08;
    return q;
}

/// A face's current backpressure level; `ok` for a handle without the shared flag.
[[nodiscard]] auto face_pressure(const FaceHandle& face) noexcept -> FacePressure {
    return face.pressure ? face.pressure->load(std::memory_order_relaxed) : FacePressure::ok;
}

// The four declaration messages a broker sends to its clique peers.
//
// Wire shape follows the reference implementation's router-to-router path
// (zenoh/src/net/routing/hat/router/pubsub.rs), which differs from the client-facing
// one in two ways that matter:
//
//  * the entity `id` is always 0 -- a router-sourced declaration is keyed by
//    (key expression, originating broker), never by an id, because it is an
//    aggregate over that broker's clients rather than any single entity;
//  * consequently the *undeclare* has to carry the key expression itself, in the
//    `wire_expr` extension, since the id cannot identify what is being withdrawn.
//
// `interest_id` is never set: routers do not answer each other's Interests (the
// reference's router hat ignores them outright), so a declaration here is never a
// reply to one.
[[nodiscard]] auto encode_declare_subscriber(std::string_view key) -> SharedBuf {
    Declare d{};
    d.body.body = DeclareSubscriber{
        .id = 0, .wire_expr = WireExpr{.scope = 0, .mapping = Mapping::sender, .suffix = key}};
    return encode_shared(d, 96 + key.size());
}

[[nodiscard]] auto encode_undeclare_subscriber(std::string_view key) -> SharedBuf {
    Declare d{};
    d.body.body = UndeclareSubscriber{
        .id = 0, .wire_expr = WireExpr{.scope = 0, .mapping = Mapping::sender, .suffix = key}};
    return encode_shared(d, 96 + key.size());
}

[[nodiscard]] auto encode_declare_queryable(std::string_view key, QueryableInfo qinfo)
    -> SharedBuf {
    Declare d{};
    d.body.body = DeclareQueryable{
        .id = 0,
        .wire_expr = WireExpr{.scope = 0, .mapping = Mapping::sender, .suffix = key},
        .qinfo = qinfo};
    return encode_shared(d, 96 + key.size());
}

[[nodiscard]] auto encode_undeclare_queryable(std::string_view key) -> SharedBuf {
    Declare d{};
    d.body.body = UndeclareQueryable{
        .id = 0, .wire_expr = WireExpr{.scope = 0, .mapping = Mapping::sender, .suffix = key}};
    return encode_shared(d, 96 + key.size());
}

} // namespace

namespace {

// The Push message `encode_push_into`/`make_push_msg` both compose.
[[nodiscard]] auto push_message(std::string_view key, std::span<const std::byte> payload,
                                bool is_del) -> Push {
    Push push{};
    push.wire_expr = WireExpr{.scope = 0, .mapping = Mapping::sender, .suffix = key};
    if (is_del) {
        push.payload = PushBody{.body = Del{}};
    } else {
        Put put{};
        put.payload = payload;
        push.payload = PushBody{.body = put};
    }
    return push;
}

// Generous upper bound on one encoded Push, covering every header/extension byte the
// encoder can emit around the key and payload it carries.
[[nodiscard]] auto push_bound(std::string_view key, std::span<const std::byte> payload)
    -> std::size_t {
    return 96 + key.size() + payload.size();
}

} // namespace

namespace {

// Make sure `out` has room for `need` bytes past `offset`, growing (or creating) the
// block and carrying over what is already encoded into it. Only ever called while
// the caller still holds the sole reference, so nobody can be reading the old
// storage as it is replaced. False if the block could not be allocated.
[[nodiscard]] auto reserve_at(SharedBuf& out, std::size_t offset, std::size_t need) -> bool {
    if (out && offset + need <= out.capacity()) return true;
    std::size_t const want = std::max(offset + need, 2 * out.capacity());
    auto bigger = SharedBuf::allocate(want);
    if (!bigger) return false;
    if (offset != 0) __builtin_memcpy(bigger.storage().data(), out.bytes().data(), offset);
    out = std::move(bigger);
    return true;
}

} // namespace

auto encode_push_into(SharedBuf& out, std::size_t offset, const Push& push)
    -> std::optional<MsgSlice> {
    std::span<const std::byte> payload;
    if (auto const* put = std::get_if<Put>(&push.payload.body)) payload = put->payload;
    if (!reserve_at(out, offset, push_bound(push.wire_expr.suffix, payload))) return std::nullopt;
    ByteWriter w{out.storage().subspan(offset)};
    if (!push.encode(w)) return std::nullopt;
    return MsgSlice{.offset = static_cast<std::uint32_t>(offset),
                    .length = static_cast<std::uint32_t>(w.written())};
}

auto copy_msg_into(SharedBuf& out, std::size_t offset, std::span<const std::byte> msg)
    -> std::optional<MsgSlice> {
    if (msg.empty() || !reserve_at(out, offset, msg.size())) return std::nullopt;
    __builtin_memcpy(out.storage().data() + offset, msg.data(), msg.size());
    return MsgSlice{.offset = static_cast<std::uint32_t>(offset),
                    .length = static_cast<std::uint32_t>(msg.size())};
}

auto make_push_msg(std::string_view key, std::span<const std::byte> payload, bool is_del)
    -> SharedBuf {
    return encode_shared(push_message(key, payload, is_del), push_bound(key, payload));
}

auto Tables::deliver_one(const FaceHandle& face, SharedBuf msg, bool block) -> void {
    if (!msg || !face.deliver) return;
    std::vector<MsgSlice> one;
    one.reserve(1);
    one.push_back(
        MsgSlice{.offset = 0, .length = static_cast<std::uint32_t>(msg.size()), .block = block});
    face.deliver(std::move(msg), std::move(one));
}

auto Tables::set_self_endpoints(std::vector<std::string> endpoints) -> void {
    members_.set_self(router_zid_, std::move(endpoints));
}

auto Tables::set_dial_request(std::function<void(std::string endpoint)> fn) -> void {
    dial_request_ = std::move(fn);
}

auto Tables::router_face_with_zid(const ZenohId& zid, FaceId except) const -> FaceId {
    for (auto const& [id, face] : faces_) {
        if (id == except || face.kind != FaceKind::router) continue;
        if (face.zid == zid) return id;
    }
    return 0;
}

auto Tables::collapse_duplicate_link(FaceId id, const ZenohId& zid, bool dialed) -> bool {
    FaceId const other = router_face_with_zid(zid, id);
    if (other == 0) return false;

    // Both ends evaluate the same pure function of the two identities and therefore
    // reach complementary answers, so exactly one link is closed -- deciding by
    // arrival order instead could close both, or neither.
    bool const keep_outbound = keep_outbound_link(router_zid_, zid);
    auto other_it = faces_.find(other);
    bool const other_dialed = other_it != faces_.end() && other_it->second.dialed;

    FaceId victim = id;
    if (dialed == keep_outbound && other_dialed != keep_outbound) {
        victim = other;
    } else if (other_dialed == keep_outbound && dialed != keep_outbound) {
        victim = id;
    }
    // (Two links in the *same* direction can only come from this broker dialling the
    // same peer twice, which the connector bookkeeping prevents; closing the newer
    // one is the safe fallback.)

    auto vit = faces_.find(victim);
    if (vit != faces_.end()) {
        // Park the loser's connector against the *survivor's* peer, so it stays
        // parked exactly as long as a working link to that broker exists.
        if (vit->second.dial_suppress) {
            vit->second.dial_suppress->store(true, std::memory_order_relaxed);
            suppressed_dials_.emplace_back(zid, vit->second.dial_suppress);
        }
        if (vit->second.close) vit->second.close();
    }
    return victim == id;
}

auto Tables::release_dial_suppression(const ZenohId& zid) -> void {
    for (auto it = suppressed_dials_.begin(); it != suppressed_dials_.end();) {
        if (it->first == zid) {
            it->second->store(false, std::memory_order_relaxed);
            it = suppressed_dials_.erase(it);
        } else {
            ++it;
        }
    }
}

auto Tables::send_gossip_to(FaceId to) -> void {
    auto it = faces_.find(to);
    if (it == faces_.end()) return;
    auto const payload = Membership::encode(members_.snapshot());
    if (payload.empty()) return;
    deliver_one(it->second, make_push_msg(gossip_key, payload, /*is_del=*/false));
}

auto Tables::advertise_membership() -> void {
    auto const payload = Membership::encode(members_.snapshot());
    if (payload.empty()) return;
    to_routers(make_push_msg(gossip_key, payload, /*is_del=*/false));
}

auto Tables::dial_missing_peers() -> void {
    if (!dial_request_) return;
    for (auto const& m : members_.snapshot()) {
        if (m.zid == router_zid_) continue;
        if (router_face_with_zid(m.zid, 0) != 0) continue; // already linked
        for (auto const& ep : m.endpoints) dial_request_(ep);
    }
}

auto Tables::unlinked_peer_count() const -> std::size_t {
    assert(strand_.running_in_this_thread());
    std::size_t n = 0;
    for (auto const& m : members_.snapshot()) {
        if (m.zid == router_zid_) continue;
        if (router_face_with_zid(m.zid, 0) == 0) ++n;
    }
    return n;
}

auto Tables::on_gossip(FaceId from, std::span<const std::byte> payload) -> void {
    assert(strand_.running_in_this_thread());
    if (!is_router_face(from)) return; // only a peer broker may speak membership
    auto decoded = Membership::decode(payload);
    if (!decoded) return; // untrusted input: ignore, never fault the link

    bool changed = false;
    for (auto const& m : *decoded) {
        if (members_.learn(m)) changed = true;
    }
    if (!changed) return;

    // One round is enough to close a clique: the newcomer hears about everyone from
    // the broker it seeded against, and everyone hears about the newcomer from the
    // same re-advertisement.
    dial_missing_peers();
    advertise_membership();
}

auto Tables::add_face(FaceHandle handle) -> void {
    assert(strand_.running_in_this_thread());
    bool const is_router = handle.kind == FaceKind::router;
    FaceId const id = handle.id;
    ZenohId const zid = handle.zid;
    bool const dialed = handle.dialed;
    faces_[id] = std::move(handle);
    if (!is_router) return;

    // A broker that dialled its own listener: nothing good can come of routing to
    // ourselves, and it would sit in the tables as a permanent duplicate.
    if (zid == router_zid_) {
        auto it = faces_.find(id);
        if (it != faces_.end() && it->second.close) it->second.close();
        return;
    }

    if (collapse_duplicate_link(id, zid, dialed)) return; // this link is the one going

    // Record the peer even before gossip arrives, so this broker can already answer
    // "am I linked to that zid" for the dedup above.
    (void)members_.learn(MemberInfo{.zid = zid, .endpoints = {}});

    // A clique link coming up needs both halves of the state it missed: who else is
    // in the mesh, and what this broker's clients currently declare. Done here
    // rather than by the Face itself so both are ordered against every other routing
    // mutation by the same strand, and so a link that flaps mid-declaration cannot
    // observe a torn view.
    send_gossip_to(id);
    replay_declarations_to(id);
}

auto Tables::local_decl(std::string_view canonical_key) const -> LocalDecl {
    LocalDecl out{};
    for (auto const& fc : resources_.faces_on(canonical_key)) {
        if (is_router_face(fc.face_id)) continue; // clique peers are not "local"
        if (fc.subscriber) ++out.subscribers;
        if (fc.queryable) {
            ++out.queryables;
            out.complete = out.complete || fc.qinfo.complete;
        }
    }
    return out;
}

auto Tables::to_routers(const SharedBuf& msg) -> void {
    if (!msg) return;
    for (auto const& [id, face] : faces_) {
        if (face.kind != FaceKind::router) continue;
        deliver_one(face, msg); // a copy: one refcount bump, the bytes are shared
    }
}

auto Tables::announce_decl_change(std::string_view key, const LocalDecl& before,
                                  const LocalDecl& after) -> void {
    if (before == after) return; // the common case: the Nth client on a shared key

    if (before.subscribers == 0 && after.subscribers != 0) {
        to_routers(encode_declare_subscriber(key));
    } else if (before.subscribers != 0 && after.subscribers == 0) {
        to_routers(encode_undeclare_subscriber(key));
    }

    if (before.queryables != 0 && after.queryables == 0) {
        to_routers(encode_undeclare_queryable(key));
    } else if (after.queryables != 0 &&
               (before.queryables == 0 || before.complete != after.complete)) {
        // Re-announced on a `complete` change as well as on appearance: otherwise a
        // second, complete queryable joining a key first declared by an incomplete
        // one would leave peers filtering `all_complete` queries away from it.
        // `distance = 1`: from the peer's point of view every local queryable of ours
        // sits exactly one broker hop away.
        to_routers(encode_declare_queryable(
            key, QueryableInfo{.complete = after.complete, .distance = 1}));
    }
}

auto Tables::replay_declarations_to(FaceId to) -> void {
    auto it = faces_.find(to);
    if (it == faces_.end()) return;
    for (auto const& key : resources_.declared_keys()) {
        auto const d = local_decl(key);
        if (d.subscribers != 0) deliver_one(it->second, encode_declare_subscriber(key));
        if (d.queryables != 0) {
            deliver_one(it->second, encode_declare_queryable(
                                        key, QueryableInfo{.complete = d.complete, .distance = 1}));
        }
    }
}

auto Tables::remove_face(FaceId id) -> void {
    assert(strand_.running_in_this_thread());
    // Snapshot the clique-visible state of exactly the keys this face touched, before
    // it is stripped, so a client that was the last subscriber on a key withdraws it
    // from every peer broker. Scoped to `keys_for_face` rather than the whole table:
    // no other key's aggregate can possibly change. A departing *peer broker* changes
    // nothing here -- its declarations were never counted as local, and the brokers
    // on the far side detect their own link loss.
    auto const touched = resources_.keys_for_face(id);
    std::vector<LocalDecl> before;
    before.reserve(touched.size());
    for (auto const& key : touched) before.push_back(local_decl(key));

    // If this was a peer link, whatever connector was parked because of it may now
    // take over -- read before the erase, while the handle is still there.
    ZenohId departing_zid{};
    bool departing_router = false;
    if (auto it = faces_.find(id); it != faces_.end() && it->second.kind == FaceKind::router) {
        departing_zid = it->second.zid;
        departing_router = true;
    }

    resources_.remove_face(id);
    faces_.erase(id);
    if (departing_router && router_face_with_zid(departing_zid, 0) == 0) {
        release_dial_suppression(departing_zid);
    }

    for (std::size_t i = 0; i < touched.size(); ++i) {
        announce_decl_change(touched[i], before[i], local_decl(touched[i]));
    }

    // A queryable at `id` vanishing mid-answer: every pending_queries_ entry keyed by
    // this face is treated as if that face had sent ResponseFinal (decrement the
    // origin's fan-in counter, synthesizing the requester's own ResponseFinal at
    // zero). A requester at `id` vanishing: just erase its entries, nothing to
    // forward to (and nothing to notify -- it's gone).
    for (auto it = pending_queries_.begin(); it != pending_queries_.end();) {
        auto const [answering_face, local_rid] = it->first;
        auto const [origin_face, origin_rid] = it->second;
        if (answering_face == id) {
            it = pending_queries_.erase(it);
            auto fit = fanout_remaining_.find({origin_face, origin_rid});
            if (fit != fanout_remaining_.end()) {
                if (--fit->second <= 0) {
                    release_fanout(fit);
                    if (auto oit = faces_.find(origin_face); oit != faces_.end()) {
                        deliver_one(oit->second, encode_response_final(origin_rid, qos_block()),
                                    /*block=*/true);
                    }
                }
            }
        } else if (origin_face == id) {
            it = pending_queries_.erase(it);
        } else {
            ++it;
        }
    }
    // Any fan-out whose *origin* was this face, with no pending_queries_ entry left
    // pointing at it (e.g. it was already at zero), has nothing further to do; erase
    // defensively in case one was left keyed by this now-gone face.
    for (auto it = fanout_remaining_.begin(); it != fanout_remaining_.end();) {
        if (it->first.first == id) {
            it = release_fanout(it);
        } else {
            ++it;
        }
    }
    face_fanouts_.erase(id); // the face is gone; so is its share of the budget
}

// The four declaration handlers share one shape: canonicalize once (so the key used
// for the resource table, the aggregate lookup, and any announcement are literally
// the same string), snapshot what the clique currently believes, mutate, then
// announce the difference. Reading the aggregate straight out of `resources_` on
// both sides -- rather than maintaining a parallel refcount -- means the announced
// state cannot drift from the routing state, and makes an idempotent redeclare a
// no-op for free (before == after).
auto Tables::on_declare_subscriber(FaceId from, std::string_view key) -> void {
    assert(strand_.running_in_this_thread());
    std::string canon(key);
    if (!ke::canonize(canon)) return;
    auto const before = local_decl(canon);
    if (!resources_.declare_subscriber(canon, from)) return;
    announce_decl_change(canon, before, local_decl(canon));
}

auto Tables::on_undeclare_subscriber(FaceId from, std::string_view key) -> void {
    assert(strand_.running_in_this_thread());
    std::string canon(key);
    if (!ke::canonize(canon)) return;
    auto const before = local_decl(canon);
    resources_.undeclare_subscriber(canon, from);
    announce_decl_change(canon, before, local_decl(canon));
}

auto Tables::on_declare_queryable(FaceId from, std::string_view key, QueryableInfo qinfo) -> void {
    assert(strand_.running_in_this_thread());
    std::string canon(key);
    if (!ke::canonize(canon)) return;
    auto const before = local_decl(canon);
    if (!resources_.declare_queryable(canon, from, qinfo)) return;
    announce_decl_change(canon, before, local_decl(canon));
}

auto Tables::on_undeclare_queryable(FaceId from, std::string_view key) -> void {
    assert(strand_.running_in_this_thread());
    std::string canon(key);
    if (!ke::canonize(canon)) return;
    auto const before = local_decl(canon);
    resources_.undeclare_queryable(canon, from);
    announce_decl_change(canon, before, local_decl(canon));
}

auto Tables::queue_delivery(std::size_t& used, FaceId to, MsgSlice slice) -> void {
    // Linear over the faces matched *so far by this batch* -- a handful in practice
    // (one per matching subscriber), so this beats a per-batch hash map outright.
    for (std::size_t i = 0; i < used; ++i) {
        if (deliveries_[i].id == to) {
            deliveries_[i].slices.push_back(slice);
            return;
        }
    }
    if (used == deliveries_.size()) deliveries_.emplace_back();
    auto& slot = deliveries_[used++];
    slot.id = to;
    slot.slices.clear(); // reuses the vector left behind by an earlier batch, if any
    slot.slices.push_back(slice);
}

auto Tables::flush_deliveries(const SharedBuf& block, std::size_t used) -> void {
    for (std::size_t i = 0; i < used; ++i) {
        auto& slot = deliveries_[i];
        if (slot.slices.empty()) continue;
        // Looked up (and found) when the slot was created, and nothing can have
        // erased it since: face removal only happens on this same strand.
        std::size_t const count = slot.slices.size();
        auto fit = faces_.find(slot.id);
        if (fit != faces_.end() && fit->second.deliver) {
            fit->second.deliver(block, std::move(slot.slices));
        }
        // `deliver` took the buffer with it; leave the slot ready, pre-sized for the
        // next batch rather than regrowing from nothing each time.
        slot.slices.clear();
        slot.slices.reserve(count);
    }
}

auto Tables::on_push_batch(FaceId from, const SharedBuf& block, const std::vector<RoutedPush>& msgs)
    -> void {
    assert(strand_.running_in_this_thread());
    // One pass over the whole batch, accumulating per target face, then exactly one
    // `deliver` (and so one Tables->Face strand hop) per face -- the Tier-2->Tier-1
    // mirror of `Face::dispatch_frame_body`'s Tier-1->Tier-2 batching. A publisher
    // sending N Pushes per frame to M subscribers costs M hops, not N*M.
    if (!block) return;
    // Split horizon (docs/CLIQUE.md): a Push that arrived from a peer broker is
    // delivered to local clients only, never re-forwarded to another broker. In a
    // clique every broker already received the origin's copy directly, so
    // re-forwarding would be pure duplicate delivery -- and refusing it outright
    // bounds every message to one inter-broker hop no matter how the mesh is wired.
    // Resolved once per batch rather than per message: the ingress face cannot
    // change mid-batch (this whole call runs on the routing strand).
    bool const from_router = is_router_face(from);
    std::size_t used = 0;
    for (auto const& msg : msgs) {
        if (msg.slice.length == 0 || !is_literal_key(msg.key)) continue;
        const ZenohId* const dest = msg.dest ? &*msg.dest : nullptr; // hoisted zid filter
        for (auto const& fc : resources_.matching_subscribers(msg.key)) {
            if (fc.face_id == from) continue;
            auto fit = faces_.find(fc.face_id);
            if (fit == faces_.end()) continue;
            bool const to_router = fit->second.kind == FaceKind::router;
            if (from_router && to_router) continue; // split horizon
            // A face that is behind still receives anything marked Block; only
            // droppable traffic is discarded for it, and only while it is behind.
            // A saturated face (one that has stopped draining entirely) receives
            // nothing at all -- it is on its way out.
            auto const pressure = face_pressure(fit->second);
            if (pressure == FacePressure::saturated) continue;
            if (pressure == FacePressure::congested && !msg.slice.block) continue;
            // zid filter: narrows, never bypasses -- but it is enforced at the
            // *terminal* hop only. A peer broker is not a delivery, and the targeted
            // zid may well live behind it, so a router face is not filtered here; the
            // broker on the far side applies this same test against its own clients.
            if (dest != nullptr && !to_router && !(fit->second.zid == *dest)) continue;
            queue_delivery(used, fc.face_id, msg.slice);
        }
    }
    flush_deliveries(block, used);
}

auto Tables::on_request(FaceId from, RoutedRequest msg) -> void {
    assert(strand_.running_in_this_thread());
    // Unlike Push, a query key is legitimately a wildcard pattern (e.g. get()'s
    // default "demo/example/**") -- ke::intersects (via matching_queryables) is
    // exactly the general two-pattern intersection this needs, and is documented
    // safe on non-canonical/non-literal input either side.
    auto origin_it = faces_.find(from);
    if (origin_it == faces_.end()) return; // requester already gone

    // Hub topology (no link-state mesh, see the broker plan): best_matching and all
    // both mean "every directly-matching queryable" -- they only diverge with
    // multi-hop routing, which v1 doesn't have. all_complete filters to complete-only.
    bool const from_router = is_router_face(from);
    std::vector<FaceCtx> candidates = resources_.matching_queryables(msg.key);
    if (msg.target == QueryTarget::all_complete) {
        std::erase_if(candidates, [](const FaceCtx& c) { return !c.qinfo.complete; });
    }
    // Split horizon, exactly as on the Push path: a query relayed from a peer broker
    // is answered by local queryables only.
    if (from_router) {
        std::erase_if(candidates, [this](const FaceCtx& c) { return is_router_face(c.face_id); });
    } else if (msg.target == QueryTarget::best_matching) {
        // `best_matching` now has something to choose between: prefer a local
        // queryable, and cross the mesh only when there is none. (Under a single
        // broker every candidate is local, so this is a no-op and behaviour is
        // bit-identical to before federation existed.) `all`/`all_complete` keep
        // reaching local and remote alike.
        bool const any_local =
            std::any_of(candidates.begin(), candidates.end(),
                        [this](const FaceCtx& c) { return !is_router_face(c.face_id); });
        if (any_local) {
            std::erase_if(candidates,
                          [this](const FaceCtx& c) { return is_router_face(c.face_id); });
        }
    }

    QueryKey const origin{from, msg.origin_rid};
    // A request id that is still live for this face means the peer reused it while
    // the earlier fan-out was outstanding. Terminate the new one immediately rather
    // than registering it: `fanout_remaining_` is keyed by exactly this pair, so
    // recording it would overwrite the in-flight entry and strand the original
    // requester waiting for a ResponseFinal that can no longer be synthesized.
    //
    // Reachable in a way it was not before federation: a peer-broker face
    // multiplexes an entire remote broker's requests onto one face id, so the id
    // space behind it is chosen by that broker rather than by a single client.
    if (fanout_remaining_.contains(origin)) {
        deliver_one(origin_it->second, encode_response_final(msg.origin_rid, qos_block()),
                    /*block=*/true);
        return;
    }

    // Refuse to register more in-flight queries than the budget allows -- globally,
    // and per face so one requester cannot consume everyone else's share. Answering
    // immediately with a ResponseFinal terminates the caller's get() with no replies,
    // which is the same thing it would see if no queryable matched; the alternative
    // is a map that grows for as long as a peer keeps asking (see the constants).
    if (fanout_remaining_.size() >= max_pending_fanouts ||
        face_fanouts_[from] >= max_pending_fanouts_per_face) {
        deliver_one(origin_it->second, encode_response_final(msg.origin_rid, qos_block()),
                    /*block=*/true);
        return;
    }

    int fanned_out = 0;
    for (auto const& fc : candidates) {
        auto fit = faces_.find(fc.face_id);
        if (fit == faces_.end()) continue;
        auto const pressure = face_pressure(fit->second);
        if (pressure == FacePressure::saturated) continue;
        if (pressure == FacePressure::congested && !is_block(msg.qos)) continue;
        // zid filter: narrows, never bypasses -- and, as on the Push path, only at
        // the terminal hop; a peer broker may be hiding the targeted client.
        if (msg.dest && fit->second.kind != FaceKind::router && !(fit->second.zid == *msg.dest))
            continue;
        std::uint32_t const local_rid = next_local_rid_++;
        auto enc = encode_request(local_rid, msg.key, msg.parameters, msg.payload, msg.target,
                                  msg.dest, msg.qos);
        if (!enc) continue;
        pending_queries_[{fc.face_id, local_rid}] = origin;
        deliver_one(fit->second, std::move(enc), is_block(msg.qos));
        ++fanned_out;
    }

    if (fanned_out == 0) {
        // Nothing to answer: a get() over zero queryables still has to terminate.
        deliver_one(origin_it->second, encode_response_final(msg.origin_rid, qos_block()),
                    /*block=*/true);
        return;
    }
    fanout_remaining_[origin] = fanned_out;
    ++face_fanouts_[from];
}

auto Tables::on_response(FaceId from, RoutedResponse msg) -> void {
    assert(strand_.running_in_this_thread());
    auto it = pending_queries_.find({from, msg.local_rid});
    if (it == pending_queries_.end()) return; // unknown/already-finalized rid: tolerate
    auto const [origin_face, origin_rid] = it->second;
    auto fit = faces_.find(origin_face);
    if (fit == faces_.end()) return; // requester gone; remove_face already cleaned this up

    deliver_one(fit->second, encode_response(origin_rid, msg.ok, msg.key, msg.payload, msg.qos),
                is_block(msg.qos));
    // Do not erase: more Responses may follow before this answering face's
    // ResponseFinal.
}

auto Tables::release_fanout(std::map<QueryKey, int>::iterator it)
    -> std::map<QueryKey, int>::iterator {
    assert(strand_.running_in_this_thread());
    if (auto fc = face_fanouts_.find(it->first.first); fc != face_fanouts_.end()) {
        if (fc->second <= 1) {
            face_fanouts_.erase(fc);
        } else {
            --fc->second;
        }
    }
    return fanout_remaining_.erase(it);
}

auto Tables::on_response_final(FaceId from, std::uint32_t local_rid) -> void {
    assert(strand_.running_in_this_thread());
    auto it = pending_queries_.find({from, local_rid});
    if (it == pending_queries_.end()) return; // unknown/already-finalized rid: tolerate
    auto const origin = it->second;
    pending_queries_.erase(it);

    auto fit = fanout_remaining_.find(origin);
    if (fit == fanout_remaining_.end()) return;
    if (--fit->second > 0) return;
    release_fanout(fit);

    if (auto oit = faces_.find(origin.first); oit != faces_.end()) {
        deliver_one(oit->second, encode_response_final(origin.second, qos_block()),
                    /*block=*/true);
    }
}

} // namespace zenoh::broker

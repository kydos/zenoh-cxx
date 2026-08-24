module;

#include <asio/strand.hpp>

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <expected>
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
                                  QueryTarget target) -> SharedBuf {
    Request req{};
    req.id = rid;
    req.wire_expr = WireExpr{.scope = 0, .mapping = Mapping::sender, .suffix = key};
    req.target = target;
    Query q{};
    q.parameters = parameters;
    if (payload) q.body = Value{.encoding = Encoding{}, .payload = *payload};
    req.payload = RequestBody{.query = q};

    std::size_t const payload_size = payload ? payload->size() : 0;
    return encode_shared(req, 96 + key.size() + parameters.size() + payload_size);
}

[[nodiscard]] auto encode_response(std::uint32_t rid, bool ok, std::string_view key,
                                   std::span<const std::byte> payload) -> SharedBuf {
    Response rsp{};
    rsp.rid = rid;
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

[[nodiscard]] auto encode_response_final(std::uint32_t rid) -> SharedBuf {
    ResponseFinal rf{};
    rf.rid = rid;
    return encode_shared(rf, 32);
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

auto Tables::deliver_one(const FaceHandle& face, SharedBuf msg) -> void {
    if (!msg || !face.deliver) return;
    std::vector<MsgSlice> one;
    one.reserve(1);
    one.push_back(MsgSlice{.offset = 0, .length = static_cast<std::uint32_t>(msg.size())});
    face.deliver(std::move(msg), std::move(one));
}

auto Tables::add_face(FaceHandle handle) -> void {
    assert(strand_.running_in_this_thread());
    faces_[handle.id] = std::move(handle);
}

auto Tables::remove_face(FaceId id) -> void {
    assert(strand_.running_in_this_thread());
    resources_.remove_face(id);
    faces_.erase(id);

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
                    fanout_remaining_.erase(fit);
                    if (auto oit = faces_.find(origin_face); oit != faces_.end()) {
                        deliver_one(oit->second, encode_response_final(origin_rid));
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
            it = fanout_remaining_.erase(it);
        } else {
            ++it;
        }
    }
}

auto Tables::on_declare_subscriber(FaceId from, std::string_view key) -> void {
    assert(strand_.running_in_this_thread());
    (void)resources_.declare_subscriber(key, from);
}

auto Tables::on_undeclare_subscriber(FaceId from, std::string_view key) -> void {
    assert(strand_.running_in_this_thread());
    resources_.undeclare_subscriber(key, from);
}

auto Tables::on_declare_queryable(FaceId from, std::string_view key, QueryableInfo qinfo) -> void {
    assert(strand_.running_in_this_thread());
    (void)resources_.declare_queryable(key, from, qinfo);
}

auto Tables::on_undeclare_queryable(FaceId from, std::string_view key) -> void {
    assert(strand_.running_in_this_thread());
    resources_.undeclare_queryable(key, from);
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
    std::size_t used = 0;
    for (auto const& msg : msgs) {
        if (msg.slice.length == 0 || !is_literal_key(msg.key)) continue;
        const ZenohId* const dest = msg.dest ? &*msg.dest : nullptr; // hoisted zid filter
        for (auto const& fc : resources_.matching_subscribers(msg.key)) {
            if (fc.face_id == from) continue;
            auto fit = faces_.find(fc.face_id);
            if (fit == faces_.end()) continue;
            if (fit->second.congested && fit->second.congested->load(std::memory_order_relaxed))
                continue; // slow consumer: drop for it, never stall the producer or others
            if (dest != nullptr && !(fit->second.zid == *dest))
                continue; // zid filter: narrows, never bypasses
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
    std::vector<FaceCtx> candidates = resources_.matching_queryables(msg.key);
    if (msg.target == QueryTarget::all_complete) {
        std::erase_if(candidates, [](const FaceCtx& c) { return !c.qinfo.complete; });
    }

    QueryKey const origin{from, msg.origin_rid};
    int fanned_out = 0;
    for (auto const& fc : candidates) {
        auto fit = faces_.find(fc.face_id);
        if (fit == faces_.end()) continue;
        if (fit->second.congested && fit->second.congested->load(std::memory_order_relaxed))
            continue; // slow consumer: drop for it, never stall the requester or others
        if (msg.dest && !(fit->second.zid == *msg.dest))
            continue; // zid filter: narrows, never bypasses
        std::uint32_t const local_rid = next_local_rid_++;
        auto enc = encode_request(local_rid, msg.key, msg.parameters, msg.payload, msg.target);
        if (!enc) continue;
        pending_queries_[{fc.face_id, local_rid}] = origin;
        deliver_one(fit->second, std::move(enc));
        ++fanned_out;
    }

    if (fanned_out == 0) {
        // Nothing to answer: a get() over zero queryables still has to terminate.
        deliver_one(origin_it->second, encode_response_final(msg.origin_rid));
        return;
    }
    fanout_remaining_[origin] = fanned_out;
}

auto Tables::on_response(FaceId from, RoutedResponse msg) -> void {
    assert(strand_.running_in_this_thread());
    auto it = pending_queries_.find({from, msg.local_rid});
    if (it == pending_queries_.end()) return; // unknown/already-finalized rid: tolerate
    auto const [origin_face, origin_rid] = it->second;
    auto fit = faces_.find(origin_face);
    if (fit == faces_.end()) return; // requester gone; remove_face already cleaned this up

    deliver_one(fit->second, encode_response(origin_rid, msg.ok, msg.key, msg.payload));
    // Do not erase: more Responses may follow before this answering face's
    // ResponseFinal.
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
    fanout_remaining_.erase(fit);

    if (auto oit = faces_.find(origin.first); oit != faces_.end()) {
        deliver_one(oit->second, encode_response_final(origin.second));
    }
}

} // namespace zenoh::broker

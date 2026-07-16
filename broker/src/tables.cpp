module;

#include <asio/strand.hpp>

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
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

// Encode one Push (Put or Del) network message. Returns nullopt only on a pathological
// encode failure (buffer sized generously up front, so this should not happen in
// practice); callers skip delivery to that target rather than propagating an error
// that would fault the whole routing step for every other target.
[[nodiscard]] auto encode_push(std::string_view key, std::span<const std::byte> payload,
                               bool is_del) -> std::optional<std::vector<std::byte>> {
    Push push{};
    push.wire_expr = WireExpr{.scope = 0, .mapping = Mapping::sender, .suffix = key};
    if (is_del) {
        push.payload = PushBody{.body = Del{}};
    } else {
        Put put{};
        put.payload = payload;
        push.payload = PushBody{.body = put};
    }
    std::vector<std::byte> buf(96 + key.size() + payload.size());
    ByteWriter w{buf};
    if (!push.encode(w)) return std::nullopt;
    buf.resize(w.written());
    return buf;
}

[[nodiscard]] auto encode_request(std::uint32_t rid, std::string_view key,
                                  std::string_view parameters,
                                  const std::optional<std::vector<std::byte>>& payload,
                                  QueryTarget target) -> std::optional<std::vector<std::byte>> {
    Request req{};
    req.id = rid;
    req.wire_expr = WireExpr{.scope = 0, .mapping = Mapping::sender, .suffix = key};
    req.target = target;
    Query q{};
    q.parameters = parameters;
    if (payload) q.body = Value{.encoding = Encoding{}, .payload = *payload};
    req.payload = RequestBody{.query = q};

    std::size_t const payload_size = payload ? payload->size() : 0;
    std::vector<std::byte> buf(96 + key.size() + parameters.size() + payload_size);
    ByteWriter w{buf};
    if (!req.encode(w)) return std::nullopt;
    buf.resize(w.written());
    return buf;
}

[[nodiscard]] auto encode_response(std::uint32_t rid, bool ok, std::string_view key,
                                   std::span<const std::byte> payload)
    -> std::optional<std::vector<std::byte>> {
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
    std::vector<std::byte> buf(96 + key.size() + payload.size());
    ByteWriter w{buf};
    if (!rsp.encode(w)) return std::nullopt;
    buf.resize(w.written());
    return buf;
}

[[nodiscard]] auto encode_response_final(std::uint32_t rid)
    -> std::optional<std::vector<std::byte>> {
    ResponseFinal rf{};
    rf.rid = rid;
    std::vector<std::byte> buf(32);
    ByteWriter w{buf};
    if (!rf.encode(w)) return std::nullopt;
    buf.resize(w.written());
    return buf;
}

} // namespace

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
                        if (auto enc = encode_response_final(origin_rid)) {
                            oit->second.deliver(std::move(*enc));
                        }
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

auto Tables::route_push(FaceId from, const RoutedPush& msg) -> void {
    if (!is_literal_key(msg.key)) return;

    for (auto const& fc : resources_.matching_subscribers(msg.key)) {
        if (fc.face_id == from) continue;
        auto fit = faces_.find(fc.face_id);
        if (fit == faces_.end()) continue;
        if (fit->second.congested && fit->second.congested->load(std::memory_order_relaxed))
            continue; // slow consumer: drop for it, never stall the producer or others
        if (msg.dest && !(fit->second.zid == *msg.dest))
            continue; // zid filter: narrows, never bypasses
        if (auto enc = encode_push(msg.key, msg.payload, msg.is_del))
            fit->second.deliver(std::move(*enc));
    }
}

auto Tables::on_push_batch(FaceId from, const std::vector<RoutedPush>& msgs) -> void {
    assert(strand_.running_in_this_thread());
    for (auto const& msg : msgs) route_push(from, msg);
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
        fit->second.deliver(std::move(*enc));
        ++fanned_out;
    }

    if (fanned_out == 0) {
        // Nothing to answer: a get() over zero queryables still has to terminate.
        if (auto enc = encode_response_final(msg.origin_rid))
            origin_it->second.deliver(std::move(*enc));
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

    if (auto enc = encode_response(origin_rid, msg.ok, msg.key, msg.payload))
        fit->second.deliver(std::move(*enc));
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
        if (auto enc = encode_response_final(origin.second)) oit->second.deliver(std::move(*enc));
    }
}

} // namespace zenoh::broker

module;

#include <algorithm>
#include <cstddef>
#include <string>
#include <string_view>
#include <vector>

module zenoh.broker.resource;

// Implementation unit for zenoh.broker.resource.
namespace zenoh::broker {

namespace {

// Find `face_id`'s FaceCtx in `faces`, or nullptr.
[[nodiscard]] auto find_face(std::vector<FaceCtx>& faces, FaceId face_id) noexcept -> FaceCtx* {
    for (auto& fc : faces) {
        if (fc.face_id == face_id) return &fc;
    }
    return nullptr;
}

} // namespace

auto ResourceTable::declare_subscriber(std::string_view keyexpr, FaceId face_id) -> bool {
    std::string canon(keyexpr);
    if (!zenoh::ke::canonize(canon)) return false;
    auto& res = by_key_[canon];
    res.keyexpr = canon;
    if (auto* fc = find_face(res.faces, face_id)) {
        fc->subscriber = true;
    } else {
        res.faces.push_back(FaceCtx{.face_id = face_id, .subscriber = true});
    }
    return true;
}

auto ResourceTable::declare_queryable(std::string_view keyexpr, FaceId face_id, QueryableInfo qinfo)
    -> bool {
    std::string canon(keyexpr);
    if (!zenoh::ke::canonize(canon)) return false;
    auto& res = by_key_[canon];
    res.keyexpr = canon;
    if (auto* fc = find_face(res.faces, face_id)) {
        fc->queryable = true;
        fc->qinfo = qinfo;
    } else {
        res.faces.push_back(FaceCtx{.face_id = face_id, .queryable = true, .qinfo = qinfo});
    }
    return true;
}

auto ResourceTable::undeclare_subscriber(std::string_view keyexpr, FaceId face_id) -> void {
    std::string canon(keyexpr);
    if (!zenoh::ke::canonize(canon)) return;
    auto it = by_key_.find(canon);
    if (it == by_key_.end()) return;
    auto* fc = find_face(it->second.faces, face_id);
    if (fc == nullptr) return;
    fc->subscriber = false;
    if (!fc->subscriber && !fc->queryable) {
        std::erase_if(it->second.faces,
                      [face_id](const FaceCtx& c) { return c.face_id == face_id; });
    }
    if (it->second.faces.empty()) by_key_.erase(it);
}

auto ResourceTable::undeclare_queryable(std::string_view keyexpr, FaceId face_id) -> void {
    std::string canon(keyexpr);
    if (!zenoh::ke::canonize(canon)) return;
    auto it = by_key_.find(canon);
    if (it == by_key_.end()) return;
    auto* fc = find_face(it->second.faces, face_id);
    if (fc == nullptr) return;
    fc->queryable = false;
    if (!fc->subscriber && !fc->queryable) {
        std::erase_if(it->second.faces,
                      [face_id](const FaceCtx& c) { return c.face_id == face_id; });
    }
    if (it->second.faces.empty()) by_key_.erase(it);
}

auto ResourceTable::face_count_for(std::string_view canonical_key) const -> std::size_t {
    auto it = by_key_.find(canonical_key); // heterogeneous lookup, no temporary string
    return it == by_key_.end() ? 0 : it->second.faces.size();
}

auto ResourceTable::remove_face(FaceId face_id) -> void {
    for (auto it = by_key_.begin(); it != by_key_.end();) {
        std::erase_if(it->second.faces,
                      [face_id](const FaceCtx& c) { return c.face_id == face_id; });
        if (it->second.faces.empty()) {
            it = by_key_.erase(it);
        } else {
            ++it;
        }
    }
}

auto ResourceTable::matching_subscribers(std::string_view literal_key) const
    -> std::vector<FaceCtx> {
    std::vector<FaceCtx> out;
    // Fast path: an exact hash lookup handles the common case (a subscription
    // declared on precisely the literal key being published) without ever
    // calling ke::intersects, whose chunk-splitting + DP rows cost several heap
    // allocations even for a trivial two-chunk key. The general scan below still
    // covers every *other* declared resource (wildcarded ones, and any literal
    // one that happens not to match), explicitly skipping the entry already
    // handled here rather than double-counting it.
    auto const exact = by_key_.find(literal_key);
    if (exact != by_key_.end()) {
        for (auto const& fc : exact->second.faces) {
            if (fc.subscriber) out.push_back(fc);
        }
    }
    for (auto const& [key, res] : by_key_) {
        if (exact != by_key_.end() && key == exact->first) continue; // already handled above
        if (!zenoh::ke::intersects(literal_key, res.keyexpr)) continue;
        for (auto const& fc : res.faces) {
            if (!fc.subscriber) continue;
            // A face with two+ overlapping declared subscriptions must still be
            // delivered to exactly once per message (mirrors the reference router's
            // route dedup by destination face id, not by matched resource).
            if (find_face(out, fc.face_id) != nullptr) continue;
            out.push_back(fc);
        }
    }
    return out;
}

auto ResourceTable::matching_queryables(std::string_view key) const -> std::vector<FaceCtx> {
    std::vector<FaceCtx> out;
    // Fast path: see matching_subscribers. Safe here too even though `key` may
    // itself be a pattern (e.g. get()'s "demo/example/**") -- an exact hash hit
    // only ever fires when a queryable happens to be declared on that identical
    // string, which is still a correct match (a pattern intersects itself).
    auto const exact = by_key_.find(key);
    if (exact != by_key_.end()) {
        for (auto const& fc : exact->second.faces) {
            if (fc.queryable) out.push_back(fc);
        }
    }
    for (auto const& [declared_key, res] : by_key_) {
        if (exact != by_key_.end() && declared_key == exact->first) continue;
        if (!zenoh::ke::intersects(key, res.keyexpr)) continue;
        for (auto const& fc : res.faces) {
            if (!fc.queryable) continue;
            if (find_face(out, fc.face_id) != nullptr) continue; // dedup by face, see above
            out.push_back(fc);
        }
    }
    return out;
}

} // namespace zenoh::broker

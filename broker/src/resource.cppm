module;

#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

export module zenoh.broker.resource;

import zenoh.proto; // QueryableInfo
import zenoh.ke;

// The broker's resource table: which faces have declared a Subscriber/Queryable on
// which key-expression pattern, and matching a literal (wildcard-free) publish/query
// key against those patterns. A deliberate simplification vs. the reference
// implementation's prefix trie + precomputed cross-resource `matches` list (see the
// broker plan's M5 section) — v1 hub-broker scale (declaration count, not per-message
// key cardinality) doesn't need trie-sharing for correctness, only for very-large-N
// lookup speed. Pure, synchronous logic: thread-safety is the caller's job (in
// practice, `Tables` only ever touches this from its own routing strand).
namespace zenoh::broker {

// Transparent hash for `by_key_` so a `std::string_view` (a published/queried key,
// already owned by the caller) can `find()` the map directly instead of
// constructing a temporary `std::string` just to probe it -- part of the exact-
// literal-match fast path in `matching_subscribers`/`matching_queryables` (see
// their doc comments): a hash lookup answers "is there a declaration on exactly
// this key" without ever calling `zenoh.ke::intersects`, which -- even for a
// trivial two-chunk key -- costs several heap allocations (chunk-splitting both
// sides plus the DP rows). `std::equal_to<>` (transparent) is the matching
// comparator, set as `by_key_`'s `KeyEqual` template argument.
struct TransparentStringHash {
    using is_transparent = void;
    [[nodiscard]] auto operator()(std::string_view sv) const noexcept -> std::size_t {
        return std::hash<std::string_view>{}(sv);
    }
};

} // namespace zenoh::broker

export namespace zenoh::broker {

/// A face's identity within the broker: a monotonic id assigned at accept time,
/// never reused. Chosen over a raw pointer/vector-index so stored references (in the
/// resource table, in pending-query bookkeeping) never dangle or alias a reconnected
/// peer's slot.
using FaceId = std::uint64_t;

/// One face's declaration state on a single resource (see `Resource`).
struct FaceCtx {
    FaceId face_id = 0;
    bool subscriber = false;
    bool queryable = false;
    QueryableInfo qinfo{}; ///< meaningful only when `queryable`
};

/// One declared key-expression pattern (in canonical form, see `zenoh.ke::canonize`)
/// and every face that has a live Subscriber and/or Queryable declaration on it.
struct Resource {
    std::string keyexpr;
    std::vector<FaceCtx> faces;
};

/// The broker's declaration table: `declare_*`/`undeclare_*` maintain it as
/// Declare/Undeclare messages arrive; `matching_*` answers "which faces care about
/// this literal key" for Push/Request routing (`Tables::on_push_batch`/`on_request`).
class ResourceTable {
  public:
    /// Record `face_id` as a subscriber on `keyexpr`. Returns false (no-op) if
    /// `keyexpr` fails to canonicalize (malformed pattern from an untrusted peer).
    [[nodiscard]] auto declare_subscriber(std::string_view keyexpr, FaceId face_id) -> bool;
    /// Record `face_id` as a queryable (with `qinfo`) on `keyexpr`. Same malformed-
    /// pattern handling as `declare_subscriber`.
    [[nodiscard]] auto declare_queryable(std::string_view keyexpr, FaceId face_id,
                                         QueryableInfo qinfo) -> bool;
    /// Remove `face_id`'s subscriber declaration on `keyexpr`, if any. Silently a
    /// no-op if absent (matches this project's "tolerate the unknown, fault only on
    /// real desync" idiom — see e.g. `Session::resolve_key`).
    auto undeclare_subscriber(std::string_view keyexpr, FaceId face_id) -> void;
    /// Remove `face_id`'s queryable declaration on `keyexpr`, if any.
    auto undeclare_queryable(std::string_view keyexpr, FaceId face_id) -> void;
    /// Strip `face_id` from every resource (subscriber and queryable declarations
    /// alike) and drop any resource left with no faces. Called once when a face
    /// disconnects — O(declared resource count), acceptable at v1 hub scale.
    auto remove_face(FaceId face_id) -> void;

    /// Every `FaceCtx` with `subscriber == true` on a resource whose pattern
    /// intersects `literal_key`, deduplicated by `face_id` (a face with two+
    /// overlapping declared subscriptions on the same key is still returned once).
    /// `literal_key` must be wildcard-free — a Push/Del key is always literal, and
    /// `Tables::route_push` rejects one containing `*` before calling this. Takes an
    /// exact-hash-lookup fast path first (an O(1) `by_key_.find`, skipping
    /// `zenoh.ke::intersects` entirely) before falling back to the general
    /// wildcard scan for every other declared resource — the common case (a
    /// publish landing on a subscription declared on that exact literal key) never
    /// pays `intersects`'s chunk-splitting/DP cost at all.
    [[nodiscard]] auto matching_subscribers(std::string_view literal_key) const
        -> std::vector<FaceCtx>;
    /// Every `FaceCtx` with `queryable == true` on a resource whose pattern
    /// intersects `key`, deduplicated by `face_id` (see `matching_subscribers`).
    /// Unlike `matching_subscribers`, `key` need not be wildcard-free: a query key
    /// expression is legitimately a pattern (e.g. `get()`'s own default shape,
    /// `"demo/example/**"`) — `zenoh.ke::intersects` is documented safe on
    /// non-canonical/wildcarded input on either side, so `Tables::on_request` does
    /// not reject one the way `route_push` rejects a wildcarded publish key.
    [[nodiscard]] auto matching_queryables(std::string_view key) const -> std::vector<FaceCtx>;

    /// Number of distinct declared (canonical) patterns. Test-only introspection.
    [[nodiscard]] auto resource_count() const noexcept -> std::size_t { return by_key_.size(); }

    /// Number of faces (subscriber or queryable) registered on the exact resource
    /// keyed by `canonical_key` (0 if no such resource exists). Test-only
    /// introspection: lets a test await "N sessions have all declared the same
    /// subscription/queryable" deterministically instead of via a fixed sleep —
    /// `resource_count()` alone can't distinguish "1 of N faces registered" from
    /// "all N registered" when every face declares the identical pattern.
    [[nodiscard]] auto face_count_for(std::string_view canonical_key) const -> std::size_t;

  private:
    std::unordered_map<std::string, Resource, TransparentStringHash, std::equal_to<>> by_key_;
};

} // namespace zenoh::broker

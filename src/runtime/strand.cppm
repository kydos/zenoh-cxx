module;

#include <cstddef>
#include <cstdint>
#include <functional>
#include <list>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>

export module zenoh.runtime.strand;

// A per-subscriber strand (SUBSCRIBER.md): a bounded queue that serializes a single
// subscriber's samples. Templated on the value type so it unit-tests in isolation,
// independent of the proto/Sample types. Two modes:
//
//   ordered    — bounded FIFO; when full, `post` reports `full` and the dispatcher
//                applies backpressure (it never silently drops).
//   last_value — under backpressure, conflates by key: a new value whose key is
//                already pending overwrites that key's *most-recent* node and re-tails
//                it, so delivery stays an ordered subsequence of the arrival stream
//                (same order, some intermediate same-key values dropped). When full and
//                the key is absent, reports `full` (nothing to conflate).
//
// The structure is guarded by a mutex: the producer (decode thread) and the consumer
// (a pump thread) are genuinely different threads (MPSC). The `latest_` index is only
// built/maintained in `last_value` mode, so `ordered` strands carry no extra cost.
export namespace zenoh {

/// Delivery discipline for a subscriber's strand.
enum class StrandMode : std::uint8_t {
    ordered,    ///< bounded FIFO, backpressure on full
    last_value, ///< conflate by key under backpressure
};

/// Outcome of `Strand::post`.
enum class PostResult : std::uint8_t {
    posted,    ///< appended at the tail
    conflated, ///< (last_value) replaced an existing key's value and re-tailed it
    full,      ///< no room and nothing to conflate — caller must drain then retry
};

/// A bounded, optionally key-conflating FIFO for one subscriber. Move-only.
template <class T> class Strand {
public:
    /// Build a strand holding at most `capacity` entries (clamped to ≥1) with the given
    /// delivery `mode`.
    explicit Strand(std::size_t capacity, StrandMode mode = StrandMode::ordered)
        : capacity_(capacity == 0 ? 1 : capacity), mode_(mode) {}

    Strand(const Strand&) = delete;
    auto operator=(const Strand&) -> Strand& = delete;
    Strand(Strand&&) = delete; // pinned: the `latest_` iterators alias `list_` nodes
    auto operator=(Strand&&) -> Strand& = delete;

    /// Enqueue `value` under `key`. Appends at the tail when there is room. When full:
    /// in `last_value` mode, if `key` is already pending its most-recent node is
    /// overwritten and moved to the tail (`conflated`); otherwise reports `full`.
    [[nodiscard]] auto post(std::string_view key, T value) -> PostResult {
        std::lock_guard lk(mu_);
        if (list_.size() < capacity_) {
            list_.push_back(Node{std::move(value), std::string{}});
            if (mode_ == StrandMode::last_value) {
                auto it = std::prev(list_.end());
                it->key.assign(key);
                latest_[it->key] = it; // most-recent node for this key
            }
            return PostResult::posted;
        }
        if (mode_ == StrandMode::last_value) {
            if (auto m = latest_.find(key); m != latest_.end()) { // never operator[]
                auto node = m->second;
                node->value = std::move(value);
                list_.splice(list_.end(), list_, node); // re-tail: newest position
                return PostResult::conflated;
            }
        }
        return PostResult::full;
    }

    /// Pop and return the head value (FIFO), or `nullopt` if empty.
    [[nodiscard]] auto pop() -> std::optional<T> {
        std::lock_guard lk(mu_);
        if (list_.empty()) return std::nullopt;
        auto it = list_.begin();
        if (mode_ == StrandMode::last_value) {
            // Drop the index entry only if this node is still the key's most-recent.
            if (auto m = latest_.find(it->key); m != latest_.end() && m->second == it)
                latest_.erase(m);
        }
        T value = std::move(it->value);
        list_.pop_front();
        return value;
    }

    [[nodiscard]] auto size() const -> std::size_t {
        std::lock_guard lk(mu_);
        return list_.size();
    }
    [[nodiscard]] auto empty() const -> bool {
        std::lock_guard lk(mu_);
        return list_.empty();
    }
    [[nodiscard]] auto capacity() const noexcept -> std::size_t { return capacity_; }
    [[nodiscard]] auto mode() const noexcept -> StrandMode { return mode_; }

private:
    struct Node {
        T value;
        std::string key; ///< populated only in last_value mode (else empty, no alloc)
    };
    using List = std::list<Node>;

    // Transparent hashing so `find(string_view)` matches owned `std::string` keys with
    // no temporary allocation on the conflation/pop paths.
    struct StringHash {
        using is_transparent = void;
        auto operator()(std::string_view s) const noexcept -> std::size_t {
            return std::hash<std::string_view>{}(s);
        }
    };

    mutable std::mutex mu_;
    List list_;
    // key → most-recent node, last_value mode only. Keyed by an owned string so the
    // index outlives the nodes it points at; one entry per distinct pending key.
    std::unordered_map<std::string, typename List::iterator, StringHash, std::equal_to<>> latest_;
    std::size_t capacity_;
    StrandMode mode_;
};

} // namespace zenoh

module;

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

export module zenoh.broker.membership;

export import zenoh.proto; // ZenohId appears in this module's exported API

// Clique membership: who else is in the mesh, where they can be dialled, and the
// rules that keep a set of brokers converging on a full mesh without a central
// registry. See docs/CLIQUE.md.
//
// Deliberately pure -- no ASIO, no sockets, no strand. It knows nothing about faces
// or connections; it is a value table plus three decisions (learn / dial / which
// duplicate link survives), which is what makes it unit-testable on its own the same
// way `zenoh.broker.resource` is. Everything that has to actually open a socket
// lives in broker.cpp.
export namespace zenoh::broker {

/// A broker in the clique and the endpoints it can be reached on.
struct MemberInfo {
    ZenohId zid{};
    /// Advertised, dialable endpoints (`tcp/host:port`). Empty for a broker that
    /// listens on a wildcard address and was given no explicit `--advertise`: it can
    /// still dial out and take part, it just cannot be dialled back.
    std::vector<std::string> endpoints{};
    auto operator==(const MemberInfo&) const -> bool = default;
};

/// The reserved key expression gossip travels on, as an ordinary `Push`. Using a
/// normal message rather than a new one keeps the wire format untouched and lets
/// gossip ride the existing batching and framing; the key is intercepted on the
/// receiving `Face` and never reaches the routing tables, so it can neither be
/// subscribed to nor fanned out to a client.
inline constexpr std::string_view gossip_key = "@/router/gossip";

/// Whether `key` is broker-internal and must never be routed. Checked on every
/// inbound Push, so that a *client* cannot inject or observe clique control traffic
/// by publishing or subscribing under this prefix.
[[nodiscard]] auto is_internal_key(std::string_view key) noexcept -> bool;

/// Total order on Zenoh ids, used only to break dial ties (below). Any consistent
/// order works; this one compares length first, then the significant bytes, so it is
/// stable regardless of the zero padding `ZenohId` carries.
[[nodiscard]] auto zid_less(const ZenohId& a, const ZenohId& b) noexcept -> bool;

/// Which of two simultaneous links between the same pair of brokers survives.
///
/// Gossip makes mutual dialling the normal case rather than a rare race: both ends
/// learn about each other at nearly the same instant and both dial. Deciding by
/// arrival order would let each end keep a different link -- or drop both -- so the
/// rule is a pure function of the two identities: the link *dialled by the lower
/// zid* is the one that survives. Each end evaluates it with its own zid first and
/// necessarily reaches the complementary answer, so exactly one link is closed.
///
/// Returns true if *our own outbound* link to `peer` is the survivor.
[[nodiscard]] auto keep_outbound_link(const ZenohId& self, const ZenohId& peer) noexcept -> bool;

/// Whether `endpoint` is worth dialling: syntactically a `[tcp/]host:port` with a
/// non-zero port. A bad entry is dropped rather than retried forever, so one
/// misconfigured broker cannot make the whole clique spin on it.
[[nodiscard]] auto is_dialable_endpoint(std::string_view endpoint) noexcept -> bool;

/// The set of brokers this one knows about, including itself.
///
/// Deliberately has no removal operation. Gossiping departures is where this class
/// of protocol usually goes wrong -- a departure racing a re-announcement resurrects
/// the member, and a partition makes both halves declare the other dead. Instead a
/// member, once learned, is kept and re-dialled forever with capped backoff. That is
/// bounded in practice because a clique is tens of brokers, and it means a broker
/// that restarts, moves, or is briefly unreachable rejoins with no operator action.
class Membership {
  public:
    /// `self_endpoints` is what other brokers will be told to dial us on.
    Membership() = default;

    /// Set (once, at startup) this broker's own identity and advertised endpoints.
    auto set_self(ZenohId zid, std::vector<std::string> endpoints) -> void;
    [[nodiscard]] auto self() const noexcept -> const MemberInfo& { return self_; }

    /// Record `m`, merging its endpoints into any entry already held. Returns true
    /// if this actually taught us something -- a new broker, or a new endpoint for a
    /// known one -- which is the signal to re-advertise and to dial.
    /// Self-announcements and members with an unusable zid are ignored.
    auto learn(const MemberInfo& m) -> bool;

    /// Everything known, self included -- the payload sent on a new link and
    /// whenever the view changes.
    [[nodiscard]] auto snapshot() const -> std::vector<MemberInfo>;

    /// Endpoints of `zid`, or empty if unknown.
    [[nodiscard]] auto endpoints_of(const ZenohId& zid) const -> std::vector<std::string>;

    /// Number of *other* brokers known.
    [[nodiscard]] auto peer_count() const noexcept -> std::size_t { return peers_.size(); }

    /// Cap on how many brokers may be tracked. A clique is small; the bound is what
    /// stops a misconfigured or hostile peer from growing this table without limit
    /// via gossip. At the cap, further unknown members are ignored rather than
    /// evicting a live one.
    static constexpr std::size_t max_members = 1024;
    /// Cap on advertised endpoints per member, same rationale.
    static constexpr std::size_t max_endpoints_per_member = 8;

    /// Encode `members` as a gossip payload. Self-describing and length-prefixed
    /// throughout, so a peer running a different build can reject it cleanly rather
    /// than misparse it.
    [[nodiscard]] static auto encode(const std::vector<MemberInfo>& members)
        -> std::vector<std::byte>;
    /// Inverse of `encode`. Nullopt on any malformed or over-long input -- this is
    /// untrusted data off a socket, so every count and length is bounds-checked
    /// against the caps above before anything is allocated for it.
    [[nodiscard]] static auto decode(std::span<const std::byte> payload)
        -> std::optional<std::vector<MemberInfo>>;

  private:
    MemberInfo self_{};
    std::vector<MemberInfo> peers_{};
};

} // namespace zenoh::broker

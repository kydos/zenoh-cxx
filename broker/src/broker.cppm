module;

#include <cstdint>
#include <expected>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

export module zenoh.broker;

export import zenoh.broker.tables; // Tables is a public member (tests introspect it)

// The broker's top-level reactor: one `asio::io_context`, a configurable-size worker
// thread pool, and the listening acceptor. See the broker plan's "Concurrency and
// thread safety" section: I/O (accept, per-Face read/write) is genuinely parallel
// across the thread pool; all cross-face routing is serialized on `tables.strand()`
// (Tier 2).
//
// IMPORTANT: this file's global module fragment must never include
// `<asio/io_context.hpp>`, `<asio/ip/tcp.hpp>`, or `<asio/awaitable.hpp>` (nor use
// any type from them, even in a private, non-exported declaration): doing so was
// found to poison this module's BMI for *any* importer that also textually includes
// a handful of ordinary standard headers (`<string>` alone is enough), triggering a
// clang/libc++ "cannot add 'abi_tag' attribute in a redeclaration" error -- the same
// class of named-modules/libc++ fragility this project's docs already flag
// elsewhere (PLAN.md's clang+libc++/gcc+libstdc++ toolchain notes), just via a
// different trigger (confirmed empirically: the identical headers compile cleanly
// outside of a module, and `zenoh.broker.tables`, whose GMF only needs
// `<asio/strand.hpp>`/`<asio/any_io_executor.hpp>`, does not exhibit it). The fix is
// PIMPL: every asio type `Broker` needs (`io_context`, `acceptor`) lives behind an
// opaque `Impl`, fully defined only in broker.cpp, which -- being an implementation
// unit, not the primary interface unit -- does not contribute to the importable BMI.
export namespace zenoh::broker {

/// Why `Broker::bind` failed.
enum class BindError : std::uint8_t { bad_address, bind_failed };

/// How to bring a broker up, including its place in a clique (`docs/CLIQUE.md`).
/// Plain value types only -- no ASIO appears in this interface unit, by necessity
/// (see the module comment above).
struct BrokerConfig {
    /// Address to listen on for both client sessions and peer brokers; `0.0.0.0`
    /// accepts on every interface. `port == 0` picks an ephemeral port.
    std::string listen_host = "0.0.0.0";
    std::uint16_t listen_port = 7447;
    /// Seed peers to dial, as `tcp/host:port` (the `tcp/` prefix is optional).
    /// Each is dialled on startup and re-dialled forever with capped backoff if the
    /// link drops -- a peer that is not up yet is not an error. Both ends may name
    /// each other; the duplicate link that produces is collapsed deterministically.
    std::vector<std::string> peers{};
    /// The endpoint other brokers should be told to dial this one on, gossiped
    /// around the clique. Required when `listen_host` is a wildcard (`0.0.0.0`,
    /// `::`), since that is not something a peer can connect to; otherwise it
    /// defaults to `listen_host` plus the bound port. A broker with no advertisable
    /// endpoint still takes part -- it dials out and routes normally -- it simply
    /// cannot be dialled back, so it must be able to reach its peers itself.
    std::optional<std::string> advertise{};
};

class Broker {
  public:
    Broker(const Broker&) = delete;
    auto operator=(const Broker&) -> Broker& = delete;
    Broker(Broker&&) = delete; // Impl's io_context has a stable address for *this's lifetime
    auto operator=(Broker&&) -> Broker& = delete;
    ~Broker();

    /// Bind and listen on `host:port` (`port == 0` picks an ephemeral port --
    /// `port()` resolves it, used by in-process broker tests exactly like the
    /// client-side test routers bind port 0 today). Generates a fresh random router
    /// Zenoh id (same `std::random_device` idiom as `Session::open`).
    [[nodiscard]] static auto bind(std::string_view host, std::uint16_t port)
        -> std::expected<std::unique_ptr<Broker>, BindError>;

    /// Bind per `cfg`, additionally dialling every peer in `cfg.peers` once `run()`
    /// starts (the connectors need the `io_context` to be running, so `bind` only
    /// records them). Equivalent to the two-argument overload when `peers` is empty.
    [[nodiscard]] static auto bind(BrokerConfig cfg)
        -> std::expected<std::unique_ptr<Broker>, BindError>;

    /// The bound local port (resolves an ephemeral `port=0` bind).
    [[nodiscard]] auto port() const noexcept -> std::uint16_t;

    /// Spawn `num_threads` (clamped to >= 1) worker threads running the io_context
    /// (this call's own thread is one of them) and the accept loop, blocking until
    /// `stop()` is called from another thread. `num_threads == 1` is what
    /// deterministic tests use.
    auto run(unsigned num_threads) -> void;

    /// Stop accepting and ask every worker thread's `io_context::run()` to return
    /// promptly. Not a graceful drain (in-flight connections are simply abandoned
    /// once no thread services them) -- adequate for v1, see docs/BROKER.md.
    auto stop() -> void;

    // Declaration order matters from here down: members initialize in declaration
    // order regardless of access-specifier interleaving, and `tables`'s strand is
    // built from the io_context inside `impl_`, so `impl_` must be declared (and
    // thus constructed) first.
  private:
    struct Impl; // opaque: io_context + acceptor, defined only in broker.cpp
    std::unique_ptr<Impl> impl_;

  public:
    /// Global routing state, exposed so tests can introspect it (via
    /// `tables.strand()`-marshaled accessors) and so the per-connection accept/read
    /// coroutines (broker.cpp) have something to route into.
    Tables tables;

  private:
    explicit Broker(ZenohId router_zid);
    [[nodiscard]] auto do_bind(std::string_view host, std::uint16_t port) -> bool;
};

} // namespace zenoh::broker

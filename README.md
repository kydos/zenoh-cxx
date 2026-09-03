# zenoh-cxx

[![CI](https://github.com/kydos/zenoh-cxx/actions/workflows/ci.yml/badge.svg)](https://github.com/kydos/zenoh-cxx/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/kydos/zenoh-cxx/branch/main/graph/badge.svg)](https://codecov.io/gh/kydos/zenoh-cxx)
[![License: AGPL v3](https://img.shields.io/badge/License-AGPL_v3-blue.svg)](https://www.gnu.org/licenses/agpl-3.0)
[![C++23](https://img.shields.io/badge/C%2B%2B-23-blue.svg)](https://en.cppreference.com/w/cpp/23)
[![code style: clang-format](https://img.shields.io/badge/code%20style-clang--format-blue.svg)](.clang-format)

A modern C++23 implementation of the Zenoh protocol (wire version 9), written with
named modules and interoperable with the [Rust reference
implementation](https://github.com/eclipse-zenoh/zenoh) on the wire.

Three static libraries, built from one CMake project:

- **`zenoh-proto`** — the pure, I/O-free wire codec plus key-expression matching
  (`zenoh.ke`). No sockets, no allocation surprises; messages decode as borrow-only
  views into the receive buffer. See [`docs/PROTO.md`](docs/PROTO.md).
- **`zenoh`** — the client runtime (`Session`): connects to a router, put/get,
  subscribe/queryable. See [`docs/RUNTIME.md`](docs/RUNTIME.md).
- **`zenoh-broker`** — `zenohb`, a multithreaded ASIO router that also **federates
  into a clique** with other brokers. See [`docs/BROKER.md`](docs/BROKER.md) and
  [`docs/CLIQUE.md`](docs/CLIQUE.md).

## What works today

| Capability | Client (`zenoh`) | Broker (`zenohb`) |
| --- | --- | --- |
| Pub/sub | `put`, `try_put` (non-blocking), `batch()`, `declare_subscriber` (pull or callback, bounded queue with FIFO or last-value conflation) | fan-out to every matching face, wildcard `*`/`**` matching |
| Query/reply | `get` (pull or callback, `GetTarget`, consolidation, timeout), `declare_queryable` | fan-out to matching queryables, fan-in with a synthesized `ResponseFinal` |
| Delivery | `del` on the receive path | `Put` and `Del` relayed; a message that needs no rewrite is forwarded byte-for-byte |
| QoS | `CongestionControl::{drop,block}` per operation | per-message: `block` traffic is queued past the watermark, `drop` traffic is shed |
| Addressing | zid-targeting via `target_zid` on `put`/`try_put`/`get` | enforced as a **filter**, never a bypass — including across the clique |
| Federation | — | full-mesh clique: gossip membership, split-horizon routing, aggregated declarations, partition reporting |
| Transport | TCP, 4-way handshake, keepalives, batching | same, plus per-connection and global `asio::strand` tiers (no mutexes) |

Interoperability is tested both ways: this codec's bytes are asserted identical to
golden vectors generated from the Rust `zenoh-codec`, and both `zenohb` and the client
are exercised against real `zenoh-rust` binaries (see the interop sections in
[`docs/RUNTIME.md`](docs/RUNTIME.md) and [`docs/BROKER.md`](docs/BROKER.md)).

**Deliberate gaps** (v1 scope, documented rather than hidden): no scouting or
peer-to-peer — a client always talks through a broker; no `Fragment` reassembly, so a
payload larger than the negotiated batch is not carried; no liveliness tokens; no
`$*`/`@` key-expression syntax; no TLS/QUIC/UDP transports; no authentication, which
is why inbound clique links are opt-in (see below). `docs/BROKER.md` keeps the full
list.

## Using the client library

`import zenoh;` is the whole public API — one module, no headers. Every fallible call
returns `std::expected`, so nothing throws and nothing is silently ignored:

```cpp
import zenoh;

int main() {
    auto session = zenoh::Session::open("tcp/127.0.0.1:7447");
    if (!session) return 1;

    // Publish.
    std::string_view const hello = "hello";
    (void)session->put("demo/greeting",
                       std::as_bytes(std::span{hello.data(), hello.size()}));

    // Subscribe, pull-style. (Pass a callback instead and drive it with run_once().)
    auto sub = session->declare_subscriber("demo/**");
    if (!sub) return 1;
    for (;;) {
        auto sample = sub->recv();
        if (!sample) break; // connection_closed, or a protocol fault
        // sample->key_expr(), sample->payload(), sample->kind()
    }
}
```

Query/reply is the same shape: `session->get(key, params, opts)` returns a `Getter`
whose `recv()` yields each reply and then an empty result when the query completes,
and `session->declare_queryable(key)` yields `IncomingQuery` objects to reply to.
Linking is `target_link_libraries(app PRIVATE zenoh)`. The runnable versions of all of
this are in [`examples/`](examples) — see [Examples](#examples) below.

## Prerequisites

- **CMake 3.28+** (C++23 named-module support)
- **Ninja** — required; the module dependency scanning relies on it
- **Clang 18+**
  - macOS: `brew install llvm` (use the Homebrew `clang++`)
  - Linux: Clang **with libstdc++** — easiest via the Docker image below

> **GCC is not supported** for this build: a libstdc++ `<type_traits>` bug breaks
> named modules with g++. Use Clang.

## Building

The build uses C++23 named modules, so it always needs Ninja + Clang. The workflow is
the same on every platform: configure a preset, build it, test it.

```sh
cmake --preset <preset>          # configure  -> build/<preset>/
cmake --build --preset <preset>  # build
ctest --preset <preset>          # run the test suite
```

### macOS

| Preset | Build type | Notes |
| --- | --- | --- |
| `clang` | Debug | ASan + UBSan — for development |
| `clang-release` | Release | `-O3 -DNDEBUG`, native CPU tuning (`-mcpu=native`/`-march=native`) and ThinLTO — fastest, host-specific (non-portable) binary |

```sh
# Debug (development)
cmake --preset clang
cmake --build --preset clang
ctest --preset clang

# Release (fastest)
cmake --preset clang-release
cmake --build --preset clang-release
ctest --preset clang-release
```

For a **portable** release (drop the host-CPU tuning, keep `-O3 -DNDEBUG` + ThinLTO):

```sh
cmake -S . -B build/release -G Ninja \
    -DCMAKE_CXX_COMPILER=clang++ -DCMAKE_BUILD_TYPE=Release   # ZENOH_NATIVE defaults OFF
cmake --build build/release
```

### Linux

Linux builds run inside the provided Docker image (Clang + libstdc++). The helper
builds the image and runs configure + build + test; artifacts land in `build/linux-*`.

```sh
scripts/docker-ci.sh                       # Debug  (linux-clang): build + test
scripts/docker-ci.sh linux-clang-release   # Release (linux-clang-release): build + test
scripts/docker-ci.sh linux-tsan            # ThreadSanitizer: the broker's concurrency gate
```

If you already have Clang + libstdc++ on the host, you can use the presets directly
instead of Docker:

```sh
cmake --preset linux-clang-release
cmake --build --preset linux-clang-release
ctest --preset linux-clang-release
```

### Reconfiguring

After editing `CMakeLists.txt` just rebuild — Ninja re-runs CMake automatically. To
wipe a stale cache (e.g. when a flag was removed from a preset), reconfigure fresh:

```sh
cmake --preset <preset> --fresh
```

## Testing

`ctest` registers one case per test, so the suite reports test-by-test and can be
narrowed or run in parallel:

```sh
ctest --preset clang                  # every test, one ctest case each
ctest --preset clang -j8              # ... in parallel
ctest --preset clang -R 'is_canon'    # only cases matching a regex
ctest --preset clang -L '^test_ke$'   # only cases from tests/test_ke.cpp
```

The test binary can also be run directly, which prints an `ok`/`FAIL` line per case
with its elapsed time, grouped by source file:

```sh
build/clang/tests/zenoh-tests                     # everything
build/clang/tests/zenoh-tests --filter test_ke    # cases whose id contains the text
build/clang/tests/zenoh-tests --list              # every case id, one per line
build/clang/tests/zenoh-tests --help
```

## Formatting & linting

Style is enforced with **clang-format** (`.clang-format`) and **clang-tidy**
(`.clang-tidy`); CI runs both. Locally:

```sh
scripts/format.sh          # check formatting (CI mode)
scripts/format.sh --fix    # reformat in place

# clang-tidy needs a built tree for its compile database (and the module BMIs):
cmake --preset clang && cmake --build build/clang
scripts/tidy.sh build/clang
```

The generated `tests/diff_vectors.hpp` is excluded from formatting
(`.clang-format-ignore`).

## Running the broker

`zenohb` is a multithreaded ASIO-based router — every preset builds it to
`build/<preset>/zenohb`, no extra flag needed:

```sh
./build/clang-release/zenohb -l tcp/0.0.0.0:7447 --threads 4
```

| Option | Meaning |
| --- | --- |
| `-l`, `--listen tcp/host:port` | Listen endpoint (default `tcp/0.0.0.0:7447`). |
| `-p`, `--peer tcp/host:port` | A peer broker to dial. Repeatable. Dialled on startup and re-dialled forever with capped backoff, so a peer that is not up yet is not an error. See [multi-broker topologies](#multi-broker-topologies). |
| `--advertise tcp/host:port` | The endpoint the rest of the clique should dial this broker on. **Required when `--listen` is a wildcard** (`0.0.0.0`, `::`), since that is not an address a peer can connect to. |
| `--threads N` | ASIO `io_context` thread pool size (default: `std::thread::hardware_concurrency()`; `--threads 1` is a valid single-threaded configuration). |
| `--accept-router-faces` | Allow an **inbound** connection announcing `whatami = router` to become a clique link. **Off by default** — see below. |
| `-h`, `--help` | Usage. Anything unrecognised is an error, not silently ignored. |

Point any client (this project's own examples below, or a real `zenoh-rust`
`z_pub`/`z_sub`/`z_get`/`z_queryable` — pass `-m client` to those, since their CLI
defaults to peer mode) at that endpoint instead of a real `zenohd`. Full routing
semantics, the concurrency model, and the wire extensions are documented in
[`docs/BROKER.md`](docs/BROKER.md).

**Use a Release build (`clang-release`/`linux-clang-release`) for anything
throughput-sensitive.** The `clang`/`linux-clang` presets are ASan+UBSan-instrumented
for correctness testing, not speed — running a real max-rate benchmark (e.g.
`zenoh-rust`'s `z_pub_thr`/`z_sub_thr`) against the ASan build can fall behind and
trip the peer's own timeout, which looks like a broker bug but isn't; see
[`docs/BROKER.md`](docs/BROKER.md#performance-testing-use-a-release-build-not-the-asanubsan-dev-preset)
for the full story.

## Multi-broker topologies

Brokers federate into a **clique**: every broker is directly linked to every other, so
a message crosses at most one inter-broker hop. Clients are unaware of it — a
subscriber on one broker receives what a publisher on another publishes, and neither
knows the mesh exists. The routing rules, gossip protocol and congestion behaviour are
in [`docs/CLIQUE.md`](docs/CLIQUE.md); what follows is how to run one.

Two things to know before the examples:

1. **You only have to configure a spanning set of links.** Each broker dials the peers
   named with `--peer`; the rest of the mesh is learned by gossip, and every learned
   member is dialled automatically. Seeding two brokers against a third is enough for
   all three to link to each other.
2. **`--accept-router-faces` is needed on whichever end of a pair *receives* the
   link.** A broker's `whatami` claim is not authenticated, so accepting it from
   anyone would let a plain client take on clique privileges (gossip ingestion, a
   replay of local declarations, split-horizon treatment, the much larger router
   congestion budgets). Inbound clique links are therefore opt-in. When both ends name
   each other with `--peer`, the duplicate connection is collapsed deterministically
   and exactly one direction survives — so in practice, **pass the flag to every
   broker in the clique**. Without it the failure is quiet: the peer connects and is
   then treated as an ordinary client, so the mesh looks up but does not federate
   (the demotion is logged to stderr).

### A pair

The smallest useful topology. Each broker names the other, so the link comes up
whichever starts first:

```sh
B=./build/clang-release

$B/zenohb -l tcp/127.0.0.1:7447 --peer tcp/127.0.0.1:7448 --accept-router-faces &
$B/zenohb -l tcp/127.0.0.1:7448 --peer tcp/127.0.0.1:7447 --accept-router-faces &

# A subscriber on one, a publisher on the other.
$B/examples/z_sub -e tcp/127.0.0.1:7447 -k 'demo/**' &
$B/examples/z_put -e tcp/127.0.0.1:7448 -k demo/greeting -p hello
```

### Three brokers, two of them linked only by gossip

`:7448` and `:7449` are each told about `:7447` and nothing else. They discover each
other, and the `:7448`↔`:7449` link is one the operator never configured:

```sh
B=./build/clang-release

$B/zenohb -l tcp/127.0.0.1:7447 --threads 2 --accept-router-faces &
$B/zenohb -l tcp/127.0.0.1:7448 --peer tcp/127.0.0.1:7447 --threads 2 --accept-router-faces &
$B/zenohb -l tcp/127.0.0.1:7449 --peer tcp/127.0.0.1:7447 --threads 2 --accept-router-faces &

# Publish on :7448, subscribe on :7449 — two brokers that were never told about each
# other. `stdbuf -o0` only matters because z_sub's stdout is a pipe here.
stdbuf -o0 $B/examples/z_sub -e tcp/127.0.0.1:7449 -k 'demo/example/**' &
$B/examples/z_put -e tcp/127.0.0.1:7448 -k demo/example/k -p hello
```

Kill `:7448` and the other two report the partition within about five seconds — with
strict split horizon a dead link does not reroute, so it is said out loud instead of
silently degrading.

### Across hosts

On a real network a broker listens on a wildcard address, which is not something a
peer can dial — so tell the clique what to dial with `--advertise`:

```sh
# Host A (10.0.0.1)
zenohb -l tcp/0.0.0.0:7447 --advertise tcp/10.0.0.1:7447 \
       --peer tcp/10.0.0.2:7447 --accept-router-faces

# Host B (10.0.0.2)
zenohb -l tcp/0.0.0.0:7447 --advertise tcp/10.0.0.2:7447 \
       --peer tcp/10.0.0.1:7447 --accept-router-faces

# Host C (10.0.0.3) — seeded against A only; it will learn B by gossip and dial it.
zenohb -l tcp/0.0.0.0:7447 --advertise tcp/10.0.0.3:7447 \
       --peer tcp/10.0.0.1:7447 --accept-router-faces
```

A broker with no advertisable endpoint still takes part — it dials out and routes
normally — it simply cannot be dialled back, so it has to be able to reach its peers
itself.

### Interoperating with `zenoh-rust`

Federation is invisible to clients, including the reference ones. `-m client` is
required on every reference binary, since their CLI defaults to peer mode:

```sh
Z=../../zenoh-rust/target/release/examples
$Z/z_sub -m client -e tcp/127.0.0.1:7449 -k 'demo/example/**' &
$Z/z_pub -m client -e tcp/127.0.0.1:7448 -k demo/example/test -p hello
```

### Checking the mesh

Each broker logs its peer links as they come up and reports partitions on change. To
verify split horizon is doing its job, put a subscriber behind *both* peers of the
publisher's broker and publish twice: each subscriber must see both messages exactly
once, never the same message twice.

## Examples

Every preset builds runtime example programs under `build/<preset>/examples/`:
`z_put`, `z_pub`, `z_put_float`, `z_pub_thr`, `z_sub`, `z_sub_thr` (pub/sub),
`z_get`, `z_queryable`, `z_querier` (query/reply), and `z_ping`/`z_pong` (latency). See
[`docs/RUNTIME.md`](docs/RUNTIME.md) for how to run them against a Zenoh router (this
project's own `zenohb`, above, or a real `zenohd`). Protocol/codec internals are
documented in [`docs/PROTO.md`](docs/PROTO.md).

Each example's CLI mirrors its `zenoh-rust` counterpart's — same option names, short
forms and defaults — so a command line written for one runs against the other. Options
the reference has but this runtime does not implement are parsed and then reported as
`note: ... has no effect` rather than being silently accepted; anything that is not a
reference option is rejected.

## Documentation

| Document | Covers |
| --- | --- |
| [`docs/PROTO.md`](docs/PROTO.md) | Wire format and codec internals: message layout, extensions, the strict-decoding rules, adding a message. |
| [`docs/RUNTIME.md`](docs/RUNTIME.md) | The client `Session`: handshake, TCP framing and batch handling, `put` vs `try_put` commit semantics, subscriber strands, interop runs against `zenohd`. |
| [`docs/BROKER.md`](docs/BROKER.md) | `zenohb`: routing semantics, the two-tier strand concurrency model, the `DestinationId` wire extension, congestion control, performance notes, documented v1 gaps. |
| [`docs/CLIQUE.md`](docs/CLIQUE.md) | Broker-to-broker federation: the split-horizon invariant, gossip membership, aggregated declarations, partition detection, peer trust. |
| [`docs/STYLE.md`](docs/STYLE.md) | The short form of the coding conventions. |
| [`CLAUDE.md`](CLAUDE.md) | Repository guide: layout, build/test workflow, conventions, toolchain constraints. |
| [`PLAN.md`](PLAN.md) / [`RESTRUCTURE.md`](RESTRUCTURE.md) | Architectural decisions (D1–D8) and the module folder layout, with rationale. |

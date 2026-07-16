# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A **high-performance C++23** implementation of the Zenoh wire protocol (v9), built
with **named modules** (`import`/`export module`, no headers for the public API).
Three static libraries:

- **`zenoh-proto`** — the pure, I/O-free codec (messages + encode/decode) plus
  `zenoh.ke` key-expression matching. No sockets, no allocation surprises, zero-copy
  decode via borrow-only views into the receive buffer.
- **`zenoh`** — the user-facing client runtime (TCP transport + `Session`), built on top
  of `zenoh-proto`. Put/get, subscribe/queryable — see `docs/RUNTIME.md`.
- **`zenoh-broker`** — `zenohb`, a multithreaded ASIO-based broker/router (accepts
  many client connections, routes pub/sub and query/reply between matching faces),
  built on `zenoh-proto` only (not `zenoh` — a from-scratch listener, not another
  client). See `docs/BROKER.md`.

### Reference implementation

[eclipse-zenoh/zenoh](https://github.com/eclipse-zenoh/zenoh) (Rust) is the
protocol/behavior reference, checked out locally at `../zenoh-rust`. Use it both to
validate behavior and to test this codebase:

- **Wire format**: `tools/vector-gen` path-deps the reference's `zenoh-proto` crate
  and emits golden byte vectors (`tests/diff_vectors.hpp`, consumed by
  `test_diff.cpp`) — the highest-value check when a message's encode/decode changes.
  If you touch wire format, regenerate/consult these vectors rather than trusting
  hand-written expectations.
- **Runtime/interop**: the reference's `zenohd` router and its `z_put`/`z_pub`/
  `z_sub`/`z_pub_thr`/`z_sub_thr`/`z_delete` example binaries are the manual
  interop test path for the `zenoh` runtime layer — see `docs/RUNTIME.md` for the
  exact invocation sequences. When adding runtime behavior (handshake, framing,
  subscriber semantics), verify it against a real reference router/peer, not just
  this repo's own binaries talking to each other.
- **Broker interop** (the reverse direction): real `zenoh-rust` `z_pub`/`z_sub`/
  `z_get`/`z_queryable` binaries connecting to this project's own `zenohb` broker —
  see `docs/BROKER.md`'s "Manual interop test" for the exact invocation sequences.

Read `PLAN.md` (architectural decisions D1–D8, with rationale) and `RESTRUCTURE.md`
(module folder layout, interface/impl split) before making structural changes — both
are living design docs, not historical records. `docs/STYLE.md` is the short version.
`docs/PROTO.md` documents wire-format/codec internals; `docs/RUNTIME.md` documents the
session/handshake/framing/subscriber behavior in detail; `docs/BROKER.md` documents
the broker's routing semantics, its two-tier ASIO strand concurrency model, and the
`DestinationId` (zid-targeting) wire extension.

## Build

Requires **CMake 3.28+, Ninja, and Clang 18+ (GCC is not supported)** — a libstdc++
`<type_traits>` bug breaks named modules with g++. macOS: `brew install llvm`. Linux:
use the Docker image (clang + libstdc++) since that's the only combination validated
for named-module builds.

```sh
cmake --preset <preset>          # configure  -> build/<preset>/
cmake --build --preset <preset>  # build
ctest --preset <preset>          # run the test suite
```

Key presets (see `CMakePresets.json`):
- `clang` — macOS Debug, ASan+UBSan (primary dev loop)
- `clang-release` — macOS Release, native CPU tuning + ThinLTO
- `clang-coverage` — macOS, LLVM source coverage
- `linux-clang` — Linux Docker, Debug, ASan+UBSan (**CI primary**)
- `linux-clang-release`, `linux-coverage`, `linux-fuzz`
- `linux-tsan` — Linux Docker, ThreadSanitizer (`-DZENOH_TSAN=ON`, mutually exclusive
  with `ZENOH_SANITIZE`) — the broker's concurrency-safety gate; runs
  `tests/test_broker.cpp`'s multi-threaded stress cases instrumented with
  `-fsanitize=thread` (see `docs/BROKER.md`).

Linux builds run via `scripts/docker-ci.sh [preset]` (builds the toolchain image,
then configure+build+test — same path as CI). On a host with clang+libstdc++ already
set up you can use the presets directly.

After editing `CMakeLists.txt`, just rebuild (Ninja re-runs CMake). To wipe a stale
cache: `cmake --preset <preset> --fresh`.

## Testing

There is **one test binary** (`zenoh-tests`, `tests/*.cpp`) registered as a single
ctest case (`ctest --preset <preset>` runs the whole suite — there's no ctest-level
filtering to a single source file). The harness is a from-scratch minimal one
(`tests/ztest.hpp`: `TEST("name") { CHECK(cond); }`), not doctest/GoogleTest — no
CLI test-name filter exists in `run()`; narrow by commenting out `TEST` blocks or
running the whole binary directly (`build/<preset>/tests/zenoh-tests`).

Test files are organized by layer, not 1:1 with modules: `test_varint.cpp`,
`test_codec.cpp`, `test_ext.cpp`, `test_ke.cpp` (key-expression matching),
`test_transport.cpp`, `test_declare.cpp`, `test_query.cpp`, `test_push.cpp`,
`test_put.cpp`, `test_negative.cpp` (malformed input / error paths), `test_diff.cpp`
(differential vectors, see below), `test_session.cpp`/`test_tcp.cpp`/
`test_strand.cpp`/`test_subscriber.cpp`/`test_query_api.cpp` (client runtime, against
a real `socketpair`/loopback), `test_broker.cpp` (the broker — a real `Broker` on
loopback port 0, driven by real `Session` clients; see `docs/BROKER.md`'s "Testing"),
`test_coverage.cpp`/`test_umbrella.cpp` (fill gaps for the coverage gate).

**Differential testing**: `tools/vector-gen` (a small Rust program that path-deps the
`zenoh-rust` reference `zenoh-proto` crate) emits golden byte vectors into the
generated `tests/diff_vectors.hpp` (excluded from clang-format —
`.clang-format-ignore`). `test_diff.cpp` asserts our encode is byte-identical to the
reference and that we can decode+re-encode the reference's bytes unchanged. This is
the highest-value check when touching wire format — if you change a message's
encode/decode, regenerate/consult these vectors.

**Fuzzing**: `fuzz/fuzz_decode.cpp` is a libFuzzer target over the decoders (Linux
clang only, `linux-fuzz` preset). Run via `scripts/fuzz.sh [seconds]`.

Coverage: `scripts/coverage.sh [preset]` (default `linux-coverage`) emits
`build/<preset>/coverage.lcov` covering **library sources only** (tests/examples/fuzz
excluded via `-ignore-filename-regex`). Codecov gate: 80% project line coverage
(`codecov.yml`), patch coverage is informational only.

## Formatting & linting

```sh
scripts/format.sh          # clang-format check (CI mode)
scripts/format.sh --fix    # reformat in place

# clang-tidy needs a built tree (compile database + module BMIs):
cmake --preset clang && cmake --build --preset clang
scripts/tidy.sh build/clang     # defaults to build/linux-clang if no arg given
```

CI runs both via `scripts/lint.sh` inside the Docker image, reusing the
`build/linux-clang` compile database from the build step. `.clang-tidy` disables a
handful of checks that fight this codebase's deliberate idioms (documented inline in
the file) — e.g. the `ZTRY` macro's `expected::value()` call never actually throws.

## Conventions

- **Public API must be well documented.** Every exported type/function/enum (not
  just the codec — this applies to `zenoh.session`/`Sample`/`Subscriber`/etc. too)
  gets a `///` doc comment: purpose, parameters, `expected` error conditions, and a
  wire-format reference where applicable. See `docs/STYLE.md` and the Codec design
  section below for the full standard.
- **Every new feature needs corresponding tests.** New codec/message support needs
  round-trip coverage and, where the reference implementation defines the wire
  format, differential vectors (see `tools/vector-gen`/`test_diff.cpp` above); new
  runtime/session behavior needs a `tests/test_*.cpp` case exercising it over a real
  `socketpair`/loopback, matching the existing test files' granularity.

## Architecture

### Module layering (acyclic, one folder per functional area under `src/`)

```
util/    zenoh.util             ByteField<Shift,Bits> header bit descriptors, ZTRY macro, LE helpers
buffer/  zenoh.buffer           ByteReader/ByteWriter (concrete cursors over std::span), Readable/Writable concepts
codec/   zenoh.varint           VLE u64 (1-9 byte LEB128-style), branch-lean encode/decode
         zenoh.codec            primitive codecs: u8/VLE-int/array<byte,N>/prefixed-or-remainder span & string_view/optional<T>
         zenoh.codec.ext        optional-field ("extension") header helpers: write/read_ext_{unit,u64,zbuf}, skip_ext
ke/      zenoh.ke               key-expression matching: is_canon/canonize/intersects/includes (*/** only, v1 scope)
proto/
  fields/     zenoh.proto.fields    ZenohId, WhatAmI, Reliability, WireExpr, QoS, Encoding, Resolution, ...
  exts/       zenoh.proto.exts      SourceInfo, Timestamp, Attachment, NodeId, QueryableInfo, ...
  transport/  zenoh.proto.transport Init{Syn,Ack}, Open{Syn,Ack}, Close, KeepAlive, Frame
  network/    zenoh.proto.network   Push/Put, Request/Query, Response/Reply/Err, ResponseFinal, Interest
  declare/    zenoh.proto.declare   Declare + its 9 sub-bodies
  interest/   zenoh.proto.interest  Interest/InterestFinal
  proto.cppm  zenoh.proto           umbrella over all message modules; Message/NetworkBody variants
runtime/
  tcp.{cppm,cpp}      zenoh.runtime.tcp     TcpLink (RAII POSIX socket), IoError; POSIX headers stay in the .cpp
  strand.cppm         zenoh.runtime.strand  Strand<T> per-subscriber bounded queue (ordered / last_value conflation)
  session.{cppm,cpp}  zenoh.session         Session, ZError, Sample, Subscriber, Queryable, Getter — handshake,
                                            put/try_put/batch (with target_zid), get/declare_queryable, recv pump
zenoh.cppm  zenoh   public umbrella; re-exports zenoh.session only (codec types are NOT re-exported)
```

Broker (`broker/src/`, `zenoh-broker` library, links `zenoh-proto` not `zenoh`; see
`docs/BROKER.md` for the full concurrency model / routing semantics):

```
broker/src/
  resource.{cppm,cpp}  zenoh.broker.resource  ResourceTable, FaceCtx — declared subscriber/queryable patterns
  tables.{cppm,cpp}    zenoh.broker.tables    Tables — global routing state on one asio::strand (Tier 2)
  broker.{cppm,cpp}    zenoh.broker           Broker (bind/run/stop); Face + accept loop live inside broker.cpp
                                              only (a toolchain constraint — see docs/BROKER.md)
  main.cpp             (zenohb executable)    CLI: -l/--listen tcp/host:port, --threads N
```

`zenoh-proto` (util → buffer → codec → codec.ext → proto/*) is a pure leaf: no I/O,
no allocation on the codec hot path, fully independent of `zenoh` (runtime). `zenoh`
depends on `zenoh-proto` but not vice versa. Tests import specific leaf modules
(`zenoh.proto.network`), never the umbrella, to avoid full-rebuild fan-out.

### Where code lives: interface (`.cppm`) vs implementation (`.cpp`)

This split is deliberate and **not** "everything goes to `.cpp`" — see `RESTRUCTURE.md`
Decision A for the full rationale. The short version:

- **Stays `inline` in the `.cppm`** (hot, called per-field, or generic): `util`,
  `buffer`, `varint`, `codec`, `codec.ext` in their entirety; within the message
  modules — `body_len()`/`encoded_len()`, `operator==`, anything templated
  (`get_uint_as<T>`, `load_le<T>`), and variant-dispatching `encode`/`decode`
  (`PushBody`, `ResponseBody`, `DeclareBody`).
- **Moves to `.cpp`** (called once per message, not per field — safe to out-of-line):
  the plain per-message `encode`/`decode` bodies in `proto.{fields,exts,network,
  transport,declare,interest}`.

Rationale: a definition in a module implementation unit is invisible to the optimizer
across the module boundary. For the byte-level primitives, inlining *is* the
performance story (LTO is off in ASan/fuzz builds), so they stay in the interface
unit; message bodies are large enough that this cost is negligible.

### Codec design (see PLAN.md D1-D7 for full rationale)

- **No exceptions on the codec path** — `std::expected<T, CodecError>`, short-circuited
  with the `ZTRY` macro (GNU statement-expression extension; requires
  `CMAKE_CXX_EXTENSIONS ON`).
- **Messages are borrow-only views** into the receive buffer (`std::span<const
  std::byte>` / `std::string_view` fields) — valid only while that buffer lives. No
  `Bytes` policy template parameter; decode entry points carry
  `[[clang::lifetimebound]]`.
- **Contiguous-frame contract**: `read_slice` always returns a contiguous borrow; the
  transport reassembles a full frame before handing it to decode. Scatter-gather /
  ring buffers live only in the runtime layer, never in the codec.
- **No virtual dispatch, no macro-generated codec engine.** Each message has two flat,
  hand-written functions — `encode(writer, m)` / `decode(reader) -> expected<Msg>` —
  that read top-to-bottom like the wire diagram. "Extensions" (the protocol's optional
  fields) are ordinary `std::optional<T>` members with a short `while`/`switch` decode
  loop per message, not a generic ID-dispatch framework. If you're adding a new
  message or field, follow this pattern — don't introduce a generic engine.
- **Every function/member function uses trailing return type** (`auto f(args) -> R`);
  constructors/destructors are exempt (`docs/STYLE.md`).
- **Documentation**: every `.cppm`/`.cpp` gets a one-paragraph file header; every
  exported type/function/enum gets a `///` doc comment (purpose, params, `expected`
  error conditions, wire-format reference where applicable); private `.cpp` internals
  get `//` comments only where the logic is non-obvious (bit math, ordering quirks,
  the contiguous-frame assumption).

### Runtime layer (client `Session`) — see `docs/RUNTIME.md` for full detail

**Vertically integrated, not sans-IO** (PLAN.md D8): the deliberate divergence from
the Rust reference's `zenoh-sansio` split. `Session` owns the socket, protocol state
(zid, frame SN, keyexpr resmap), and encode/decode buffers directly, driving
`recv → reassemble → decode → dispatch` and `encode → batch → send` as one
mostly-inlinable flow rather than through link/driver trait-object layers. Only a
narrow `recv`/`send`/`poll` syscall boundary is mockable; the runtime is tested over a
real `socketpair`/loopback pair, not a fake transport.

A client talks **only through a router** (`zenohd`) — no scouting, no peer-to-peer.
Handshake: InitSyn → InitAck (adopt cookie + router's batch size) → OpenSyn → OpenAck,
blocking; the socket then switches to non-blocking for the data phase (`try_put`
backpressure detection). TCP framing is a 2-byte LE length prefix per batch.
`put` blocks (payload sent via `writev`, never copied); `try_put` never blocks and
preserves frame-SN/byte-stream integrity across partial writes (see `docs/RUNTIME.md`
"put vs try_put" for the exact commit semantics — this is easy to get subtly wrong).
Subscribers use a bounded per-subscriber `Strand` (`ordered` FIFO-with-backpressure,
or `last_value` conflation by key); decode errors permanently fault the subscriber
(no resync mid byte-stream). `get`/`declare_queryable` follow the same shape
(`Getter`/`Queryable`, `pending_gets_` keyed by request id); `put`/`try_put`/`get`
accept an optional `target_zid` (zid-targeting — see `docs/BROKER.md`, since the
broker is what actually enforces it as a filter).

### Broker (`zenohb`) — see `docs/BROKER.md` for full detail

The broker is a **separate, from-scratch** ASIO-based component, not built on the
client `Session`/`TcpLink` (a blocking, single-peer design is the wrong shape for a
multi-connection listener). It's this project's first genuinely multithreaded
component: `Broker::run(unsigned num_threads)` spawns an ASIO `io_context` thread
pool, and correctness comes from a **two-tier `asio::strand` model** (one strand per
connection for socket/rx-cursor/tx-queue state, one global strand for the shared
routing state — face registry, `ResourceTable`, query fan-out/fan-in bookkeeping) —
no mutexes anywhere. `assert(strand_.running_in_this_thread())` at the top of every
mutating method makes an off-strand access a deterministic assertion failure, not
just a probabilistic ThreadSanitizer catch; the `linux-tsan` preset (see Build above)
is the actual dynamic verification gate. Uses vendored standalone ASIO
(`third_party/asio/`) via non-throwing completion-token forms only, consistent with
this codebase's no-exceptions convention.

### Examples

`examples/{z_put,z_pub,z_put_float,z_pub_thr,z_sub,z_sub_thr}.cpp` are runnable
C++ equivalents of the corresponding `zenoh-rust` example binaries, built by default
(`ZENOH_EXAMPLES=ON`) into `build/<preset>/examples/`. They're also the manual
interop test path against a real `zenohd` router — see `docs/RUNTIME.md` for the
exact invocation sequences (router + reference pub/sub + this repo's binaries).

`zenohb` (`broker/src/main.cpp`, always built) is the broker executable —
`zenohb -l tcp/host:port [--threads N]`. It's the manual interop test path in the
*other* direction: real `zenoh-rust` example binaries connecting to this project's
own broker — see `docs/BROKER.md`'s "Manual interop test".

### Toolchain constraints (don't try to "fix" these — they're upstream bugs)

- **GCC is blocked entirely**: libstdc++'s `<type_traits>` fails inside a module's
  global-module-fragment on both g++-14 and g++-15.
- **clang + libc++ needs a recent clang** (works on clang 22/macOS dev build; broken
  on clang-19 — libc++ internal headers clash when pulled into a module GMF).
- **Linux CI therefore uses clang + libstdc++** (`linux-clang`), the only combination
  validated for named modules on Linux. `linux-gcc` preset exists but is parked.
- **ASIO types must never appear in a `.cppm` interface unit** (`asio::io_context`,
  `asio::ip::tcp::acceptor`/`socket`, `asio::awaitable<T>` — even in a private,
  non-exported declaration): doing so poisons that module's BMI for any importer that
  also textually includes an ordinary standard header, triggering a clang/libc++
  `cannot add 'abi_tag' attribute in a redeclaration` error. Confirmed via bisection
  to be a modules-only interaction (identical headers compile fine outside a module).
  See `docs/BROKER.md`'s "Why `Face` isn't its own module" for the PIMPL/anonymous-
  namespace workaround this forces on the broker.

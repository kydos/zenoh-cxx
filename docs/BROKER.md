# zenohb — the C++23 broker

`zenohb` is a multithreaded Zenoh **broker**: it accepts many client connections,
tracks their Declare state (subscribers, queryables), and routes Push (pub/sub) and
Request/Response (query/reply) traffic between matching faces. It is this project's
first genuinely multithreaded component and the counterpart to `docs/RUNTIME.md`'s
client `Session` — where the client library talks to a router, `zenohb` **is** the
router, built from scratch on standalone ASIO rather than the client's hand-rolled
blocking POSIX transport.

A client (this project's `Session`, or a real `zenoh-rust` peer) talks to `zenohb`
exactly as it would talk to `zenohd` (the reference router): connect, complete the
InitSyn/InitAck/OpenSyn/OpenAck handshake, then exchange Declare/Push/Request/Response
traffic. `zenohb` is a **hub** — every client face connects directly to it, there is
no scouting, no peer-to-peer, and no multi-hop mesh/link-state routing between
brokers.

## Module map

| Module | Unit | Contents |
| --- | --- | --- |
| `zenoh.ke` | `src/ke/ke.cppm`/`.cpp` | Key-expression matching: `is_canon`, `canonize`, `intersects`, `includes`. Pure, no I/O — lives in `zenoh-proto`, not the broker library, since it's a codec-adjacent concern (matching `WireExpr` patterns), reused by both. `*`/`**` only (v1 scope); `$*`/`@` are an explicit gap. |
| `zenoh.broker.resource` | `broker/src/resource.cppm`/`.cpp` | `ResourceTable`, `FaceCtx`, `FaceId`. Declared subscriber/queryable patterns and `matching_subscribers`/`matching_queryables` lookups (via `zenoh.ke::intersects`). Pure logic, no ASIO, no I/O. |
| `zenoh.broker.tables` | `broker/src/tables.cppm`/`.cpp` | `Tables` — the broker's global routing state: face registry, `ResourceTable`, and query fan-out/fan-in bookkeeping, all serialized on one `asio::strand`. `FaceHandle`/`RoutedPush`/`RoutedRequest`/`RoutedResponse` are the owned, strand-crossing-safe shapes routing operates on. |
| `zenoh.broker` | `broker/src/broker.cppm`/`.cpp` | `Broker` (bind/run/stop, the `io_context` + configurable thread pool) **and**, entirely inside `broker.cpp`'s anonymous namespace, the per-connection `Face` class and the accept loop — see "Why `Face` isn't its own module" below. |
| `zenohb` | `broker/src/main.cpp` | CLI (`-l`/`--listen tcp/host:port`, `--threads N`) + `Broker::bind`/`run`. |

`zenoh-broker` (the static library backing `zenohb` and the test suite) links
`zenoh-proto` — **not** `zenoh`, the client runtime. `Session`/`TcpLink`'s blocking,
single-peer-client design is the wrong shape for a from-scratch ASIO listener; the
broker only needs the pure codec.

## Wire extension: `DestinationId` (zid-targeting)

A project-local, **non-mandatory** wire extension — not present in upstream
`zenoh-rust`, and not needed for interop with it (a real reference peer that doesn't
know this extension simply skips it via the ordinary extension-skip path; it never
sees a targeted message narrowed away from it any differently than an untargeted
one it wasn't subscribed to). `struct DestinationId { ZenohId zid; }`
(`src/proto/exts/exts.cppm`), mirroring the existing `EntityGlobalId` ZStruct layout
(header nibble = `zid.len - 1`, then the zid bytes):

- `Push` carries it as `std::optional<DestinationId> dest` at **ext id `0x4`**.
- `Request` carries it as `std::optional<DestinationId> dest` at **ext id `0x7`**
  (encoded before `nodeid`, which stays last — a pre-existing, unrelated quirk this
  extension doesn't disturb).

`Session::put`/`try_put`/`get` accept a trailing `std::optional<PeerId> target_zid`
(`PeerId` is the client's own opaque mirror of `ZenohId` — see `docs/RUNTIME.md`).
When set, `zenohb` narrows delivery/fan-out to the one face whose `zid` matches.

**This is a filter, never a bypass**: a targeted `put`/`get` still only ever reaches
a face with a genuinely matching Subscriber/Queryable declaration on that key —
`target_zid` narrows *which* of the matching faces gets it, exactly like a `WHERE`
clause narrows a query that must already match on the join condition. Setting
`target_zid` to a zid with no live matching declaration at all yields **zero**
deliveries, not a fallback to every matching face. `tests/test_broker.cpp` proves
this directly for both `put` and `get` (see "Testing" below).

## Concurrency: the two-tier strand model

`zenohb` is built on **standalone (non-Boost) ASIO** (`third_party/asio/`, vendored —
see its `NOTICE.md`), consumed only through non-throwing completion-token forms
(`asio::as_tuple(asio::use_awaitable)` for coroutines, explicit `asio::error_code`
out-params for callback-style calls), consistent with this project's no-exceptions
codec convention. `Broker::run(unsigned num_threads)` spawns `num_threads` OS threads
each calling `io_context::run()` — the standard ASIO N-threads-on-one-`io_context`
pattern. `--threads 0` (the CLI default) resolves to `std::thread::hardware_concurrency()`;
`--threads 1` is a valid, fully single-threaded configuration (what `tests/test_broker.cpp`'s
deterministic functional cases use).

No mutex appears anywhere in the routing/connection-handling code. Correctness comes
from "at most one handler ever touches this state at a time," enforced structurally by
which `asio::strand` a given operation runs on — not from locking:

- **Tier 1 — one `asio::strand` per `Face`** (`broker.cpp`'s `Face::strand_`): guards
  that connection's socket, rx cursor, per-face `resmap_`/`sub_ids_`/`qbl_ids_`,
  outgoing frame queue, and frame SN. Nothing outside a `Face`'s own strand ever
  touches its state directly. `Face` is `shared_ptr`-managed
  (`enable_shared_from_this`) so an in-flight async handler can outlive whatever
  triggered its cleanup; `FaceId` is a monotonic, never-reused integer (an atomic
  counter), so stored references in `Tables` never alias a reconnected peer's slot.
- **Tier 2 — one global `Tables::strand()`**: guards the face registry, the
  `ResourceTable`, and the query fan-out/fan-in maps (`pending_queries_`/
  `fanout_remaining_`). A decoded Push/Request/Response is **materialized into an
  owned struct** (`RoutedPush`/`RoutedRequest`/`RoutedResponse` — owned `std::string`/
  `std::vector<std::byte>`, never a borrowed view) on the Face's own strand *before*
  `asio::post`ing it to `Tables::strand()` — required because `zenoh.proto` messages
  are borrow-only views into the receive buffer (PLAN.md D2), and that buffer is
  reused by the next `async_read` before a posted cross-strand handler runs.
  Delivery back down to a target face is symmetric: `asio::post(target
  face's strand, ...)`.
- **This is a deliberate v1 simplification, not a ceiling.** One global routing strand
  means routing *decisions* aren't parallelized across cores — aggregate throughput
  under `--threads N` scales from concurrent I/O completion handling across faces
  (Tier 1, genuinely parallel), not concurrent routing (Tier 2, intentionally serial).
  For a hub broker, this is very unlikely to be the bottleneck at realistic scale;
  sharding `Tables` (e.g. per-key-hash strands) is a contained future optimization if
  profiling ever shows otherwise.
- **Defensive backlog bound between the tiers**: `asio::post` itself has no queue
  limit, so nothing would otherwise stop a `Face` decoding/posting faster than the
  single routing strand can drain it. `Tables::pending_routing_jobs()` is an atomic
  counter incremented by `Face::post_to_tables` before every post and decremented
  when the posted job starts running; `Face::throttle_if_backlogged` pauses that
  Face's own reads once the aggregate count crosses `max_pending_routing_jobs`
  (4096), letting ordinary TCP flow control push resulting backpressure back to the
  peer. This is hardening for the many-simultaneous-fast-publishers case the point
  above already flags as unproven risk — a single fast publisher was measured (via
  the counter itself, and via release-build RSS staying flat across 3M+ published
  messages) to never actually backlog the routing strand at all, so this guards
  against a scenario worse than anything actually observed, not a fix for a
  confirmed bottleneck.
- **A genuine footgun this design deliberately avoids**: nothing here ever
  `co_await`s onto the routing strand and assumes the following lines run serialized
  with other routing work — a coroutine's associated executor doesn't change just
  because it posted a job elsewhere. Every `Tables`/cross-face interaction is a
  fire-and-forget `asio::post(...)`, never a `co_await`. (A real bug of exactly this
  *shape* — though at the accept-loop level, not a `co_await` — was caught during
  development: the accept loop was originally spawning each connection's whole
  coroutine chain onto the bare `io_context` instead of its own per-Face `strand`, so
  the read path silently ran un-serialized with the strand-posted write path. Adding
  `assert(strand_.running_in_this_thread())` at the top of every `Face`-state-mutating
  method — mirroring `Tables`'s existing discipline checks — caught it immediately.)

### Verification, not just design

`assert(strand_.running_in_this_thread())` at the top of every `Tables`- and
`Face`-mutating method converts "did a handler run on the wrong strand" from a
probabilistic ThreadSanitizer catch into a deterministic one, fired on the very first
off-strand call regardless of whether a race happened to manifest in that run.

TSan itself is still the actual verification that the design has no data races: the
`linux-tsan` CMake preset (`-DZENOH_TSAN=ON`, mutually exclusive with
`-DZENOH_SANITIZE=ON`/ASan+UBSan — the two sanitizer families can't be linked
together) runs the full test suite, including `tests/test_broker.cpp`'s
multi-threaded stress cases, instrumented with `-fsanitize=thread`:

```sh
cmake --preset linux-tsan && cmake --build --preset linux-tsan
ctest --preset linux-tsan
```

A clean run here is the concrete check behind "the broker implementation is
multithreaded safe" — not an aspirational note. (This preset is Linux/Docker-primary,
like `linux-clang`; run it via `scripts/docker-ci.sh linux-tsan` on a host without a
clang+libstdc++ toolchain already set up for named modules.)

## Routing semantics

- **Push**: the inbound `wire_expr` is resolved to an owned key (supporting both
  `scope == 0` literal and `scope != 0` per-face `resmap_` lookup — a `Face` tracks
  its own peer's `DeclareKeyExpr`/`UndeclareKeyExpr` traffic, the same shape as the
  client `Session::resolve_key`). The resolved key **must be literal** — a `*`/`**`
  in a *published* key is malformed input, not a wildcard, and is rejected rather
  than silently mismatched via `ke::intersects`. Matching subscriber faces
  (`ResourceTable::matching_subscribers`, deduplicated by `face_id` so a face with
  two+ overlapping declared subscriptions is still delivered to exactly once) are
  found via `zenoh.ke::intersects`, filtered by `dest` if set, and the message is
  **re-encoded per target** (decode → match → re-encode, never a byte relay — see
  "v1 simplifications" below for why). A target face that's congested (its own
  outbound queue backed up past a watermark) is silently skipped rather than
  queued further or disconnected — see "Performance" below for the full policy and
  why this can drop an otherwise-reliable Push.
- **Request/Response/ResponseFinal**: unlike Push, a query key expression is
  legitimately a *pattern* — `get()`'s own default shape is `"demo/example/**"` — so
  `Tables::on_request` does **not** reject a wildcarded key the way `route_push` rejects
  one; `ke::intersects` is documented safe (order-invariant, no wrong answers, only
  possibly-redundant ones) on non-canonical/wildcarded input on either side. Matching
  queryable faces are found the same way (deduplicated by `face_id`), filtered by
  `QueryTarget`/`dest`, and a **fresh local request id is allocated per target face**,
  recorded in `pending_queries_` so the eventual `Response`/`ResponseFinal` can be
  routed back to the right requester. **Zero matching queryables synthesizes an
  immediate `ResponseFinal`** — a `get()` over nothing still has to terminate, not
  hang. A `Response` is forwarded without erasing its `pending_queries_` entry (more
  may follow); a `ResponseFinal` erases it and decrements the requester's fan-in
  counter, synthesizing the requester's own `ResponseFinal` once every answering face
  has finished.
- **Disconnect mid-query**: `Tables::remove_face` walks both `pending_queries_` and
  `fanout_remaining_`. A queryable face vanishing mid-answer is treated exactly as if
  it had sent its `ResponseFinal` (so a requester waiting on it still terminates,
  never hangs); a requester face vanishing just erases its entries (nothing left to
  forward to). `tests/test_broker.cpp` has a dedicated case proving this is genuinely
  the broker's own cleanup doing the work, not a client-side RAII coincidence.
- **`QueryTarget` in a hub topology**: `best_matching` and `all` mean the same thing
  here — "every directly-matching queryable" — since they only diverge with
  multi-hop link-state routing, which a single-broker hub doesn't have.
  `all_complete` filters to queryables that advertised `QueryableInfo.complete`.

## v1 simplifications (documented gaps, not accidents)

- **`Interest`/`InterestFinal` (mid `0x19`) are decoded and tolerated, never
  answered.** A real `zenoh-rust` client sends an `Interest` unconditionally in some
  cases (e.g. a `Publisher`'s writer-side matching-status tracking) even though v1
  implements no declare-replay/interest-based sync at all. `Face::on_interest`
  decodes it (disambiguating `Interest` vs `InterestFinal` by the header byte's
  2-bit MODE field) purely to stay correctly positioned in the byte stream — it does
  **not** reply. This was empirically necessary: replying with an `InterestFinal`
  (the natural-seeming "nothing to reconcile" response) was tried first and
  triggered a genuine `unreachable code` panic inside the reference client's own
  routing internals (`south-bound client hat`) — a real client-mode session does not
  expect a router to answer this particular `Interest` shape at all. Silently
  *dropping* the message (the fallback for a genuinely unrecognized mid) is also
  wrong, since that faults the whole face — so it must be decoded, just never
  answered. If you're extending broker-side declare-sync in the future, verify
  against a real `zenoh-rust` peer before assuming a "helpful" reply is correct;
  the protocol's actual expectations here are narrower than they look.
- **Outbound composition always emits `scope == 0`** (full keyexpr) to every
  receiver, regardless of what that receiving face itself declared via
  `DeclareKeyExpr`. Per-target outbound resmap compression is a contained future
  optimization, not required for correctness.
- **No broker-side query timeout enforcement.** `GetOptions::timeout_ms` is enforced
  entirely client-side (`Getter::recv()`'s pump loop) — sufficient for correctness on
  its own; a slow queryable just means a slow reply, not a broker-side leak (the
  fan-in bookkeeping is bounded by `remove_face`, not by a timer).
- **No scouting, no peer-to-peer, no multi-broker mesh.** `zenohb` is a single hub;
  every face is a direct client connection.
- **`$*` sub-chunk globbing and `@`-verbatim key-expression chunks** are out of scope
  for `zenoh.ke` (v1: `*`/`**` only) — matches what this project's own `WireExpr`
  usage and its interop targets actually produce.

## Why `Face` isn't its own module (a toolchain constraint, not a design choice)

A real, reproducible clang+libc++ named-modules bug: if a `.cppm` interface unit's
global module fragment — or even a private, non-exported declaration inside it —
causes `asio::io_context`, `asio::ip::tcp::acceptor`/`socket`, or `asio::awaitable<T>`
to appear in that interface unit's AST at all, then *any* importer of that module
that also textually includes an ordinary standard header (`<string>` alone is
enough) fails with `cannot add 'abi_tag' attribute in a redeclaration` inside
libc++'s `<__bit_reference>`/`<vector>` internals — confirmed via bisection, and
confirmed the identical headers compile fine in a *non-module* translation unit, so
this is a modules-only interaction bug, not a code error.

The workaround, applied throughout `broker/src/broker.{cppm,cpp}`:

- `Broker` uses PIMPL: `struct Impl;` is forward-declared only in `broker.cppm`
  (behind `std::unique_ptr<Impl> impl_`); the real `asio::io_context`/
  `asio::ip::tcp::acceptor` (and the acceptor's own dedicated strand — see below)
  live in `struct Broker::Impl`, fully defined only in `broker.cpp`, an
  implementation unit that never contributes to the module's importable BMI.
  `~Broker()` is declared in the header but defined out-of-line in `broker.cpp` (a
  `unique_ptr<Impl>`'s deleter needs `Impl` complete).
- The entire `Face` class, the accept loop, and everything that spells
  `asio::awaitable<T>` live as file-local (anonymous-namespace) code inside
  `broker.cpp`. `Broker::run()`'s accept loop is a **local lambda** passed directly
  to `asio::co_spawn`, never a named/declared coroutine method — so no `.cppm`
  anywhere in this project ever names `asio::awaitable<T>`.

If you're extending the broker and tempted to pull `Face` (or anything ASIO-typed)
out into its own module for organizational tidiness: don't, until this upstream bug
is fixed — verify empirically first (a throwaway module + `#include <string>`
importer) rather than assuming it's been fixed by a newer clang.

## Manual interop test

`zenohb` replaces `zenohd` as the router in this recipe — the direction is reversed
from `docs/RUNTIME.md`'s (there, this project's `Session` connects to a real
`zenohd`; here, real `zenoh-rust` client binaries connect to this project's broker).
Build `zenohb` (`ZENOH_EXAMPLES`/default build, see `CLAUDE.md`) and the reference
`z_pub`/`z_sub`/`z_get`/`z_queryable` binaries from `../zenoh-rust`
(`cargo build --release -p zenoh-examples --example z_pub --example z_sub --example
z_get --example z_queryable`, plus `cargo build --release -p zenohd --bin zenohd` if
you also want to cross-check against the real router separately).

**Pass `-m client` to every reference binary.** Their shared CLI helper
(`zenoh_examples::CommonArgs`) defaults `mode` to **`peer`**, not `client` — in peer
mode a real `zenoh-rust` session establishes its own scouting/routing-table
machinery and expects a full peer, not a plain client-facing hub, on the other end
of the link; `zenohb` only implements the client-facing router role. Without
`-m client` the connection silently never delivers data (no error printed on
either side — this is easy to misdiagnose as a broker bug rather than a CLI-default
mismatch). `-e`/`--connect` still points at `zenohb` the same way either way.

```sh
# Start the broker.
./build/clang/zenohb -l tcp/127.0.0.1:7447 --threads 4 &

# Pub/sub: real zenoh-rust subscriber, this repo's publisher.
../zenoh-rust/target/release/examples/z_sub -m client -e tcp/127.0.0.1:7447 -k 'demo/example/**' &
./build/clang/examples/z_pub -e tcp/127.0.0.1:7447 -k demo/example/test -p hello

# Pub/sub, the other direction: this repo's subscriber, real zenoh-rust publisher.
./build/clang/examples/z_sub -e tcp/127.0.0.1:7447 -k 'demo/example/**' &
../zenoh-rust/target/release/examples/z_pub -m client -e tcp/127.0.0.1:7447 -k demo/example/test -p hello

# Query/reply: real zenoh-rust queryable, real zenoh-rust get() -- both through zenohb.
../zenoh-rust/target/release/examples/z_queryable -m client -e tcp/127.0.0.1:7447 &
../zenoh-rust/target/release/examples/z_get -m client -e tcp/127.0.0.1:7447 -s 'demo/example/**'
```

The last line's wildcarded `-s 'demo/example/**'` against `z_queryable`'s literal
default key (`demo/example/zenoh-rs-queryable`) exercises the exact wildcard-query
routing path `tests/test_broker.cpp` regression-tests — confirming it holds against
a real, independent implementation's wire encoding, not just this project's own
client talking to its own broker. All three scenarios above (both pub/sub
directions and query/reply) were manually verified against real, freshly-built
`z_pub`/`z_sub`/`z_get`/`z_queryable` binaries while writing this doc, including the
`Interest`-tolerance fix noted under "v1 simplifications" (real `z_pub` sessions
send one unconditionally; the fix was required for this exact recipe's pub/sub
directions to work, not a hypothetical).

`target_zid`/`DestinationId` has **no interop story**: it's project-local, and a real
`zenoh-rust` peer has no API surface to set it. Exercising it requires this project's
own `Session::put`/`get`'s `target_zid` parameter (see `tests/test_broker.cpp`'s
zid-targeting cases) — a real reference peer simply never sends the extension, and
`zenohb` treats its absence exactly like today's unfiltered fan-out (always has:
`DestinationId` is a non-mandatory extension, so old/foreign peers are unaffected by
its existence).

## Performance: from a crashing connection to ~700k–900k+ msg/s

**Use a Release build (`clang-release`/`linux-clang-release`) for anything
throughput-sensitive** — the `clang`/`linux-clang` presets (ASan+UBSan) are for
correctness testing, not speed, and their per-access instrumentation overhead is
enough on its own to change the numbers below by several times.

### The original bug wasn't ASan — it was a real close-on-overflow policy

An earlier version of this section claimed the ~20k–26k msg/s ceiling seen with
real `zenoh-rust` `z_pub_thr`/`z_sub_thr` was fine, and that only the ASan preset's
overhead caused the `Unable to push non droppable network message ... Closing
transport!` crash some users hit. That was wrong, caught only once a faster
producer (this project's own `z_pub_thr --batch N`, and a from-scratch measurement
harness) was used against a Release build and the *same* connection-killing
behavior reproduced instantly. The real root cause: `Face::enqueue_and_pump`'s
overflow policy used to call `close_now()` the moment a consumer's outbound
`tx_queue_` crossed 1024 buffered frames — turning *any* transient production/drain
mismatch, not just a genuinely stuck peer, into an outright disconnect. The
reference client's own unbatched, one-`put()`-at-a-time publish rate (~20k–35k
msg/s, limited by its own per-call overhead, not the broker) happened to stay just
under that threshold, which is why it looked fine until a faster producer was
tried.

Fixed with a proper congestion policy (see below) plus several throughput fixes
identified by an architect review and a systems-expert review specifically
commissioned to chase this down. Net result (Release build, loopback, 8-byte
payload, single publisher → single subscriber):

| Configuration | Throughput | Notes |
| --- | --- | --- |
| Before any of this (original bug) | connection closed within ~1s of a fast producer | not a real number — the "ceiling" was actually a crash |
| Real `zenoh-rust` `z_pub_thr` (unbatched) → this project's `z_sub_thr` | ~575k–580k msg/s, `--threads 1` | the reference client's own per-`put()` rate is now well below the broker's actual capacity |
| This project's `z_pub_thr --batch 50` → real `zenoh-rust` `z_sub_thr` | ~900k–913k msg/s, `--threads 4` | the real `zenoh-rust` subscriber's receive path outperforms this project's own `z_sub_thr` example |
| This project's `z_pub_thr --batch 50` → this project's `z_sub_thr` | ~675k–680k msg/s, `--threads 4` | limited by this project's own single-threaded subscriber example, not the broker (see below) |

At this point **the broker is no longer the bottleneck for this benchmark shape**:
`ps` during a sustained run shows the *subscriber* process pinned at ~100% CPU on
its one thread, while `zenohb` has headroom to spare across its thread pool.
Getting past ~900k msg/s end-to-end from here requires speeding up the
single-threaded client's own receive/decode/dispatch path (`zenoh.session`), which
is a different component with its own review scope, not this broker.

**There is no memory leak** in either the old or new code. What looked like
unbounded RSS growth while chasing the original bug was an ASan quarantine/redzone
artifact under sustained high-frequency allocation (confirmed: `leaks` refuses to
even inspect an ASan-instrumented process; a Release-build broker's RSS stayed
flat processing 3M+ messages from a single tight-loop publisher, both before and
after the throughput fixes below).

### What changed

- **Congestion-based backpressure replaces close-on-overflow.** `FaceHandle` (in
  `zenoh.broker.tables`) carries a `std::shared_ptr<std::atomic<bool>> congested`,
  owned by the `Face` and set from the `Face`'s own strand once `tx_queue_` crosses
  a high watermark (65536 frames), cleared once drained back below a low watermark
  (16384) — the same hysteresis shape as `pending_routing_jobs`/
  `throttle_if_backlogged` above, just for a different queue. `Tables::route_push`
  and the `on_request` fan-out loop skip (drop) delivery to a congested target
  *before* ever calling `deliver()`, rather than queuing further or closing the
  face. **This means a genuinely reliable-marked Push (`FrameHeader::reliability`
  is always `Reliability::reliable` for Push) can still be silently dropped for
  one specific slow consumer under sustained congestion** — a deliberate v1
  trade-off (a slow consumer stays connected and catches back up once it can, but
  loses some deliveries while congested, rather than the previous "kill the
  connection outright" behavior, and rather than blocking every *other*, faster
  consumer or the producer itself to accommodate one slow one). No retransmission
  or true flow-controlled blocking is implemented; that would require decoding a
  per-message congestion-control bit this project doesn't currently read.
- **Outbound write coalescing.** `Face::pump_tx()` used to issue one
  `asio::async_write` per queued frame — one TCP segment, one syscall, one epoll
  round-trip per message, confirmed as the dominant per-message cost once the
  close-on-overflow bug stopped masking it. It now gathers every currently-queued
  frame into one scatter-gather `async_write` (a `std::vector<asio::const_buffer>`
  referencing `tx_queue_`'s own elements — safe because `std::deque::push_back`
  never invalidates references to existing elements, so more frames can queue up
  while a coalesced write is in flight without disturbing it) and pops them all on
  one completion.
- **Exact-literal-match fast path.** `ResourceTable::matching_subscribers`/
  `matching_queryables` used to call `zenoh.ke::intersects` (several heap
  allocations per call, even for a trivial two-chunk key) against *every* declared
  resource, every message. They now try an O(1) hash lookup for an exact match
  first (`by_key_` gained a transparent hash/equality so a `std::string_view` can
  probe it without constructing a temporary `std::string`), skipping `intersects`
  entirely for the common case of a publish landing on a subscription declared on
  that exact key, and only falling back to the general wildcard scan for every
  *other* resource.
- **Batched routing-strand posts.** `Face::dispatch_frame_body` used to
  `asio::post` each decoded Push to `Tables::strand()` individually. It now
  accumulates every consecutive Push decoded from one inbound frame into a batch
  and posts the whole batch once (`Tables::on_push_batch`), amortizing the
  Face→Tables `asio::post` hop (a heap-allocated handler node plus a cross-strand
  wakeup) over the batch instead of paying it per message. The batch is flushed
  before any *other* message type in the same frame is handled, so relative
  ordering against interleaved Declare/Request/Response traffic within one frame is
  preserved exactly — only consecutive Pushes are ever batched together. (A real,
  narrow bug was caught and fixed here during review: several early-return paths
  in the decode loop — a malformed trailing message, a decode failure mid-run —
  used to discard an already-accumulated, not-yet-posted batch instead of flushing
  it first, silently losing otherwise-valid Pushes decoded earlier in the same
  frame. `tests/test_broker.cpp` has a dedicated multi-Push-per-frame case, via the
  client `Batch` API, covering the batching path itself — no test yet covers the
  specific malformed-trailing-message scenario the bug required, which remains a
  documented coverage gap.)
- **`asio::recycling_allocator` on the per-message `asio::post` calls.** Both
  `Face::post_to_tables` and the delivery closure `build_handle()` builds now bind
  a recycling allocator (the same mechanism ASIO's own strand/coroutine-frame
  machinery already uses internally) instead of the default `::operator new`/
  `delete` per post — a per-thread free list for a handler shape that repeats at
  message rate.
- **Minor allocation cleanups**: `RoutedPush::key` is move- rather than
  copy-constructed; `ResourceTable::face_count_for` uses the same heterogeneous
  lookup as the matching fast path instead of constructing a temporary
  `std::string`.

### Reproducing the numbers

```sh
./build/clang-release/zenohb -l tcp/127.0.0.1:7447 --threads 4 &
./build/clang-release/examples/z_sub_thr -e tcp/127.0.0.1:7447 -s 8 -n 500000 &
./build/clang-release/examples/z_pub_thr -e tcp/127.0.0.1:7447 --batch 50 8
```

`--batch` (this project's `z_pub_thr` only — the reference has no equivalent flag)
matters more than expected: throughput peaks around `--batch 50`–`64` and is
*lower* at much larger batch sizes (e.g. `--batch 256`) or much smaller ones (e.g.
`--batch 20`) in informal testing here — the exact mechanism wasn't fully
characterized (plausibly an interaction between how much work accumulates per
inbound frame and per-strand scheduling fairness under `--threads > 1`) and is
worth profiling further if you're chasing the last bit of throughput. `--threads`
scaling also inverted from the pre-fix advice: with the close-on-overflow bug
present, more threads made things *worse* for a single pub/sub pair (more
cross-thread wakeup jitter around a bug that was already trigger-happy); with it
fixed, `--threads 2`–`6` all measurably outperform `--threads 1` for this
benchmark shape, plateauing somewhere in that range on an 8-core test machine.

## Testing

`tests/test_broker.cpp` (part of the single `zenoh-tests` binary — see `CLAUDE.md`)
spins a real `zenoh::broker::Broker` on loopback port 0, driven by real
`zenoh::Session` clients over the actual wire — the inverted `FakeRouter`/`SubRouter`
pattern (`docs/RUNTIME.md`'s test harnesses have an in-process router drive a real
`Session`; here a real `Session` drives a real, in-process `Broker`). Covers: Push
fan-out (matching incl. `**`, and proving a non-matcher never receives anything),
undeclare cleanup, all three `QueryTarget` variants, zero-queryable `get()`
termination, the `Err` reply path, `target_zid` filter-not-bypass proofs on both
`put`/`get` (including a zid matching no live peer at all), disconnect-mid-query
cleanup (verified via strand-marshaled `Tables` introspection accessors — reading
them directly from the test thread would race the routing strand exactly like
production code would), and multi-threaded concurrency-stress cases (both pub/sub and
query/reply) run under the `linux-tsan` preset as the actual thread-safety gate.

# Subscriber design (C++23 runtime)

Adds **subscriber** support to the client `Session` (PLAN.md D8 vertically-integrated
runtime). Produced by an architect proposal + an expert systems-C++ review, then
refined in design discussion. This is the design record; status tracked at the bottom.

## North star: an Asio-style dispatch model

The dispatch loop is the one primitive. A **subscriber is declared with a handler**
(callback); the loop decodes each incoming network message and routes `Push(Put|Del)`
to the matching subscriber's handler. Pull-style delivery is *not* a separate
mechanism — it is a built-in **FIFO handler** layered on top (this is how `z_sub` stays
a simple `while (recv()) {…}` loop).

Concurrency is dialed by **how many threads pump the loop**, exactly like
`io_context::run()`:

- `Session::run()` — pump until stopped (blocks). Call from *n* threads for *n*-way
  concurrent dispatch.
- `Session::run_once()` — process at most one unit of work (one batch decode and/or one
  ready strand), then return. For user-paced single-threaded loops.

**The single TCP socket is the one serialization point:** framing is sequential, so
**decode is a critical section** — only one thread decodes at a time (an internal "I/O
strand"). Parallelism is in *handler execution*, fanned out across subscribers.

## Strands = per-subscriber FIFO + cross-subscriber concurrency

Each `Subscriber` is a **strand**: a queue + an atomic owner flag. `post(sample)`
enqueues; if the strand is idle the posting thread drains it in FIFO order then
releases, else the current owner drains it. Handlers on one strand never run
concurrently and never reorder, across any number of pump threads; different strands
run fully in parallel.

- **1 pump thread:** every post drains inline, in order — strands never contend.
- **n pump threads:** idle strands grabbed by free threads; a busy strand just queues.

### Bounded, optionally conflating strands

Each strand has a **fixed capacity `N`** (set at `declare_subscriber`) and one of two
modes:

- **ordered** (default): bounded FIFO; on full → **block** the decoder (TCP
  backpressure to the router). Bounded-by-construction — this is also what closes the
  reviewer's unbounded-queue DoS.
- **last_value** (conflating): a FIFO that allows duplicate keys while not full (full
  fidelity when the consumer keeps up). On arrival of `(key, v)` when **full**:
  - key already pending → take its **most-recent** node, overwrite the value, and
    **splice that node to the tail**;
  - key not pending → **block** (nothing to conflate).

  Conflation targets the most-recent occurrence and re-tails it so delivery is always
  an **ordered subsequence of the arrival stream** — same order, with some intermediate
  same-key values dropped only under backpressure. (Replacing an *earlier* occurrence
  would deliver a newer value before an older one = per-key out of order; replacing
  *in place* at the old slot would deliver it before later-arrived other-key entries =
  cross-key out of order. Overwrite-most-recent-and-tail avoids both.) A `Del`
  conflates the same way (it's the newest event for that key).

  Structure: doubly-linked list (the FIFO) + `unordered_map<key, node*>` (key →
  most-recent node), maintained on append (`map[k]=node`) and head-pop
  (`if popped==map[k] erase`). All ops O(1). A short per-strand lock guards the
  structure (producer = decode thread, consumer = a pump thread → genuinely MPSC).

## Receive path

`recv_batch` (existing) → `FrameHeader::decode` → `while (remaining>0)` peek msg-id and
dispatch: `Push`→decode→owned `Sample`→post to the subscriber's strand;
`Declare{DeclareKeyExpr}`→update resmap; other decodable msgs ignored; **any
`CodecError` mid-loop faults the session permanently** (sticky terminal fault — a byte
stream can't resync; every later `run`/`recv` drains what's queued then returns
`protocol_error`).

The borrow-only decoded `Put`/`Del` (PLAN.md D2) are views into the reused rx buffer,
so the **copy into the owned `Sample` happens at post time**, before the next batch
overwrites the buffer. `Sample` = `{ std::string key, std::vector<std::byte> payload,
SampleKind kind }` for the first slice (encoding/attachment/timestamp deferred; when
added they must be **owned**, never the borrow-holding proto structs).

### Keyexpr resolution (resmap)

No local keyexpr matcher exists (Phase 2). The router may push using a numeric keyexpr
id bound by a router→client `DeclareKeyExpr`. `resmap_` = `unordered_map<u16, string>`,
**bounded** (cap entries + cap key length; re-declare replaces; `UndeclareKeyExpr`
evicts). `resolve_key(WireExpr)`: `scope==0`→suffix; `scope==id`→`find(id)` (never
`operator[]`) + residual suffix, length-bounded + UTF-8-checked; missing id →
`protocol_error`.

### Routing policy (first cut)

No local matching, so **one subscriber per session**, delivering every forwarded
`Push` to it (the router already matched). Multiple subscribers per session needs the
Phase-2 keyexpr matcher; documented limitation. (A second `declare_subscriber` →
`already_subscribed`.)

## Keepalive / lease

A keepalive is a timer driven from the pump (Asio: timers fire only while `run()`
runs). While ≥1 thread is pumping, the I/O wait uses `poll_readable(lease/4)`; on
timeout it sends an SN-less `KeepAlive`. The **negotiated lease** (captured from the
handshake) drives the cadence. If the user stops pumping for > lease/2 they must call an
explicit `keepalive()` or accept being dropped — exactly the Asio "no run(), no timers"
contract.

## TX/RX, threading, teardown

One socket; the socket stays non-blocking; blocking paths poll EAGAIN. The single
`frame_sn_` counter carries declares + puts + the existing batch path; `KeepAlive`/
`Close` are SN-less and interleave safely. Multi-thread pumping requires a mutex around
socket writes + `frame_sn_` (added with `run()`); `put()` and pumping from different
threads is serialized by it. `Subscriber` holds a non-owning `Session*` (same contract
as `Batch`: session must outlive it; nulls its pointer on move). `~Subscriber`/
`undeclare()` sends `Frame(Declare{UndeclareSubscriber{id}})` best-effort; inert if the
session is already closed.

## Module / file layout

Extend `zenoh.session` (shares socket, SN, framing, rx/resmap state — like `Batch`).
One new `TcpLink` method: `poll_readable(timeout_ms)`. New `examples/z_sub.cpp` on the
FIFO handler. No new modules.

## Testing (every new feature) + interop

- **Unit (strand)**: ordered FIFO + block-on-full; last_value conflation
  (overwrite-most-recent + tail), the ordered-subsequence property, block-when-absent,
  `Del` conflation, capacity bound.
- **Unit (resolve_key/resmap)**: scope==0, scope==id + residual, missing-id error,
  eviction, bounds.
- **Unit (Sample ownership)**: decode → Sample, overwrite source buffer, Sample intact.
- **Integration (extend `FakeRouter`)**: router pushes Frames of Put/Del (and a
  `DeclareKeyExpr` then an id-based Push); assert handler receives the right
  key/payload/kind in order; multi-thread `run()` concurrency; EOF→`connection_closed`;
  sticky fault on a malformed frame; keepalive emitted on idle (poll-timeout).
- **Interop vs `../zenoh-rust`** (the gating step 0 first, then end-to-end):
  `zenohd` (release) + reference `z_pub`/`z_put` → C++ `z_sub` prints each sample;
  also C++ pub → reference `z_sub` still green (no regression).

## Build order

0. **Wire spike** — prove a bare `DeclareSubscriber` makes `zenohd` forward. *Gating.*
1. `TcpLink::poll_readable`.
2. Strand (bounded ordered + last_value conflate-to-tail) + unit tests.
3. `Sample` + `resolve_key`/resmap (hardened) + unit tests.
4. Dispatch loop + `run()`/`run_once()` + keepalive + sticky fault; `FakeRouter` RX
   tests.
5. `Subscriber` + `declare_subscriber(key, handler, {N, mode})` + FIFO-handler adapter
   + undeclare/teardown.
6. `examples/z_sub.cpp` + CMake + interop vs reference; `docs/RUNTIME.md` update.

## Smallest first slice

Single-subscriber, single-thread `run_once()` loop delivering PUT (key+payload+kind)
via a FIFO handler, with keepalive and bounded strands, interoperating with reference
`z_pub`/`z_put`. Defer encoding/attachment/timestamp, multi-thread tuning, and
multi-subscriber routing.

## Status

- **Step 0 (wire spike) — DONE, assumption confirmed.** A bare
  `Frame(Declare{DeclareSubscriber{scope:0, suffix:key}})` (no `Interest`) makes
  `zenohd` forward matching Push data. Observations folded into the design:
  - The router pushes the **literal suffix with `scope=0`**, not a numeric keyexpr id,
    so the resmap is *defensive* (keep it bounded/hardened, but it is not on the basic
    path).
  - The router's **frame SN is arbitrary** (not 0) — RX SN is track-don't-validate;
    we trust TCP ordering.
  - The `demo/example/**` subscription matched concrete keys — router-side matching is
    authoritative; deliver-all to the single subscriber is correct.
- **Steps 1–6 — DONE.** Implemented and green:
  - `TcpLink::poll_readable(timeout_ms)` (keepalive timer seam) + tests
    (`test_tcp.cpp`). Caught a real bug: `POLLIN` must take priority over `POLLHUP` so a
    half-closed peer's buffered data is read before EOF is surfaced.
  - `Strand<T>` (`src/runtime/strand.cppm`) — bounded `ordered` + `last_value`
    conflate-to-tail; 8 unit tests (`test_strand.cpp`).
  - `Sample`, `resolve_key`/bounded resmap, the `dispatch_cursor`/`pump_step` decode
    loop, `run()`/`run_once()`, `Subscriber::recv()`, sticky fault, keepalive cadence
    (lease/4), `undeclare`/teardown — all in `zenoh.session`.
  - `examples/z_sub.cpp` (pull-based `recv` loop, mirrors `z_sub.rs`).
  - **Tests:** 114 total green (incl. 11 subscriber integration tests over an in-process
    `SubRouter`: ordered delivery, DELETE, callback `run_once`, resmap, backpressure
    cursor-resume, last-value conflation, sticky fault, EOF, already-subscribed,
    keepalive), stable across repeated runs, ASan/UBSan clean.
  - **Interop vs `../zenoh-rust`:** C++ `z_sub` receives PUT (from `z_put`/`z_pub`) and
    DELETE (from `z_delete`) through real `zenohd`; C++ publish → reference `z_sub` still
    green (no regression).
- **Deferred (as planned):** encoding/attachment/timestamp on `Sample`; true
  multi-thread `run()` fan-out (meaningful only with multi-subscriber routing, which
  needs the Phase-2 keyexpr matcher); slowloris mid-batch read bound.

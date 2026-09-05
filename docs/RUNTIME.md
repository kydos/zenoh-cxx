# zenoh runtime (C++23) — client session

The `zenoh` library is the user-facing runtime, layered on the I/O-free `zenoh.proto`
codec. Its first slice is a **client** `Session`: a TCP transport to a Zenoh router
plus `put` / `try_put`. It follows the vertically-integrated design (PLAN.md D8) — the
session owns the socket, the protocol state, and the encode buffers, and drives
`encode → send` directly, with no sans-IO state machine in between.

A client communicates **only through a router** (`zenohd`); there is no scouting or
peer-to-peer layer here. Verified interoperable against the Rust reference router and
subscriber (checked out as a sibling of this repo's root — `../../zenoh-rust` from a
worktree such as `zenoh-cxx/main`).

## Module map

| Module | Unit | Contents |
| --- | --- | --- |
| `zenoh.runtime.tcp` | `src/runtime/tcp.{cppm,cpp}` | `TcpLink` (RAII POSIX socket), `IoError`. Blocking `write_all`/`writev_all`/`read_exact` (handshake only), non-blocking `write_some`/`read_some`, `poll_readable` (keepalive timer). POSIX headers stay in the `.cpp`. |
| `zenoh.runtime.strand` | `src/runtime/strand.cppm` | `Strand<T>` — the per-subscriber bounded queue (`ordered` / `last_value` conflation), `StrandMode`. Header-only template. |
| `zenoh.session` | `src/runtime/session.{cppm,cpp}` | `Session`, `ZError`, `Sample`, `Publisher`, `Subscriber`, `Queryable`/`IncomingQuery`, `Getter`/`GetReply`, `Computation`/`Eval`, `Evaluator`, `Batch`, `PeerId`, and the option structs (`PutOptions`, `PublisherOptions`, `GetOptions`, `SubscriberOptions`, `QueryableOptions`, `ComputationOptions`, `EvalOptions`). Endpoint parsing, the 4-way handshake, `put`/`try_put`/`batch`/`get`/`eval`/`close`, `declare_publisher`, and the receive pump (`declare_subscriber`/`declare_queryable`/`declare_computation`, `run`/`run_once`, `Subscriber::recv`). |
| `zenoh` | `src/zenoh.cppm` | Public umbrella; re-exports `zenoh.session`. **Import this for the client API.** |

## Connecting (the handshake)

`Session::open("tcp/127.0.0.1:7447")` performs the unicast establishment as the
opener/client:

1. **InitSyn** — `version = 9`, `whatami = client`, a fresh random 16-byte `zid`.
   Default SN/request resolution and batch size, so the `S` section is omitted; no
   QoS/patch/SHM extensions (all optional — the router fills defaults).
2. **InitAck** — the router's reply; we keep the **cookie** to echo back and adopt the
   router's advertised **batch size** (an MTU below `u16::MAX`, e.g. 65328) as the
   ceiling for everything we send — length prefix included.
3. **OpenSyn** — `lease = 10 s`, `initial_sn = 0`, cookie echoed. `initial_sn` is
   exchanged in the handshake (not assumed), so declaring 0 and starting frames there
   is sufficient; the router adopts it as the expected baseline.
4. **OpenAck** — completes the handshake.

The socket is **blocking during the handshake** (request/response), then switched to
**non-blocking** for the data phase so `try_put` can detect backpressure.

## Receiving: never block inside a batch

`pump_step` reads with `TcpLink::read_some` and keeps a partially-arrived batch on the
session (`rx_hdr_`/`rx_need_`/`rx_fill_`), so it can return to its caller and resume
later. That matters because the alternative — two blocking `read_exact`s — commits the
session to waiting for the whole body the moment the 2-byte length prefix is consumed:
a peer that announces 100 bytes, sends 5 and stalls used to freeze the pump, past any
`get()` deadline and with no keepalive going out, until it eventually closed. Timing
out and *discarding* the partial read is not an alternative — those bytes are gone from
the socket, so the byte stream would desynchronize. `read_exact` remains, for the
handshake, which is blocking by design.

A full subscriber/queryable strand pauses the dispatch cursor rather than dropping the
message, and `dispatch_cursor` reports that it stalled. Any pump caller then drains
*callback-backed* registrations (`drain_handlers`) and retries, because those need no
application call to drain: without that, a callback subscriber whose queue filled would
head-of-line block the shared cursor for everyone, and a `get()` pumping for its reply
would spin re-decoding the same undeliverable sample until it timed out with the reply
already in the buffer. A **pull**-style strand nobody is draining still blocks the
cursor — that is inherent to one cursor plus bounded queues — but the pump paces itself
instead of spinning, and the application calling `recv()` is what unblocks it.

Every terminal error is reported *after* whatever the same pump already delivered:
`sub_recv`/`qbl_recv`/`run_once` hand over queued messages first and surface the fault
once the queue drains. A `Close` packed in behind a frame is the routine case.

## TCP framing

Each TCP batch is prefixed by a **2-byte little-endian length** covering the bytes
that follow (per-batch, max 65535). A published sample is one batch:

```
[u16 LE len] [FrameHeader 0x05] [Push 0x1d | M | N] [WireExpr] [Put 0x01] [payload]
```

A batch is a **sequence of transport messages**, not a single one, and this matters
on receive: the reference appends a `KeepAlive` (or a `Close`, or a fresh
`FrameHeader`) to whichever batch is currently staging — including one that already
carries a frame — so `[Frame][Push][KeepAlive]` and `[KeepAlive][Frame][Push]` are
both ordinary traffic from a real `zenohd`. A frame's body therefore ends at the
first id that is **not** a network message rather than at the end of the batch. The
two id spaces are disjoint by design for exactly this purpose — network `0x19..0x1f`,
transport `0x00..0x07` — which is what `zenoh::is_network_mid` tests; zenoh-rust
reaches the same point from the other side, by decoding a network message and
rewinding its reader when that fails. `Session::dispatch_cursor` walks both kinds in
one loop, so an id it cannot length-skip is still a sticky `protocol_error`, but a
transport message riding behind a frame is not.

- `FrameHeader`: reliable (R flag set), per-channel SN starting at `initial_sn` and
  incrementing by 1 (mod the U32 resolution mask `0x0FFFFFFF`).
- `Push`: literal `WireExpr{scope:0, mapping:Sender, suffix:key}` — a basic put needs
  **no prior key-expression declaration**. QoS/nodeid are defaults, so elided.
- `Put`: defaults (no timestamp/encoding/attachment), payload length-prefixed.

## `put` vs `try_put`

Both publish `Frame(Push(Put))`. They differ only in how they treat a transport that
cannot accept the bytes right now:

- **`put`** blocks until the whole message is handed to the transport (polling
  `POLLOUT` on `EAGAIN`). Any bytes a prior `try_put` left buffered are flushed first.
  It encodes only the frame header into a staging buffer and writes it together with
  the borrowed payload via `writev` scatter-gather, so the payload is never copied
  (see `Put::encode_head` / `Push::encode_head` in the codec).
- **`try_put`** never blocks. It returns `ZError::would_block` only when the socket
  could not accept *any* bytes of the message (nothing sent), leaving the decision
  (drop, retry, back off) to the caller.

`try_put` keeps the byte stream intact under backpressure:

- The frame's **SN is consumed only once the frame is committed** (fully sent, or
  partially sent with the tail buffered). If nothing goes out, the SN is untouched and
  the next call re-sends the same frame number — no desync.
- On a partial write, the unsent tail is buffered and `try_put` returns **success**:
  the message is committed (not lost) and the tail is flushed, preserving wire order,
  on the next `put`/`try_put` before any new frame.
- A pending tail blocks new frames: `try_put` first tries to flush it and returns
  `would_block` if it still cannot drain at all, rather than interleaving a new frame
  ahead.

## Congestion control and zid-targeting (`PutOptions` / `GetOptions`)

`put`, `try_put`, `Batch::put` and `get` take an options struct rather than a growing
list of trailing parameters:

```cpp
session->put("cmd/arm", payload, {.congestion = CongestionControl::block});
session->put("telemetry/x", payload, {.target_zid = peer});   // congestion defaults to drop
session->get("sensors/**", "", {.target = GetTarget::all,
                                .congestion = CongestionControl::block});
```

- **`congestion`** is the standard Zenoh `CongestionControl`, carried as bit 3 ("D")
  of the QoS extension that `Push`/`Request` already have — not a project-local flag,
  so a `zenohd` or `zenohb` on the other end honours it identically, and a peer that
  ignores QoS is unaffected.
  - `drop` (the default) lets a router discard the message rather than let one slow
    consumer stall the producer. Right for telemetry and anything the next message
    supersedes.
  - `block` says the message must not be discarded: the router queues it past the
    point where it would drop and pushes back on the producer instead. Right for
    commands, configuration, and anything a receiver cannot reconstruct later. The
    cost is that the backpressure slows this publisher toward *every* destination,
    not just the congested one — which is why it is per message rather than a
    session-wide mode. See `docs/CLIQUE.md`'s "Congestion control is per message" for
    what the broker actually does with it.
- **`target_zid`** narrows delivery to the one peer with that Zenoh id, ANDed with
  normal key-expression matching — a filter, never a bypass (`docs/BROKER.md`). It
  works across a broker clique: the filter is applied by whichever broker owns that
  peer.

Priority (QoS bits 2:0) and express (bit 4) are **not** exposed — by `PutOptions`,
`GetOptions` or `PublisherOptions`: honouring them needs per-priority frame SNs and
queues, and this runtime has a single `frame_sn_` that always writes
`Reliability::reliable`.

## Batching

`Session::batch()` returns a `Batch` that coalesces many puts into **one Frame** (one
FrameHeader, one SN, the `Push` messages concatenated) sent as a single TCP batch —
exactly how zenoh-rust batches. This amortizes the per-message frame header and the
syscall across many puts.

```cpp
auto b = session->batch();
b.put("demo/a", data1);
b.put("demo/b", data2);
b.flush();              // or let the Batch go out of scope
```

- A `Batch::put` appends a `Push(Put)` to the in-progress frame, and takes the same
  `PutOptions` as `Session::put`.
- When the next put would exceed the negotiated batch size, the buffered frame is
  **sent first** (blocking) and the new put begins the next frame — so an arbitrarily
  long run of puts streams out as a sequence of full frames.
- `flush()` sends whatever is buffered; the **destructor flushes** too (best-effort —
  call `flush()` explicitly if you need to observe the result).
- A single put too large to fit one frame on its own returns `ZError::encode_error`.
- `put`/`flush` block like `Session::put`. The batch routes through the session's SN
  counter and pending-buffer, so interleaving `session->put()` and batch puts stays
  wire-consistent. The session must outlive any batch created from it.

## Publishers (declared key expressions)

`Session::declare_publisher("demo/example/some/long/key")` returns a `Publisher`
bound to one key expression and one fixed set of publication settings. Its `put` is
the same `Frame(Push(Put))` a bare `Session::put` sends, with the one difference that
is the whole point of declaring a publisher: the `Push` carries the key expression's
**numeric id** rather than its text.

```cpp
auto pub = session->declare_publisher("demo/example/some/long/key");
pub->put(payload);        // blocking, like Session::put
pub->try_put(payload);    // non-blocking, like Session::try_put
pub->del();               // Push(Del) — subscribers see SampleKind::del
pub->undeclare();         // or let the Publisher go out of scope
```

Declaring one sends two messages, matching what a `zenoh-rust` publisher sends:

1. `Frame(Declare{DeclareKeyExpr{id, key_expr}})` — binds a session-local numeric id
   to the key expression, spelled out in full since this is what defines the id.
2. `Frame(Interest{CurrentFuture, KEYEXPRS|SUBSCRIBERS, id})` — the reference's
   writer-side matching-status query. This runtime has no `matching_status()` to feed
   (see below), but the message is what a real publisher puts on the wire, so it is
   sent for fidelity. `zenohb` decodes and deliberately does not answer it
   (`docs/BROKER.md`); `zenohd` *does* answer, which has one consequence worth
   knowing.

   **A publisher-only session accumulates unread inbound bytes against `zenohd`.**
   Having asked to be told about matching subscribers, the session is sent a
   declaration every time one appears or disappears — and an application that only
   publishes never pumps, so those bytes sit in the socket receive queue (measured:
   258 bytes after six subscriber connect/disconnect cycles, ~43 bytes per event).
   Publishing is unaffected, and the queue only grows with *subscriber churn*, not
   with traffic; but a very long-lived publisher facing thousands of churn events
   will eventually fill the receive buffer and push back on the router. Call
   `run_once()` occasionally from such a loop, or drop the publisher and use
   `Session::put` if the key expression is published rarely enough not to need an id.

Afterwards every publication is a `Push` with `wire_expr = {scope: id, suffix: ""}`,
so a thirty-character key expression costs one or two bytes per message. The router
resolves it through the per-face id→key map it built from the declaration — the same
mechanism this session's own receive path uses for router-declared ids.

- **Ids are refcounted per key expression.** Two publishers on the same key share one
  `DeclareKeyExpr`, and the `UndeclareKeyExpr` goes out only when the last one is
  undeclared (mirroring the reference's `local_resources`).
- **Undeclaring** sends `InterestFinal{id}` and then, if it was the last holder,
  `Frame(Declare{UndeclareKeyExpr{id}})`. `undeclare()` is idempotent and the
  destructor runs it; publishing through an undeclared handle returns
  `connection_closed`. As with `Subscriber`/`Queryable`, undeclare *before*
  `Session::close()` if the wire message needs to actually reach the peer.
- **There is no per-session limit** on publishers (unlike the one-subscriber and
  one-queryable cuts): a publisher holds no receive-side state.
- **A publisher that cannot get an id still works.** The session declines to bind more
  than 4096 ids — the cap both `zenohb` and this session's own receive path put on
  their id→key maps, past which a router silently forgets the declaration and would
  leave every later `Push` unresolvable. Past it, `keyexpr_id()` is 0 and the
  publisher sends the key expression in full, exactly as `Session::put` does.
- **QoS is fixed at declaration** (`PublisherOptions{target_zid, congestion}`) rather
  than passed per publication, as in the reference. The two knobs mean exactly what
  they mean for `PutOptions`.

Not modeled from the reference's `Publisher`, consistent with the rest of this
runtime's scope: per-publication encoding, attachments and timestamps; priority and
express QoS; and `matching_status()`/`matching_listener()`, which would need the
router to answer the publisher's `Interest` with the declarations it matches — v1
routers here decode that query and deliberately do not reply (`docs/BROKER.md`).

## Subscribing

`Session::declare_subscriber("demo/example/**")` sends a
`Frame(Declare{DeclareSubscriber})` (reliable, on the session's frame SN) and returns a
`Subscriber`. A bare `DeclareSubscriber` is sufficient for the router to start
forwarding matching data — no `Interest` exchange is needed (verified against
`zenohd`). First cut: **one subscriber per session** (a second returns
`already_subscribed`); the router does the key-expression matching, so every forwarded
sample is delivered to the subscriber.

Two ways to consume, both driving the same decode pump (modeled on `io_context::run`):

- **Pull** — `Subscriber::recv() -> expected<Sample, ZError>` blocks until the next
  sample, pumping the receive loop itself (this is what `z_sub` uses):
  ```cpp
  auto sub = session->declare_subscriber("demo/example/**");
  while (auto s = sub->recv()) {
      // s->key_expr(), s->payload(), s->kind()  (SampleKind::put / del)
  }
  ```
- **Callback** — `declare_subscriber(key, handler)` registers a
  `void(const Sample&)`; `Session::run()` (loop) or `Session::run_once()` (one step)
  invoke it for each sample. The number of threads pumping `run()` is the dispatch
  concurrency (single-threaded in this cut).

A `Sample` is an **owned** snapshot (key string + payload bytes + kind), copied out of
the receive buffer at delivery time — it outlives the next `recv`/`run`.

### Strands (per-subscriber buffering)

Each subscriber has a bounded **strand** (`SubscriberOptions{capacity, mode}`):

- `StrandMode::ordered` (default) — bounded FIFO; when full, the decoder applies
  backpressure (it pauses mid-frame and resumes as the consumer drains — never drops).
- `StrandMode::last_value` — under backpressure, conflates by key: a new value whose key
  is already pending overwrites that key's most-recent entry and re-tails it, so delivery
  stays an **ordered subsequence** of the arrival stream (intermediate same-key values
  dropped only when full). A `Del` conflates the same way.

### Keepalive

A receive-only session is idle on TX, so the pump emits an SN-less `KeepAlive` whenever
it has waited `lease/4` (2.5 s) with no data — the client must stay in `recv`/`run` (or
call within the lease) to keep the session alive. Decode errors **fault the subscriber
permanently** (a byte stream can't resync): every later `recv`/`run` returns
`protocol_error`.

### `ZError`

`would_block` (try_put backpressure), `connection_closed` (EOF / router Close, and
publishing through an undeclared `Publisher`), `io_error`, `protocol_error` (malformed
handshake or data stream), `encode_error` (message exceeds a batch — including a key
expression too long for one), `bad_endpoint` (unparseable/unresolvable locator),
`already_subscribed` (second subscriber on a session), `invalid_key_expr`
(`declare_computation` on a wild or non-canonical key). `declare_publisher` and
`declare_evaluator` add no error of their own.

## Evaluation (`Computation` / `Evaluator`)

Evaluation is a second abstraction over the same Query/Reply transport, with
deliberately different — and stronger — semantics:

```
Querier(key expr)   --get(  )------->  Queryable(key expr)  -->  Reply
Evaluator(key expr) --eval(argument)->  Computation(key)     -->  Reply
```

A `Queryable` describes the ability to answer queries over a *region* of the key
space. A `Computation` is **one computation registered at one concrete key**, and
`eval` invokes **every** Computation whose key matches the evaluator's key expression:

```cpp
auto c1 = session->declare_computation("robot/r1/reset", [](zenoh::Eval e) {
    reset_robot("r1");
    (void)e.reply(as_bytes("ok"));          // keyed by "robot/r1/reset" automatically
});

auto replies = session->eval("robot/*/reset", as_bytes(""));   // r1, r2, r3 — all of them
while (auto r = replies->recv()) {
    if (!*r) break;                          // every computation finished
    // (*r)->sample().key_expr() says which computation produced this result
}
```

Nothing else in this runtime works that way, and that is the point. `get` may pick one
queryable (`GetTarget::best_matching`, its default); an eval never may. A computation
may *do* something — reset a robot, claim a job, acquire a lock, actuate hardware — so
"pick a matching one arbitrarily" is not a meaningful thing to do to `robot/*/reset`,
and neither is dropping replies to consolidate them. Every eval therefore uses
`QueryTarget::all` and `ConsolidationMode::none` internally, and **neither is
exposed**: they are the contract, not tuning knobs (`EvalOptions` carries only
`timeout_ms`, `target_zid` and `congestion`).

### The API

| | Declared | Undeclared |
| --- | --- | --- |
| Serve | `declare_computation(key[, handler][, opts])` → `Computation` | — |
| Invoke | `declare_evaluator(key_expr[, opts])` → `Evaluator`, then `evaluator->eval(argument)` | `session->eval(key_expr, argument[, opts])` |

- **A Computation must be declared on a concrete, canonical key** — `robot/r1/reset`,
  never `robot/*/reset` or `math/**`, which return `ZError::invalid_key_expr`. The
  wildcard belongs on the evaluator's side.
- **There is no per-session limit**, unlike `Subscriber`/`Queryable`: a session may
  declare any number of computations, including **two at the same key**, in which case
  both run on every matching eval (no deduplication by key — see the guarantees below).
- **`Computation` consumes evals like a `Queryable` consumes queries**: pull with
  `Computation::recv()`, or pass a `void(Eval)` handler and let `run()`/`run_once()`
  deliver. `ComputationOptions` carries only `capacity`; the strand is always
  `ordered`, because conflating away an eval that may have side effects is never right.
- **`Evaluator` is to `eval` what `Publisher` is to `put`**: it binds its key
  expression to a declared numeric id, so each eval sends the id instead of the text.
  `Session::eval` is the undeclared convenience form, exactly as `get` is for a query.
- **The argument is mandatory** (`eval(key_expr, argument)`, not
  `eval(key_expr).payload(argument)`): it is the *argument of a computation*, not data
  being stored. Pass an empty span for a computation that needs none. It travels as the
  `Query`'s value extension — the same field the reference's `get().payload(..)` uses.
- **An `Eval` never asks the callback for a key.** `eval.reply(value)` is keyed by that
  computation's own concrete key; `Query::reply` needs an explicit key only because an
  ordinary queryable may itself be declared on a wildcard. `Eval` also exposes
  `argument()`, `key_expr()` (the evaluator's key expression) and `computation_key()`
  (this computation's key), plus `reply_err(error)`. There is no `reply_del`: a
  deletion is a data-centric notion with no meaning as the result of a computation.
- **Replies are ordinary `GetReply`s** collected through an ordinary `Getter`, each
  keyed by the concrete key of the computation that produced it. A computation may send
  0..N ok replies and 0..N error replies; none of them are consolidated.

Not modeled, following this runtime's existing scope rather than by choice: an eval's
encoding and attachment (there is no public `Encoding` or attachment concept here at
all — the same gap `Publisher` has), priority/express QoS, the reference's
matching-status/matching-listener machinery, its `allowed_origin`/`allowed_destination`
(both are `Locality` filters, and locality has no meaning for a client that reaches
everything through a router — the zid filter `EvalOptions::target_zid` is a different
thing and *is* modeled), and `background()` (a handle-lifetime idiom with no C++
counterpart: a `Computation` handle is undeclared when it drops, like every other
handle here). `complete` is absent for a different reason: it is a data-query notion,
and a computation is not a query.

The types live in `zenoh.session` alongside `Queryable`/`Getter` rather than in a
module of their own: the computation registry is `Session` state, so a separate module
would have to reach back into it, and this codebase's module graph is one folder per
layer rather than one per abstraction. The separation the abstraction needs is in the
vocabulary and the semantics, not the build graph.

### Isolation from ordinary Query/Reply

An eval must never invoke an ordinary `Queryable`, and an ordinary `get` must never
invoke a `Computation` — including when both are declared on the very same key. Since
both ride the same Query/Reply messages, the two are told apart by key space: a
Computation is declared, queried and matched under a **reserved internal prefix**.

```
logical computation key      robot/r1/reset
internal wire key            @eval/robot/r1/reset

logical eval key expression  robot/*/reset
internal wire key expression @eval/robot/*/reset
```

Prefixing both sides identically preserves key-expression matching exactly
(`robot/*/reset` matches `robot/r1/reset` iff the prefixed forms match), so nothing
about the application's key expressions changes. Two things then keep the namespace
closed, and both are needed:

1. `@eval` is a **verbatim chunk** (`zenoh.ke`): a chunk beginning with `@` is matched
   only by an identical literal, never by `*` or `**` — so no *wildcard* an
   application writes can reach a computation, `get("**")` included. The name sits in
   Zenoh's reserved non-alphabetic-leading key space beside the reference's own
   `@adv`, and deliberately *not* inside the `@/...` admin space, which the broker
   refuses to route at all (`zenoh.broker.membership`'s `is_internal_key`).
2. The namespace is **reserved at the API boundary**: `Session::get` and
   `declare_queryable` reject a key expression whose first chunk is `@eval`
   (`ZError::invalid_key_expr`), and the Evaluation API rejects it too, so it cannot be
   nested inside itself. Without this, a caller who *typed* `get("@eval/robot/r1/reset")`
   would match a Computation's wire declaration exactly — matching alone cannot stop a
   literal. Only the query surface needs the guard: `put`/`declare_subscriber` route to
   subscribers, and a Computation is never one.

The second guard is an API-level reservation, not a protocol rule, so it binds this
implementation's clients and not other people's: a `zenoh-rust` client is free to
declare a queryable at `@eval/foo/a` and would then receive evals for it. That is the
same status the reference's own `@adv` namespace has — a convention among
implementations — and closing it properly would need the routing layer to distinguish
the two kinds of declaration on the wire, which is exactly what this API-only design
trades away.

The mapping is entirely private (`session.cpp`'s `eval_prefix` /
`to_eval_wire_key` / `from_eval_wire_key`) and never surfaces: `Eval::key_expr()`,
`Eval::computation_key()`, `Evaluator::key_expr()` and every reply key are the logical,
application-level ones. Because a reply's key is then disjoint from the request's, the
underlying query carries the `_anyke` selector parameter — the reference's
`ReplyKeyExpr::Any`, which it likewise transports as a parameter rather than a wire
field. Note where that is enforced: **not** in the router (neither `zenohd` nor
`zenohb` checks a reply's key against the query's), but in the *querying session* —
`zenoh/src/api/session.rs` drops a reply whose key does not intersect the query's
unless `_anyke` is set. So today it is inert, since both ends of an eval are this
implementation and this `Getter` does no such filtering; it is what makes the exchange
correct for a reference peer, not something a router run will exercise.

### Fan-out, and what is *not* guaranteed

One `Request` reaches a session however many of its computations match: the router
matches per *face*, so the per-registration fan-out is done client-side by key-expression
matching, and the request's single `ResponseFinal` goes out only once every `Eval` it
produced has been dropped. Undeclaring a computation with evals still queued releases
them the same way, so an evaluator is never left waiting on replies that are no longer
coming.

The guaranteed unit of fan-out is the matching **Computation registration**. The API
does **not** promise exactly-once execution, at-most-once execution across failures,
idempotence, transactionality, one execution per distinct key, or transparent
replication. Two computations at one key both run, and `eval` is never safe to retry
blindly.

## Example & manual interop test

Every example takes the same options as its `zenoh-rust` counterpart, so a command
line written for one runs against the other: `-h`/`--help` prints the full list, and
the reference's shared `CommonArgs` (`-e/--connect`, `-m/--mode`, `-c/--config`,
`--cfg`, `-l/--listen`, `--no-multicast-scouting`, `--enable-shm`) is recognized by
all of them (`examples/zexample.hpp`'s `parse_common`). Only `-e/--connect` maps onto
a capability this runtime has: `-m/--mode` accepts `client` and rejects `peer`/
`router` (this is a client-only implementation), and the rest parse and then print a
`note: ... has no effect` line on stderr rather than failing. Per-example options with
no runtime equivalent behave the same way — `z_pub -a/--attach`,
`z_pub --add-matching-listener`, `z_get -p/--payload`, `z_pub_thr --express`,
`z_pub_thr -p/--priority`, `z_ping/z_pong --no-express`,
`z_querier --add-matching-listener`. An option that is not a reference option at all
is rejected with `error: unknown option`.


`examples/z_sub.cpp` (`z_sub`) — the C++ equivalent of zenoh-rust's `z_sub`: declares a
subscriber and prints every received sample in a blocking `recv` loop.
`z_sub [OPTIONS]` with `-k/--key` (default `demo/example/**`). Verified against the
reference router + publishers:

```sh
zenohd -l tcp/127.0.0.1:7447 &
./build/clang/examples/z_sub -e tcp/127.0.0.1:7447 -k 'demo/example/**' &
z_put -e tcp/127.0.0.1:7447 -k demo/example/test -p hello   # from ../zenoh-rust
z_pub -e tcp/127.0.0.1:7447 -k demo/example/loop            # looping publisher
z_delete -e tcp/127.0.0.1:7447 -k demo/example/test         # delivered as DELETE
```

`examples/z_put.cpp` (`z_put`):
`z_put [OPTIONS] [endpoint] [key] [value]`. `-k/--key` and `-p/--payload` mirror the
reference; the three positional arguments and `--try`/`--batch`/`--count` are
extensions of this port. With `--batch` the puts are coalesced into API-level batches
(one Frame per batch).

`examples/z_pub.cpp` (`z_pub`) — the C++ equivalent of zenoh-rust's `z_pub`:
declares a `Publisher` on the key expression (as the reference does) and publishes
`"[<idx>] <payload>"` through it once a second, forever — so every message on the wire
carries the declared key-expression id rather than the text.
`z_pub [OPTIONS]` with `-k/--key` and `-p/--payload`. The reference also sets a
TEXT_PLAIN encoding; `put` carries no such metadata, and its `-a/--attach` and
`--add-matching-listener` parse but have no effect.

`examples/z_put_float.cpp` (`z_put_float`) — the C++ equivalent of zenoh-rust's
`z_put_float`: publishes a single `double`. The payload is the 8 little-endian bytes
of the value (`f64::to_le_bytes()`), the exact layout zenoh-ext's `z_serialize`
produces, so a reference subscriber can `z_deserialize::<f64>` it.
`z_put_float [OPTIONS]` with `-k/--key` and `-p/--payload`.

`examples/z_pub_thr.cpp` (`z_pub_thr`) — the C++ equivalent of zenoh-rust's
`z_pub_thr`: publishes a fixed-size payload to `test/thr` in a tight loop and, with
`-t`, prints `msg/s` every `-n N` messages.
`z_pub_thr [OPTIONS] <PAYLOAD_SIZE>` with `-t/--print` and `-n/--number`. Blocking
`put` matches the reference's `CongestionControl::Block`; its `--express` and
`-p/--priority` parse but have no effect. `--batch B`, which coalesces B puts per
flush (markedly higher throughput), is an extension with no reference counterpart. Measure with the reference `z_sub_thr`:

```sh
zenohd -l tcp/127.0.0.1:7447 &
z_sub_thr -m client -e tcp/127.0.0.1:7447 &        # from ../zenoh-rust
./build/clang/examples/z_pub_thr -e tcp/127.0.0.1:7447 -t 8            # single puts
./build/clang/examples/z_pub_thr -e tcp/127.0.0.1:7447 -t --batch 256 8  # batched
```

`examples/z_sub_thr.cpp` (`z_sub_thr`) — the C++ equivalent of zenoh-rust's
`z_sub_thr`: a callback subscriber on `test/thr` that counts messages in rounds of
`-n N`, prints `msg/s` per round, and exits after `-s M` rounds (printing a final
summary). `z_sub_thr [OPTIONS]` with `-s/--samples` and `-n/--number`. Driven by `Session::run()` (the
receive pump). Pair it with either publisher:

```sh
zenohd -l tcp/127.0.0.1:7447 &
./build/clang/examples/z_sub_thr -e tcp/127.0.0.1:7447 &
./build/clang/examples/z_pub_thr -e tcp/127.0.0.1:7447 8      # or zenoh-rust z_pub_thr
```

### Query/reply

`examples/z_get.cpp` (`z_get`) — the C++ equivalent of zenoh-rust's `z_get`: sends one
query and prints every reply until the query completes.
`z_get [OPTIONS]` with `-s/--selector`, `-t/--target` and `-o/--timeout`.
The selector is split at the first `?` into the key expression and the parameters
`get` takes separately. The reference's `-p/--payload` is not modeled: `get` sends a
key expression and parameters only (a queryable can *read* a query payload via
`IncomingQuery::payload()`, but the client cannot send one).

`examples/z_queryable.cpp` (`z_queryable`) — the C++ equivalent of zenoh-rust's
`z_queryable`: declares a queryable and answers every query with a fixed payload in a
blocking `recv` loop. `z_queryable [OPTIONS]` with `-k/--key`, `-p/--payload` and `--complete`.
Each `IncomingQuery` sends its `ResponseFinal` when it goes out of scope.

`examples/z_querier.cpp` (`z_querier`) — the C++ equivalent of zenoh-rust's
`z_querier`: the same query once a second, forever. Same flags as `z_get`. Two
differences, both runtime gaps: there is no `declare_querier` (a querier declared once
and reused, with a matching listener), so each iteration issues a plain `get()`; and
since a query cannot carry a payload, the iteration counter the reference sends as the
query payload appears only in this side's printed output.

```sh
zenohd -l tcp/127.0.0.1:7447 &                     # or this project's zenohb
./build/clang/examples/z_queryable -e tcp/127.0.0.1:7447 &
./build/clang/examples/z_get -e tcp/127.0.0.1:7447 -s 'demo/example/**'
z_get -e tcp/127.0.0.1:7447 -s 'demo/example/**'   # from ../zenoh-rust
z_queryable -e tcp/127.0.0.1:7447 &                # reference queryable, C++ getter
```

### Evaluation

`examples/z_computation.cpp` (`z_computation`) and `examples/z_eval.cpp` (`z_eval`) —
the two sides of the Evaluation abstraction above. These are the only examples with no
`zenoh-rust` counterpart (the abstraction is specific to this implementation), so their
CLIs mirror their closest siblings here — `z_queryable`'s and `z_get`'s — rather than a
reference binary's, and they take the same shared connection options as everything
else. `z_computation [OPTIONS]` with `-k/--key` (concrete: a wild key is rejected) and
`-p/--payload`; `z_eval [OPTIONS]` with `-k/--key` (any key expression), `-p/--payload`
(the argument, which is mandatory and so has a default), `-o/--timeout` and
`-d/--declare` (evaluate through a declared `Evaluator` — what `z_querier` is to
`z_get`). There is no `-t/--target` on `z_eval`: an eval always reaches every matching
registration.

```sh
zenohd -l tcp/127.0.0.1:7447 &                     # or this project's zenohb
./build/clang/examples/z_computation -k demo/example/r1 -p 'r1 done' &
./build/clang/examples/z_computation -k demo/example/r2 -p 'r2 done' &
./build/clang/examples/z_eval -k 'demo/example/*' -p go
# >> Received ('demo/example/r1': 'r1 done')
# >> Received ('demo/example/r2': 'r2 done')   -- both, always, never one of them
```

Both ends are this implementation's (there is no reference eval API to talk to), but
the *router* in between can be either: verified against a real `zenohd`, which is what
proves the `_anyke` reply-key handling. The isolation is worth checking there too — a
reference `z_get -s '**'` reaches a reference `z_queryable` on `demo/example/r1` and
never the C++ computation registered at that same key, while `z_eval` reaches only the
computation.

### Ping/pong (latency)

`examples/z_ping.cpp` (`z_ping`) and `examples/z_pong.cpp` (`z_pong`) — the C++
equivalents of zenoh-rust's `z_ping`/`z_pong`. `z_ping` publishes a `<payload_size>`
payload on `test/ping` and blocks for the echo on `test/pong`, timing each round trip;
`z_pong` subscribes to `test/ping` and republishes each payload on `test/pong`.
`z_ping [OPTIONS] <PAYLOAD_SIZE>` with `-n/--samples` and `-w/--warmup`;
`z_pong [OPTIONS]`. Because `put` blocks until the payload is written, it already
behaves like the reference's `CongestionControl::Block`; the reference's express-QoS
`--no-express` flag has no runtime equivalent and is not modeled.

```sh
zenohd -l tcp/127.0.0.1:7447 &                     # or this project's zenohb
./build/clang/examples/z_pong -e tcp/127.0.0.1:7447 &
./build/clang/examples/z_ping -e tcp/127.0.0.1:7447 -n 100 64
z_pong -e tcp/127.0.0.1:7447 &                     # from ../zenoh-rust: reference pong,
./build/clang/examples/z_ping -e tcp/127.0.0.1:7447 64   # C++ ping — and vice versa
```

`z_ping` and `z_pong` both declare a subscriber and publish on the same session, so
they exercise the full-duplex path (a blocking `put` interleaved with the receive
pump) that no other example covers.

```sh
# Router + reference subscriber (from ../zenoh-rust):
zenohd -l tcp/127.0.0.1:7447 &
z_sub -e tcp/127.0.0.1:7447 -k 'demo/example/**' &

# Publish from the C++ client:
./build/clang/examples/z_put tcp/127.0.0.1:7447 demo/example/test 'hello'
./build/clang/examples/z_put tcp/127.0.0.1:7447 demo/example/test 'hello' --try
./build/clang/examples/z_put tcp/127.0.0.1:7447 demo/example/burst msg --count 5
```

The subscriber prints each received `PUT`, confirming the handshake, the Put framing,
and (with `--count`) per-frame SN sequencing are all wire-compatible with zenoh-rust.

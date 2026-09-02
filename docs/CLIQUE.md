# Broker-to-broker federation (the clique)

`zenohb` started as a **single hub**: every face was a direct client connection, and
`docs/BROKER.md` listed "no scouting, no peer-to-peer, no multi-broker mesh" as a v1
gap. This document describes what replaced that gap — a **clique**: a set of brokers
each holding a direct link to every other, with applications remaining plain clients
of whichever broker is nearest to them.

Read `docs/BROKER.md` first. Everything there still holds — the two-tier strand model,
the resource table, `SharedBuf` fan-out, the borrow-only codec boundary. This is a
layer on top, not a replacement.

```
        client            client              client
           \                |                   /
            \               |                  /
          [broker A] === [broker B] === [broker C]
                \___________________________/
                    (every pair linked)
```

## The invariant

> **A message received on a router face is never sent to a router face.**

That is split horizon, and it is the entire loop-prevention story. It is enforced
structurally in `Tables`, from `FaceHandle::kind`, which is derived from the `whatami`
the peer announced during the transport handshake — not from a marker on the wire, and
not from a hop count.

Two consequences worth being explicit about:

- **It bounds every message to at most one inter-broker hop.** Routing therefore
  terminates even if the mesh is miswired into a cycle, which is a stronger guarantee
  than assuming the clique is correctly configured.
- **It applies to declarations as much as to data.** A `DeclareSubscriber` relayed
  from a peer is recorded locally and never re-announced, so each broker learns a key
  from the broker that actually owns the subscriber and from nowhere else.

### `whatami` is a claim, not a credential

The `whatami` a peer announces is unauthenticated, so being a clique peer is **opt-in
per broker**: an inbound face that announces `router` becomes a router face only when
the broker was started with `BrokerConfig::accept_router_faces`
(`zenohb --accept-router-faces`). Otherwise it is logged and treated as a client.
Outbound links are unaffected — this broker dialled those itself, from `--peer` or
from gossip.

That matters because a router face is not a label: it gets gossip ingestion, a replay
of this broker's declaration state, split-horizon treatment, and the much larger
router congestion budgets (16 MiB watermark, 256 MiB ceiling, against 1 MiB and 64 MiB
for a client). Any client that could claim it would get all of that.

**A mutual dial collapses to one connection, so exactly one end of every peer pair
sees the link as inbound.** A federation therefore needs `--accept-router-faces` on
the brokers that receive links — usually simplest to pass it to every broker in the
clique. Without it the symptom is quiet: the peer connects and is routed to as an
ordinary client, so the clique appears up but does not federate. The demotion is
logged to stderr for exactly that reason.

### Why this is equivalent to the reference, not an approximation of it

The Rust reference (`zenoh/src/net/routing/hat/router/`) routes between routers using
full link-state: it floods `LinkStateList` OAM, builds a Bellman-Ford spanning tree
per origin node, and forwards declarations only to `trees[origin].children`.

In a healthy clique with uniform link weights, that degenerates to exactly the rule
above. For the tree rooted at `S`, every other node's predecessor *is* `S`, so `S`'s
children are all the other routers (it floods each of them once) and every other
router has no children at all (it never re-forwards). The reference's own
`egress_filter` then drops the would-be echo back toward the source. So for the target
topology this implementation is wire-behaviour-equivalent to the reference — it simply
declines to build the machinery that would only matter in a topology it does not have.

### What is given up, deliberately

**A single dropped link partitions exactly that pair.** The reference would reroute
through an intermediate router; this does not. Both brokers stay completely healthy
toward everyone else, so nothing looks broken — their clients just stop being able to
see each other.

That silence is the reason partition detection exists (below) rather than being
optional: a persistently failing link in a clique is an operational fault to fix, and
the design's job is to say so loudly instead of papering over it.

## Membership: static seeds plus gossip

A broker is given some number of seed peers and dials them:

```sh
zenohb -l tcp/0.0.0.0:7447 --advertise tcp/10.0.0.1:7447 --peer tcp/10.0.0.2:7447 \
    --accept-router-faces
```

Everything else is learned. On a link coming up, each side sends its full member view
as a gossip message; learning a new member triggers a re-advertisement to every other
peer. A clique therefore closes in **one round**: a newcomer seeded against any single
member learns all the others and dials them, while that member's re-advertisement
tells everyone else about the newcomer. Adding broker N+1 means configuring the
newcomer, not reconfiguring the N already running.

- **Carrier.** Gossip travels as an ordinary `Push` on the reserved key
  `@/router/gossip`. No new message type, no codec change, and it reuses the existing
  framing and batching. The key is intercepted on the receiving `Face` and never
  reaches `Tables`, so a client can neither inject membership by publishing there nor
  observe it by subscribing to `@/**` — a Push on a reserved key from a client face is
  simply dropped.
- **No departure protocol.** Gossiping departures is where this class of design
  usually goes wrong: a departure racing a re-announcement resurrects the member, and
  a partition makes both halves declare the other dead. A learned member is instead
  kept forever and re-dialled with capped, jittered backoff (250 ms → 30 s). That is
  bounded because a clique is tens of brokers, and it means a broker that restarts,
  moves, or is briefly unreachable rejoins with no operator action.
- **Bounded.** The member table is capped (`Membership::max_members`), as are
  endpoints per member, and every endpoint is validated before it is stored — one
  misconfigured broker cannot grow the whole clique's state or send it chasing an
  unusable address.
- **`--advertise` is required when `-l` names a wildcard.** `0.0.0.0` is not something
  a peer can dial, so gossiping it would be worse than useless. It defaults to `-l`
  plus the bound port when that is a concrete address. A broker with no advertisable
  endpoint still takes part — it dials out and routes normally — it just cannot be
  dialled back, so it must be able to reach its peers itself.

### Collapsing a mutual dial

Gossip makes both ends learning about each other at the same instant the *normal* case,
so simultaneous dials are expected rather than rare. The tie-break is a pure function
of the two identities — **the link dialled by the lower zid survives**
(`keep_outbound_link`) — evaluated independently at each end, which necessarily yields
complementary answers. Deciding by arrival order instead could close both links, or
neither.

The losing side's connector is then **parked** (`FaceHandle::dial_suppress`) rather
than left to retry: without that it would reconnect immediately, be collapsed again,
and flap forever against a peer it already has a perfectly good link to. `Tables`
releases the park when the surviving link to that peer goes away.

## Declarations are aggregated

Each broker announces a key expression to its peers **once**, no matter how many of its
own clients declared it, and withdraws it only when the last one goes. This mirrors the
reference's `Map<Resource, Set<RouterZid>>` exactly.

The aggregate is not maintained as a parallel refcount — it is read straight out of
`ResourceTable` on both sides of every declaration change (`Tables::local_decl`), so
the announced state cannot drift from the routing state, an idempotent redeclare is a
no-op for free, and a disconnect needs no separate bookkeeping.

Wire shape, following the reference's router-to-router path:

| | client-facing | router-to-router |
| --- | --- | --- |
| entity `id` | a real, per-entity id | always `0` |
| `interest_id` | set when replying to an Interest | never set |
| undeclare identifies the entity by | its `id` | the `wire_expr` extension |

The id is always 0 because a router-sourced declaration is an aggregate keyed by
(key expression, originating broker) rather than any single entity — which is exactly
why the undeclare has to carry the key expression instead.

**Link-up sync is a plain replay**, not an Interest exchange. Routers do not send each
other Interests: the reference's `hat/router/interests.rs` is entirely no-ops and its
dispatcher logs *"Ignoring interest from router (unsupported)"*. When a link comes up,
each side simply sends one declaration per key it currently has a local client for.
Idempotent, so a replay after a flap is harmless.

**Link-down needs no extra machinery.** `Tables::remove_face` already strips every
declaration a face had announced; for a peer link that is the whole cleanup.

## Query routing

`QueryTarget` stops being degenerate once local and remote queryables can both exist
(`docs/BROKER.md` previously noted `best_matching` and `all` were synonymous "since
they only diverge with multi-hop routing"):

| target | behaviour |
| --- | --- |
| `best_matching` | matching **local** client queryables if any exist; only otherwise does the query cross the mesh |
| `all` | every matching queryable, local and remote alike |
| `all_complete` | as `all`, filtered on `QueryableInfo.complete` |

Under a single broker every candidate is local, so `best_matching` is a no-op and
behaviour is bit-identical to before federation existed.

A peer broker's announced `QueryableInfo` aggregates its clients' — `complete` is true
if *any* local queryable is complete — which is correct because the broker on the far
side re-filters against its own queryables at the terminal hop.

Fan-in is unchanged: a peer face is just another target, a fresh local request id is
allocated for it, and responses return along `pending_queries_`. One hardening came
with federation: a request id that is **still live** for a face is answered with an
immediate `ResponseFinal` rather than being registered, because recording it would
overwrite the in-flight fan-out under the same key and strand the original requester.
That needed a misbehaving client before; a peer face carries request ids chosen by a
whole remote broker, which makes it far more reachable.

## zid-targeting across the mesh

`target_zid` (the `DestinationId` extension, see `docs/BROKER.md`) is applied at the
**terminal hop**:

- to a **client** face — deliver only if the zid matches, as before;
- to a **router** face — deliver on key match alone, *without* checking the zid, since
  the target may well live behind it.

The broker on the far side sees the message arrive on a router face, considers only its
own client faces, and applies the filter there. This is stateless — no mesh-wide zid
tracking, nothing to go stale — and "filter, never a bypass" holds end to end, because
forwarding to a *broker* is not a delivery. A targeted message therefore crosses every
link whose broker has a matching declaration though only one client receives it; a
zid→broker map would cut that to one link and is a contained future optimisation.

The extension is carried explicitly through the request re-encode. It has to be:
a forwarded `Request` is rebuilt rather than copied, and dropping it there made a
targeted `get()` fan out to every queryable in the mesh.

## Congestion control is per message

Which of two congestion policies applies is chosen **by the publisher, per message**,
via the standard Zenoh `CongestionControl` — bit 3 ("D") of the QoS extension that
`Push` and `Request` already carry. This is not a project-local setting: no wire format
changed, and a real `zenoh-rust` publisher using `CongestionControl::Block` is honoured
identically.

```cpp
session.put("cmd/arm", payload, {.congestion = CongestionControl::block});
session.get("sensors/**", "", {.congestion = CongestionControl::block});
```

A face has three pressure levels (`FacePressure`):

| level | meaning |
| --- | --- |
| `ok` | everything is delivered |
| `congested` | past the high watermark: `Drop` traffic is discarded for this face, `Block` traffic is still queued, and the faces feeding it are read-throttled |
| `saturated` | past a hard ceiling: nothing more is queued and the face is closed |

- **`Drop` (the default)** behaves as before — a slow consumer is skipped rather than
  allowed to stall the producer or anyone faster — but dropping is now a *last* resort
  rather than a first response, because crossing the watermark on a clique link
  read-throttles the client faces feeding it (`Tables::congested_router_faces`) and
  lets ordinary TCP flow control carry the pressure back to the publishers.
- **`Block`** is queued past the watermark and never discarded. What bounds it is that
  same read-throttling; what bounds it when *that* fails is the hard ceiling.
- **Clique links get much larger budgets** (16 MiB / 4 MiB, vs 1 MiB / 256 KiB for a
  client face): a router link aggregates every client behind the peer, so the budget
  tuned for one slow subscriber would start discarding a whole broker's worth of
  traffic over a transient blip.
- **`ResponseFinal` is always `Block`.** Dropping a terminator does not lose data — it
  leaves the requester waiting for something that never arrives, until its own
  client-side timeout.

Two things this deliberately does **not** do:

- **Honour priority (QoS bits 2:0) or express (bit 4).** That needs per-priority frame
  sequence numbers and queues; this codebase has a single `frame_sn_` that always
  writes `Reliability::reliable`. Congestion control needs none of it, so it lands on
  its own rather than being half-built alongside.
- **Avoid slowing the publisher toward healthy destinations.** `Block` means
  backpressure reaches the producer, which slows it toward *every* destination, not
  only the congested one. That is unavoidable for "never dropped", and it is precisely
  why the choice is per message rather than a broker-wide mode.

### The hard ceiling is not the old close-on-overflow bug

`docs/BROKER.md`'s history section records that closing a face at 1024 queued *frames*
was a real defect: it turned any transient production/drain mismatch into a disconnect.
The ceilings here (64 MiB client, 256 MiB clique link) sit two orders of magnitude
higher and are only reachable **after** read-throttling has already failed to help —
i.e. a peer that has stopped draining altogether, such as a dead TCP connection the
kernel has not yet timed out. Reaching one converts unbounded memory growth into a
detectable failure.

## Link liveness and partition detection

Clique links, and only clique links, run a keepalive/lease loop: a `KeepAlive` every
`lease/4` when otherwise idle, and the link is dropped if nothing has arrived within a
full lease. Client faces keep the previous behaviour (liveness purely by TCP error or
EOF). The difference matters because a peer whose host vanishes is otherwise only
noticed when TCP gives up — minutes — during which this broker routes into a black
hole and reports nothing wrong.

`Tables::unlinked_peer_count()` is the observable partition state: known peers this
broker currently holds no link to. `zenohb` reports changes to it on stderr:

```
zenohb: 1 known peer broker(s) unreachable -- clients behind them are not visible from here
zenohb: clique complete, all peers reachable
```

Reported on change, not on a timer, so a persistently unreachable peer is one line
rather than a stream.

## Module map

| Module | Unit | Contents |
| --- | --- | --- |
| `zenoh.broker.membership` | `broker/src/membership.{cppm,cpp}` | `MemberInfo`, `Membership`, the gossip payload codec, endpoint validation, `keep_outbound_link`. Pure — no ASIO, no I/O — which is what makes it unit-testable standalone (`tests/test_membership.cpp`), like `zenoh.broker.resource`. |
| `zenoh.broker.tables` | `broker/src/tables.{cppm,cpp}` | `FaceKind`, `FacePressure`, split horizon, declaration aggregation and replay, gossip ingestion, the dial hook, partition state. |
| `zenoh.broker` | `broker/src/broker.{cppm,cpp}` | `BrokerConfig`, the dialer-side handshake, the peer connector and its backoff, gossip interception on `Face`, keepalive, partition reporting. |

Everything that touches a socket stays inside `broker.cpp` — the connector, its retry
timer, the keepalive loop. That is not tidiness: `docs/BROKER.md`'s "Why `Face` isn't
its own module" documents a clang/libc++ named-modules bug that makes any `.cppm`
naming an ASIO type poison its own BMI. `membership.cppm` being ASIO-free is a
requirement, not a preference.

## Testing

- `tests/test_membership.cpp` — the pure module: gossip codec round-trip and a
  truncation sweep, `learn` idempotence, the dial tie-break agreeing from both sides,
  table caps, endpoint validation.
- `tests/test_clique.cpp` — real `Broker` instances on loopback ephemeral ports driven
  by real `Session` clients, the same posture as `test_broker.cpp` one broker further
  out. Covers link establishment and connector resilience; aggregation and refcounted
  withdrawal; link-up replay; **exactly-once delivery in a three-broker mesh with a
  subscriber behind both peers of the publisher** (the split-horizon regression test);
  query/reply across the mesh and every `QueryTarget`; zid-targeting at the terminal
  hop; gossip convergence from a single seed, including a four-broker clique; the
  reserved-key injection defence; congestion classes; keepalive on an idle link;
  partition visibility; and a multi-threaded pool case.

The strand-discipline asserts (`assert(strand_.running_in_this_thread())`) are compiled
in on the `clang`/`linux-clang` presets, so the multi-threaded cases turn an off-strand
access into a deterministic failure. `linux-tsan` remains the dynamic race gate:

```sh
ctest --preset clang -L '^test_clique$' -j8
scripts/docker-ci.sh linux-tsan
```

There are **no differential vectors** for any of this — federation semantics are
project-local, the same position `DestinationId` is already in — so round-trip and
property tests carry that burden.

## Manual test

Three brokers, each of the last two seeded only against the first — so the link
between them is one gossip produced, not one that was configured:

```sh
B=./build/clang-release
# --accept-router-faces on :7447 because both peer links arrive there inbound.
$B/zenohb -l tcp/127.0.0.1:7447 --threads 2 --accept-router-faces &
$B/zenohb -l tcp/127.0.0.1:7448 --peer tcp/127.0.0.1:7447 --threads 2 &
$B/zenohb -l tcp/127.0.0.1:7449 --peer tcp/127.0.0.1:7447 --threads 2 &

# Subscribe on :7449, publish on :7448 -- two brokers that were never told about
# each other. `stdbuf -o0` only matters because z_sub's stdout is a pipe here and
# would otherwise stay buffered until you kill it.
stdbuf -o0 $B/examples/z_sub -e tcp/127.0.0.1:7449 -k 'demo/example/**' &
$B/examples/z_put -e tcp/127.0.0.1:7448 -k demo/example/k -p hello
```

To see the split-horizon invariant do its job, put a subscriber behind *both* peers of
the publisher's broker (`:7447` and `:7449` above) and publish twice — each subscriber
must see both messages once, not the first one twice. To see partition detection, kill
`:7448` and watch the other two report it within ~5 s.

With real `zenoh-rust` clients — federation is invisible to them, since a client
neither knows nor needs to know which broker of the clique it is attached to:

```sh
../zenoh-rust/target/release/examples/z_sub -m client -e tcp/127.0.0.1:7449 -k 'demo/example/**' &
../zenoh-rust/target/release/examples/z_pub -m client -e tcp/127.0.0.1:7448 -k demo/example/test -p hello
```

`-m client` is required on every reference binary — see `docs/BROKER.md`'s interop note
for why.

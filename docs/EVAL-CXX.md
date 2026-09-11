# Evaluation for zenoh-cxx — Evaluator / Computation

The specification of the **Evaluation** abstraction as implemented in this repository.

[`EVAL.md`](EVAL.md) is the original request. It is written against the Rust API
(async builders, `ZBytes`, a `zenoh::evaluation` module), so it describes mechanics
that do not exist here. This document is the normative one for zenoh-cxx: it states
the semantics that MUST hold, the C++ API that expresses them, and the places where
the adaptation deliberately departs from the original. [`RUNTIME.md`](RUNTIME.md)'s
"Evaluation" section is the narrative introduction with worked examples;
this is the contract.

Keywords MUST / MUST NOT / SHOULD / MAY are used in the usual normative sense.

---

## 1. Objective

Evaluation is a second abstraction over the existing Query/Reply machinery, for
**invoking distributed computations** rather than querying data.

```
Querier(key expr)   --get()------------>  Queryable(key expr)  -->  Reply
Evaluator(key expr) --eval(argument)-->   Computation(key)     -->  Reply
```

A `Queryable` describes the ability to answer queries over a *region* of the key
space. A `Computation` is one computation registered at one *concrete* key. An
`Evaluator` invokes every Computation whose key matches its key expression.

The existing Query/Reply API MUST remain unchanged. Evaluation is additional; it is
not a replacement for, nor a variant of, `get`.

This runtime has no `Querier` (see [`RUNTIME.md`](RUNTIME.md)), so the structural
analogue of `Evaluator` here is `Publisher`: a declared handle that binds its key
expression to a numeric id and fixes its options once. `Session::eval` is the
undeclared convenience form, exactly as `Session::get` is for a query.

---

## 2. Core semantics

> A `Computation` is a computation registered at one concrete Zenoh key.
>
> An `Evaluator` is associated with a Zenoh key expression.
>
> `eval(argument)` delivers the argument to every registered `Computation` whose key
> matches the Evaluator's key expression.

Given

```
Computation("robot/r1/reset")
Computation("robot/r2/reset")
Computation("robot/r3/reset")
```

an `eval` on `robot/*/reset` MUST invoke all three.

Every eval MUST use `QueryTarget::all` and MUST NOT use `best_matching`. Selecting
one matching computation arbitrarily is not a meaningful thing to do to
`robot/*/reset`, and the API MUST NOT offer it.

If several `Computation` registrations exist at the same key, **all** of them MUST
receive the eval. No deduplication by key is performed:

```
Computation A1 -> robot/r1/reset
Computation A2 -> robot/r1/reset
Computation B  -> robot/r2/reset

eval("robot/*/reset")  =>  A1, A2 and B all run
```

The guaranteed unit of fan-out is the **registration**, not the key.

---

## 3. Side effects

Computations MAY have side effects: `robot/r1/reset`, `queue/jobs/claim`,
`locks/foo/acquire`, `counter/increment` are all valid computations. An eval MAY
therefore calculate a value, read or modify state, actuate hardware, acquire or
release a resource, or start another operation.

The API MUST NOT imply that `eval` is read-only, pure, idempotent or retry-safe, and
its documentation MUST say so explicitly. This is the primary reason Evaluation
exists separately from `get`.

Two consequences are normative rather than stylistic:

- Reply consolidation MUST be off (§7). Discarding a reply because another reply on
  the same key already arrived would hide a computation that actually ran.
- A computation's delivery queue MUST NOT conflate. `ComputationOptions` therefore
  exposes `capacity` only, and the strand is always `StrandMode::ordered`
  (`last_value` would silently drop an eval that had already been accepted).

---

## 4. Computation declaration

```cpp
auto Session::declare_computation(std::string_view key,
                                  ComputationOptions opts = {})
    -> std::expected<Computation, ZError>;

auto Session::declare_computation(std::string_view key, EvalHandler on_eval,
                                  ComputationOptions opts = {})
    -> std::expected<Computation, ZError>;
```

`key` MUST be a **concrete, canonical** key expression. `robot/r1/reset` and
`math/square` are valid; `robot/*/reset`, `math/**`, `*/bar` and non-canonical forms
such as `a//b` MUST be rejected with `ZError::invalid_key_expr`. The wildcard belongs
on the Evaluator's side.

A key expression naming the reserved namespace (§8) MUST also be rejected, so the
namespace cannot be nested inside itself.

There MUST be no per-session limit on computations, and two computations MAY share a
key (§2) — unlike `Subscriber`/`Queryable`, which this runtime limits to one per
session. `Computation` is consumed either by handler (invoked from
`Session::run()`/`run_once()`) or pull-style via `Computation::recv()`, mirroring
`Queryable`.

`complete` MUST NOT be exposed: completeness is a data-query notion with no meaning
for a computation. A Computation is always declared incomplete.

### Declaration on the wire

Computations on the same key MUST share one wire declaration, refcounted, released
when the last of them is undeclared. An `UndeclareQueryable` names a key expression
rather than a registration, so per-registration declarations would let the first
undeclare stop routing to the survivors on any router that keys declarations by key
alone (`zenohb` does). This also puts N-1 fewer declarations on the wire.

---

## 5. Evaluator declaration and `eval`

```cpp
auto Session::declare_evaluator(std::string_view key_expr, EvalOptions opts = {})
    -> std::expected<Evaluator, ZError>;

auto Evaluator::eval(std::span<const std::byte> argument)
    -> std::expected<Getter, ZError>;
auto Evaluator::eval(std::span<const std::byte> argument, GetReplyHandler on_reply)
    -> std::expected<void, ZError>;

auto Session::eval(std::string_view key_expr, std::span<const std::byte> argument,
                   EvalOptions opts = {}) -> std::expected<Getter, ZError>;
auto Session::eval(std::string_view key_expr, std::span<const std::byte> argument,
                   GetReplyHandler on_reply, EvalOptions opts = {})
    -> std::expected<void, ZError>;
```

An Evaluator MAY use any canonical key expression, wild or not, outside the reserved
namespace (`ZError::invalid_key_expr` otherwise). Canonicity is required here even
though `get` does not require it, because the key expression is prefixed internally
and prefixing MUST preserve it rather than splice it into something malformed.

`Session::eval` MUST be equivalent to declaring an evaluator and evaluating once,
except that a declared `Evaluator` sends its key expression as a declared numeric id.

The argument MUST be a positional parameter of `eval`, not a builder step — it is the
*argument of a computation*, not data being stored. A computation needing no argument
is passed an empty span, which MUST be delivered as an empty argument rather than as
"no argument".

---

## 6. The `Eval` object

A computation is handed an `Eval`, never the underlying query:

```cpp
class Eval {
    auto argument() const noexcept -> std::span<const std::byte>;
    auto key_expr() const noexcept -> std::string_view;         // the evaluator's
    auto computation_key() const noexcept -> std::string_view;  // this computation's
    auto reply(std::span<const std::byte> value) -> std::expected<void, ZError>;
    auto reply_err(std::span<const std::byte> error) -> std::expected<void, ZError>;
};
```

`key_expr()` MUST be the logical key expression the evaluator supplied
(`robot/*/reset`); `computation_key()` MUST be the concrete key of the computation
processing this eval (`robot/r1/reset`). Neither MUST ever expose the internal
namespace (§8).

`reply` MUST NOT take a key: the implementation knows the computation's concrete key.
(`Query::reply` needs an explicit key only because an ordinary queryable may itself
be declared on a wildcard.)

`reply_del` MUST NOT exist: a deletion is data-centric and has no meaning as the
result of evaluating a computation.

`Eval` is move-only and RAII. Its destructor releases the eval's share of the
underlying request; the request's `ResponseFinal` is sent when the *last* `Eval` it
produced is released (§9), not when any one of them replies.

**Not modeled** (following this runtime's existing scope, not by choice): an eval's
encoding and attachment — this runtime has no public `Encoding` or attachment concept
at all — priority/express QoS, matching status/listeners, and the reference's
`allowed_origin`/`allowed_destination` (`Locality` filters, meaningless for a client
that reaches everything through a router; the zid filter `EvalOptions::target_zid` is
a different thing and *is* modeled). `background()` has no C++ counterpart: a handle
undeclares when it drops.

---

## 7. Replies

Replies MUST reuse the existing public reply types: `Session::eval` and
`Evaluator::eval` return a `Getter`, whose `recv()` yields each `GetReply` and then
`nullopt` when every computation has finished.

A successful reply MUST carry the **logical computation key**, so the caller can tell
which computation produced which result:

```
Evaluator: robot/*/compute
Replies:   robot/a/compute -> result A
           robot/b/compute -> result B
```

A computation MAY send 0..N successful replies and 0..N error replies.

Replies MUST NOT be consolidated: the underlying query uses
`ConsolidationMode::none`, and the reply queue is `ordered` (never conflating). Two
computations at one key that both reply MUST both reach the evaluator.

---

## 8. Isolation from ordinary Query/Reply

**This is the critical requirement.** An eval MUST NOT invoke an ordinary
`Queryable`, and an ordinary `get` MUST NOT invoke a `Computation` — including when
both are declared on the same key.

Because both ride the same Query/Reply messages, they are told apart by key space. A
Computation is declared, queried and matched under a reserved internal prefix:

```
logical computation key       robot/r1/reset
internal wire key             @eval/robot/r1/reset

logical eval key expression   robot/*/reset
internal wire key expression  @eval/robot/*/reset
```

Prefixing both sides identically preserves matching exactly (`robot/*/reset` matches
`robot/r1/reset` iff the prefixed forms match), so nothing about the application's key
expressions changes.

Two mechanisms then keep the namespace closed, and **both** are required:

1. **`@eval` is a verbatim chunk.** A chunk beginning with `@` is matched only by an
   identical literal, never by `*` or `**` (`zenoh.ke`, mirroring the reference's
   `MayHaveVerbatim`). No wildcard an application writes can reach a computation,
   `get("**")` included.
2. **The namespace is reserved at the API boundary.** `Session::get` and
   `Session::declare_queryable` MUST reject a key expression whose first chunk is
   `@eval`, and the Evaluation API MUST reject it too. Matching alone cannot stop a
   caller who *types* `get("@eval/robot/r1/reset")`, which would otherwise match a
   Computation's wire declaration exactly.

Only the query surface needs the second guard: `put`/`declare_subscriber` route to
subscribers, and a Computation is never one.

The prefix MUST belong to a Zenoh-reserved namespace, MUST NOT be user-visible, and
MUST be centralized in one internal constant with `to_eval_wire_key` /
`from_eval_wire_key` helpers, all private. `@eval` sits beside the reference's `@adv`
in reserved non-alphabetic-leading key space, deliberately outside the `@/...` admin
space (which `zenohb` refuses to route at all).

### Reply keys

The reply key (the logical computation key) does not intersect the namespaced request
key, so the underlying query MUST carry the `_anyke` selector parameter — the
reference's `ReplyKeyExpr::Any`, which it likewise transports as a parameter rather
than a wire field. Note where that is enforced: not in a router, but in the querying
*session* (the reference drops a non-intersecting reply unless `_anyke` is set). It
is inert between two of these clients and is what makes the exchange correct for a
reference peer.

### Limits of the guarantee

The API-boundary reservation binds this implementation's clients only. Another
implementation's client may declare a queryable inside `@eval/...` and would then
receive evals. That is the same status the reference's own `@adv` namespace has — a
convention among implementations. Closing it properly would require the routing layer
to distinguish the two kinds of declaration on the wire, which is what this API-only
design trades away.

---

## 9. Routing and request lifetime

Evaluation MUST reuse the existing Query/Reply routing rather than duplicate
transport machinery. Every eval maps to:

```
key expression = @eval/<the caller's key expression>
target         = QueryTarget::all
consolidation  = ConsolidationMode::none
parameters     = _anyke
payload        = the argument (the Query's value extension)
```

`target`, `consolidation` and reply-key acceptance MUST NOT be configurable:
`EvalOptions` carries `timeout_ms`, `target_zid` and `congestion` only. There MUST be
no `Evaluator::target(...)` or `Evaluator::consolidation(...)`.

A router matches per **face**, so one `Request` reaches a session however many of its
computations match. The per-registration fan-out is therefore performed client-side by
key-expression matching, and:

- it MUST be all-or-nothing against a full queue — a partially delivered request that
  the receive cursor later replays would run some computations twice;
- the request's single `ResponseFinal` MUST be sent only once every `Eval` it produced
  has been released;
- undeclaring a computation with evals still queued MUST release them, so an evaluator
  is never left waiting for replies that are no longer coming.

---

## 10. Guarantees, and what is not guaranteed

The API MUST NOT claim, in code or documentation:

```
exactly-once execution
at-most-once execution across failures
idempotence
transactionality
one execution per distinct key
transparent replication
```

The guaranteed fan-out unit is the matching **Computation registration**, with the
semantics achievable through current Zenoh routing. `eval` is never safe to retry
blindly.

---

## 11. Public API surface

Everything below lives in `zenoh.session` (namespace `zenoh`), alongside
`Queryable`/`Getter`, and is re-exported by the `zenoh` umbrella module.

| Type | Role |
| --- | --- |
| `Computation` | Handle to a declared computation; `recv()`, `undeclare()`, `key()` |
| `ComputationOptions` | `capacity` only (§3) |
| `Eval` | One inbound evaluation (§6) |
| `EvalHandler` | `std::function<void(Eval)>` |
| `Evaluator` | Declared evaluator; `eval()`, `undeclare()`, `key_expr()`, `keyexpr_id()` |
| `EvalOptions` | `timeout_ms`, `target_zid`, `congestion` (§9) |
| `Getter` / `GetReply` | Reused unchanged for replies (§7) |
| `ZError::invalid_key_expr` | The declaration/reservation error (§4, §5, §8) |

`Session::declare_computation`, `Session::declare_evaluator`, `Session::eval`.

The original spec suggests a dedicated `zenoh::evaluation` module. That is
deliberately not done here: the computation registry is `Session` state, so a separate
module would have to reach back into it, and this codebase's module graph is one
folder per layer rather than one per abstraction. The separation the abstraction needs
is in the vocabulary and the semantics, not in the build graph.

---

## 12. Compatibility

No existing behaviour of `Session::get`, `Queryable`, `IncomingQuery`, `Getter` or
`GetReply` may change, with one deliberate exception, required by §8: `Session::get`
and `Session::declare_queryable` now reject the reserved namespace. Before Evaluation
existed, `@eval/...` was ordinary application key space; reserving it is inherent to
the design this specification mandates.

Internal signatures may change freely — `write_request`/`start_get` gained a payload
and a scope — provided the public Query/Reply surface does not.

---

## 13. Required tests

The implementation is not complete until each of these is covered
(`tests/test_eval.cpp` unless noted):

| Area | What must be proven |
| --- | --- |
| Declaration | `foo/bar` succeeds; `foo/*`, `foo/**`, `*/bar`, `**`, and non-canonical keys fail with `invalid_key_expr` |
| Exact evaluation | with `foo/a` and `foo/b` registered, `eval("foo/a")` runs only `foo/a` |
| Wildcard evaluation | with `foo/a`, `foo/b`, `foo/c`, `bar/a` registered, `eval("foo/*")` runs exactly the three `foo/*` |
| Duplicate registration | two computations at `foo/a` both run, and both replies arrive |
| Undeclare | undeclaring one of two computations at one key leaves the other reachable (§4) |
| Query/Eval isolation | with `Queryable("foo/a")` and `Computation("foo/a")` on one session, `get` reaches only the queryable and `eval` only the computation |
| Namespace reservation | `get("**")` never reaches a computation; `get`/`declare_queryable` on `@eval/...` are refused (§8) |
| Reply identity | replies expose `foo/a`, `foo/b`, never the internal namespace |
| No consolidation | two computations at one key both reply and both replies arrive; one computation may reply several times |
| Side effects | wildcard evaluation increments every matching registration exactly once for the single delivered request — *not* an exactly-once delivery guarantee (§10) |
| Wire contract | the `Request` carries `@eval/...`, `QueryTarget::all`, `ConsolidationMode::none`, `_anyke` and the argument (`tests/test_query_api.cpp`) |
| Unchanged `get` | an ordinary `get` still sends no payload and its own target/consolidation defaults (`tests/test_query_api.cpp`) |
| Link loss | `Computation::recv()` reports `connection_closed` when the router goes away, and stays reported |

The wire-contract cases matter more than they look: `zenohb` treats `best_matching`
as `all` when every candidate is local and consolidates nothing, so a broker-level
test cannot distinguish a correct eval from an ordinary query. Only decoding the
`Request` proves §9.

---

## 14. Acceptance criteria

```cpp
auto c = session->declare_computation("math/square", [](zenoh::Eval e) {
    auto x = decode(e.argument());
    (void)e.reply(encode(x * x));
});

auto evaluator = session->declare_evaluator("math/*");
auto replies = evaluator->eval(encode(4));
```

1. Computations can only be declared on concrete keys.
2. Evaluators may use key expressions.
3. Every eval targets ALL matching Computation registrations.
4. Ordinary Queryables are never invoked by an eval.
5. Computations are never invoked by an ordinary get/query.
6. Eval arguments travel via the existing Query payload mechanism.
7. Computation replies automatically carry the computation's logical key.
8. Replies are not consolidated.
9. The existing `GetReply` is what the caller receives.
10. Existing Query/Reply behaviour is unchanged (§12).

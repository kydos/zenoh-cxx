# Zenoh Evaluator / Computation API

> **This is the original feature request, written against the Zenoh Rust API.** It
> describes mechanics that do not exist in this repository (async builders, `ZBytes`,
> a `zenoh::evaluation` module). Kept as the statement of intent it was.
>
> For what zenoh-cxx actually implements, and the C++ contract that is normative here,
> see [`EVAL-CXX.md`](EVAL-CXX.md); [`RUNTIME.md`](RUNTIME.md)'s "Evaluation" section
> is the narrative introduction.

## 1. Objective

Introduce a new **Evaluator / Computation** abstraction in the Zenoh Rust API.

The abstraction is built on top of the existing Query/Reply machinery but provides stronger and simpler semantics for invoking distributed computations.

The existing Query/Reply API MUST remain unchanged.

The conceptual distinction is:

```text
Query/Reply

Querier(KeyExpr)
      |
    get()
      |
      v
Queryable(KeyExpr)
      |
    Reply


Evaluation

Evaluator(KeyExpr)
      |
   eval(arg)
      |
      v
Computation(Key)
      |
    Reply
```

A `Queryable` describes the ability to answer queries over a region of the key space.

A `Computation` represents a computation associated with one concrete key.

An `Evaluator` evaluates all computations whose keys match its key expression.

---

## 2. Core semantics

The normative semantics are:

> A `Computation` is a computation registered at one concrete Zenoh key.

> An `Evaluator` is associated with a Zenoh key expression.

> `eval(argument)` sends the argument to every registered `Computation` whose key matches the Evaluator's key expression.

Therefore:

```text
Computation("robot/r1/reset")
Computation("robot/r2/reset")
Computation("robot/r3/reset")
```

and:

```text
Evaluator("robot/*/reset")
```

followed by:

```rust
evaluator.eval(argument)
```

MUST trigger all three computations.

This is intentionally different from ordinary Zenoh querying.

An Eval MUST always use semantics equivalent to:

```rust
QueryTarget::All
```

`BestMatching` MUST NOT be exposed by the Evaluation API.

This prevents:

```text
eval("robot/*/reset")
```

from arbitrarily triggering only one matching computation.

If multiple `Computation` registrations exist at the same key, all of them MUST receive the Eval. The current Zenoh routing layer does not provide "exactly one execution per distinct matching key", so the API MUST NOT promise that semantic.

For example:

```text
Computation A1 -> robot/r1/reset
Computation A2 -> robot/r1/reset
Computation B  -> robot/r2/reset
```

then:

```text
eval("robot/*/reset")
```

MUST trigger A1, A2, and B.

No deduplication by key is performed.

---

## 3. Side effects

Computations MAY have side effects.

The API MUST NOT imply that `eval()` is:

- read-only;
- pure;
- idempotent;
- retry-safe.

Examples of valid computations include:

```text
math/square
robot/r1/reset
queue/jobs/claim
counter/increment
locks/foo/acquire
vision/detect
planner/compute
```

An Eval MAY therefore:

- calculate a value;
- read state;
- modify state;
- actuate physical hardware;
- acquire/release resources;
- start another operation.

This distinction is one of the primary reasons for introducing `eval` separately from `get`.

---

## 4. Computation declaration

Add:

```rust
Session::declare_computation(key)
```

Example:

```rust
let computation = session
    .declare_computation("robot/r1/reset")
    .callback(|eval| {
        // Execute computation
    })
    .await?;
```

A Computation MUST be declared on a **non-wild key**.

Valid:

```text
robot/r1/reset
math/square
vision/detect
```

Invalid:

```text
robot/*/reset
robot/**/reset
math/*
```

Declaration with a wildcard MUST fail.

Zenoh already exposes non-wild key-expression types such as `OwnedNonWildKeyExpr`; the implementation may use these internally or validate an ordinary `KeyExpr` with its wildcard detection facilities.

The public API should preferably remain stylistically consistent with existing declaration APIs:

```rust
session.declare_computation("robot/r1/reset")
```

rather than forcing callers to manually construct a specialized key type.

Validation can therefore occur when resolving the builder.

Suggested error:

```text
A Computation must be declared on a concrete key; wild key expressions are not allowed: `robot/*/reset`
```

---

## 5. Evaluator declaration

Add:

```rust
Session::declare_evaluator(key_expr)
```

Example:

```rust
let evaluator = session
    .declare_evaluator("robot/*/reset")
    .await?;
```

Unlike a Computation, an Evaluator MAY use any valid key expression.

The corresponding operation is:

```rust
let replies = evaluator.eval(argument).await?;
```

An Evaluator is analogous structurally to the existing `Querier`:

```text
Querier     -> get()
Evaluator   -> eval()
```

but its routing policy is fixed by the Evaluation semantics.

---

## 6. Direct Session API

Also provide the non-declared convenience operation:

```rust
session.eval(key_expr, argument)
```

Example:

```rust
let replies = session
    .eval("robot/*/reset", command)
    .await?;
```

This should be conceptually equivalent to:

```rust
let evaluator = session.declare_evaluator(key_expr).await?;
evaluator.eval(argument).await?
```

just as `Session::get()` is currently the convenience counterpart of a declared `Querier`.

The argument SHOULD be mandatory at API level:

```rust
eval(key_expr, argument)
evaluator.eval(argument)
```

rather than:

```rust
eval(key_expr).payload(argument)
```

This emphasizes that the supplied value is the **argument of the computation**, not data being stored.

For a computation requiring no meaningful argument, an empty `ZBytes` may be supplied.

---

## 7. Incoming Eval object

A Computation callback SHOULD receive an `Eval` object rather than exposing the underlying `Query`.

Conceptually:

```rust
pub struct Eval {
    // wraps an underlying Query
}
```

Suggested API:

```rust
impl Eval {
    pub fn argument(&self) -> &ZBytes;

    pub fn encoding(&self) -> Option<&Encoding>;

    pub fn attachment(&self) -> Option<&ZBytes>;

    pub fn key_expr(&self) -> &KeyExpr;

    pub fn computation_key(&self) -> &KeyExpr;

    pub fn reply<T: Into<ZBytes>>(&self, value: T) -> EvalReplyBuilder;

    pub fn reply_err<T: Into<ZBytes>>(&self, error: T) -> EvalReplyErrBuilder;
}
```

`key_expr()` is the logical key expression submitted by the Evaluator.

For example:

```text
robot/*/reset
```

`computation_key()` is the concrete key of the Computation currently processing the Eval:

```text
robot/r1/reset
```

The Computation callback MUST NOT need to provide its own key when replying.

Instead of ordinary Query:

```rust
query.reply("robot/r1/reset", result)
```

the Evaluation API should allow:

```rust
eval.reply(result)
```

The implementation already knows the concrete Computation key.

This removes a complexity that exists in `Query::reply()` specifically because ordinary Queryables themselves may be declared on wildcard expressions. The current `Query` documentation explicitly explains why Query replies need an explicit key.

---

## 8. Replies

Prefer reusing Zenoh's existing public `Reply` type on the Evaluator side.

Example:

```rust
let replies = evaluator.eval(arg).await?;

while let Ok(reply) = replies.recv_async().await {
    // ordinary Reply
}
```

A successful reply MUST carry the **logical Computation key**.

Example:

```text
Evaluator: robot/*/compute

Replies:

robot/a/compute -> result A
robot/b/compute -> result B
robot/c/compute -> result C
```

This is important because the reply key identifies which computation produced each result.

A Computation MAY send:

```text
0..N successful replies
0..N error replies
```

unless a future version of the Evaluation abstraction introduces stronger cardinality constraints.

No reply consolidation SHOULD be performed.

The underlying query MUST therefore use:

```rust
ConsolidationMode::None
```

The current query API supports disabling consolidation explicitly.

`reply_del()` SHOULD NOT be exposed by `Eval`.

Delete replies are data-centric Query/Reply semantics and do not have an obvious meaning as the result of evaluating a computation.

The Eval API should expose only:

```text
reply(value)
reply_err(error)
```

---

## 9. Routing mapping

The implementation MUST use the existing Query/Reply routing infrastructure.

At the semantic level every Eval maps to:

```text
target        = QueryTarget::All
consolidation = ConsolidationMode::None
```

These properties MUST NOT be configurable through `EvaluatorBuilder` or `SessionEvalBuilder`.

In particular, the following APIs SHOULD NOT exist:

```rust
evaluator.target(...)
evaluator.consolidation(...)
```

because allowing them would violate the contract of Eval.

The current Zenoh API defines `QueryTarget::All` as delivery to all matching Queryables.

---

## 10. Isolation from ordinary Queryables

This is an important implementation requirement.

An Eval MUST NOT accidentally invoke an ordinary `Queryable`, and an ordinary `get()` MUST NOT invoke a `Computation`.

Therefore a Computation cannot simply be exposed on the wire as an indistinguishable Queryable at the same logical key unless the routing/dispatch implementation provides a way to distinguish the two kinds.

For an API-only implementation over the existing Query/Reply protocol, use an internal Zenoh-reserved key-space mapping.

Conceptually:

```text
logical Computation key:

robot/r1/reset

internal Queryable key:

<EVAL_INTERNAL_PREFIX>/robot/r1/reset
```

and:

```text
logical Eval:

robot/*/reset

internal Query:

<EVAL_INTERNAL_PREFIX>/robot/*/reset
```

Prefixing preserves ordinary key-expression matching:

```text
robot/*/reset
    matches
robot/r1/reset

therefore:

PREFIX/robot/*/reset
    matches
PREFIX/robot/r1/reset
```

The prefix MUST:

- belong to a Zenoh-reserved namespace;
- not be user-visible through the Evaluation API;
- be centralized in one internal helper/constant;
- preserve arbitrary valid user key expressions after prefixing.

Zenoh reserves non-alphabetic-leading key space for Zenoh-defined functionality, so the implementation should select an appropriate internal reserved prefix rather than occupying ordinary application key space.

Do not use the existing admin-space namespace casually; choose the exact prefix according to Zenoh's current internal namespace conventions.

Provide helpers equivalent to:

```rust
fn to_eval_wire_key(key: &KeyExpr) -> OwnedKeyExpr;
fn from_eval_wire_key(key: &KeyExpr) -> Option<OwnedKeyExpr>;
```

These details MUST remain private.

---

## 11. Logical reply keys

If the internal query key is namespaced but replies should expose the logical Computation key, the underlying Query must accept a reply whose key does not intersect the internal query selector.

Use:

```rust
ReplyKeyExpr::Any
```

internally.

The underlying implementation should therefore behave approximately as:

```rust
session
    .get(internal_eval_key_expr)
    .payload(argument)
    .target(QueryTarget::All)
    .consolidation(ConsolidationMode::None)
    .accept_replies(ReplyKeyExpr::Any);
```

The Computation's `Eval::reply(value)` can then internally execute approximately:

```rust
query.reply(logical_computation_key, value)
```

while the caller receives an ordinary Zenoh `Reply` whose key is the logical Computation key.

Zenoh 1.10 explicitly supports disjoint reply keys through `ReplyKeyExpr::Any`.

This approach avoids introducing an unnecessary `EvalReply` wrapper solely to translate internal key names.

---

## 12. Builder APIs

### ComputationBuilder

Model after `QueryableBuilder`.

Expose where appropriate:

```text
callback()
callback_mut()
with()
background()
allowed_origin()
```

Do NOT expose:

```text
complete()
```

Completeness is a data-query concept and has no meaning for Computations.

### EvaluatorBuilder

Model after `QuerierBuilder`.

Expose where appropriate:

```text
allowed_destination()
timeout()
priority()
congestion_control()
express()
matching_status()
matching_listener()
```

Internally force:

```text
QueryTarget::All
ConsolidationMode::None
ReplyKeyExpr::Any
```

Do NOT expose setters for those three properties.

### Eval builder

`Evaluator::eval(argument)` and `Session::eval(key_expr, argument)` SHOULD expose ordinary request-level facilities where they remain meaningful:

```text
encoding()
attachment()
callback()
callback_mut()
with()
timeout / cancellation where compatible with existing patterns
```

Do not expose Query selector parameters initially.

The Evaluation abstraction should remain:

```text
KeyExpr + argument -> Computations -> Replies
```

rather than inheriting every feature of `Selector`.

Additional capabilities can be introduced later if a concrete use case requires them.

---

## 13. Proposed public type family

Suggested names:

```rust
Computation<Handler>
ComputationBuilder<...>
ComputationUndeclaration

Eval

Evaluator
EvaluatorBuilder
EvaluatorEvalBuilder

SessionEvalBuilder
```

Public methods:

```rust
Session::declare_computation(...)
Session::declare_evaluator(...)
Session::eval(...)

Evaluator::eval(...)

Eval::argument(...)
Eval::reply(...)
Eval::reply_err(...)
```

The new API should live in a dedicated module if practical:

```rust
zenoh::evaluation
```

rather than further expanding `zenoh::query`.

This reinforces that Evaluation has its own semantics even though Query/Reply implements it internally.

---

## 14. Example

### Computations

```rust
let _c1 = session
    .declare_computation("robot/r1/reset")
    .callback(|eval| {
        reset_robot("r1");

        eval.reply("ok").wait().unwrap();
    })
    .await?;

let _c2 = session
    .declare_computation("robot/r2/reset")
    .callback(|eval| {
        reset_robot("r2");

        eval.reply("ok").wait().unwrap();
    })
    .await?;
```

### One computation

```rust
let replies = session
    .eval("robot/r1/reset", ZBytes::default())
    .await?;
```

Exactly the matching `robot/r1/reset` registration(s) execute.

### Wildcard evaluation

```rust
let replies = session
    .eval("robot/*/reset", ZBytes::default())
    .await?;
```

Both:

```text
robot/r1/reset
robot/r2/reset
```

execute.

The implementation MUST use `QueryTarget::All`; it MUST NOT select only one of them.

### Declared Evaluator

```rust
let evaluator = session
    .declare_evaluator("robot/*/reset")
    .await?;

let replies = evaluator
    .eval(ZBytes::default())
    .await?;
```

---

## 15. Required tests

### Declaration tests

Verify:

```text
declare_computation("foo/bar")       -> success
declare_computation("foo/*")         -> error
declare_computation("foo/**")        -> error
declare_computation("*/bar")         -> error
```

### Exact evaluation

Register:

```text
foo/a
foo/b
```

Evaluate:

```text
foo/a
```

Only `foo/a` executes.

### Wildcard evaluation

Register:

```text
foo/a
foo/b
foo/c
bar/a
```

Evaluate:

```text
foo/*
```

Exactly `foo/a`, `foo/b`, and `foo/c` execute.

### Duplicate computation registration

Register two Computations at:

```text
foo/a
```

Evaluate:

```text
foo/a
```

Both registrations execute.

This behavior MUST be documented.

### Query/Eval isolation

Register:

```text
Queryable("foo/a")
Computation("foo/a")
```

Then:

```text
session.get("foo/a")
```

MUST trigger only the Queryable.

And:

```text
session.eval("foo/a", arg)
```

MUST trigger only the Computation.

This is a critical acceptance test.

### Reply identity

Register:

```text
foo/a
foo/b
```

Evaluate:

```text
foo/*
```

Verify replies expose:

```text
foo/a
foo/b
```

and never expose the internal Evaluation namespace.

### No consolidation

If two Computations at the same key both reply, both Replies MUST reach the Evaluator.

### Side effects

Use counters in test Computations and verify that wildcard evaluation increments every matching registration exactly once for the single delivered Eval request.

Do NOT interpret this as an exactly-once delivery guarantee across failures or retries.

---

## 16. Compatibility and guarantees

The first implementation MUST NOT require changes to existing Query/Reply user APIs.

No existing behavior of:

```text
Session::get
Querier
Queryable
Query
Reply
```

should change.

Evaluation is an additional higher-level abstraction.

The implementation SHOULD initially be marked `unstable` if appropriate for Zenoh's API evolution process.

The API MUST NOT claim:

```text
exactly-once execution
at-most-once execution across failures
idempotence
transactionality
one execution per distinct key
transparent replication
```

The guaranteed fan-out unit is the matching **Computation registration**, using the semantics achievable through current Zenoh routing.

---

## 17. Acceptance criteria

The feature is complete when the following program model works:

```rust
let computation = session
    .declare_computation("math/square")
    .callback(|eval| {
        let x = decode(eval.argument());
        eval.reply(encode(x * x)).wait().unwrap();
    })
    .await?;

let evaluator = session
    .declare_evaluator("math/*")
    .await?;

let replies = evaluator.eval(encode(4)).await?;
```

with these properties:

```text
1. Computations can only be declared on concrete keys.
2. Evaluators may use key expressions.
3. Eval always targets ALL matching Computation registrations.
4. Ordinary Queryables are never invoked by Eval.
5. Computations are never invoked by ordinary get/query.
6. Eval arguments are transported using the existing Query payload mechanism.
7. Computation replies automatically use the Computation's logical key.
8. Replies are not consolidated.
9. Existing Reply can be reused by the caller.
10. Existing Query/Reply behavior remains unchanged.
```

The implementation should reuse existing Query/Reply types and builders internally wherever possible rather than duplicating transport or routing machinery.
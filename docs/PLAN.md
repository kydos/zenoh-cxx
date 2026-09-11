# Zenoh C++23 — Implementation Plan

A high-performance, highly modular C++23 implementation of the Zenoh v9 protocol,
targeting **Linux (primary, optimized)** and **macOS (supported)**. This document
sets the overall architecture and then drills into **Phase 1: messages and their
codecs**, which is the immediate focus.

---

## 1. Goals & Constraints

- **Protocol:** Zenoh wire protocol **v9** (matching `zenoh-nostd`).
- **Platforms:** Linux x86-64/arm64 (optimized hot paths) and macOS arm64/x86-64
  (correctness-equivalent, no perf tuning required). No Windows, no bare-metal,
  no WASM — we are *not* `no_std`/`no_alloc`. We may use the heap, the STL, and
  OS facilities freely (this is the key divergence from the reference).
- **Performance:** zero-copy decode, zero-allocation encode into caller buffers,
  no exceptions or virtual dispatch on the codec hot path, branch-lean varint.
- **Modularity:** C++23 named modules with a strict, acyclic dependency layering;
  the codec/message layer is fully independent of any I/O, transport, or runtime.
- **Toolchain (confirmed available):** clang 22 / gcc 15, CMake 4.3, Ninja 1.13.
  Primary CI compiler: **clang on Linux** with libc++; gcc as a second config.

### Reference mapping

The Rust reference splits into `zenoh-proto` (messages/codec/keyexpr/endpoint),
`zenoh-sansio` (transport state machine, sans-IO), `zenoh-nostd` (session/broker
API + driver), and `platforms/*` (link managers). **We deliberately diverge from
the sans-IO split above the codec** (see D8): the C++ transport/session is
*vertically integrated* with the I/O, not a sans-IO state machine driven through
link/driver abstraction layers. We keep only the bottom split — `zenoh-proto`'s
pure, I/O-free codec — and fold transport + framing + I/O + dispatch into one
tight Linux-first stack.

**Phase 1 covers only the equivalent of `zenoh-proto`'s codec + `msgs` + `exts`.**

---

## 2. Architectural Decisions (with rationale)

These are recommendations to be validated in architect review. Each has a default.

### D1 — Error handling: `std::expected`, no exceptions on the codec path
Codec functions return `std::expected<T, CodecError>` (decode) and
`std::expected<void, CodecError>` (encode). Rationale: matches Rust's `Result`,
zero-cost on the success path, no unwinding tables in the hottest loops, and
composes with a `ZTRY`-style macro (`auto x = ZTRY(expr);`) for ergonomic
short-circuiting. Exceptions remain allowed in non-hot setup/config code.
- `ZTRY` relies on the **GNU statement-expression extension** (`({ ... })`) to
  bind-and-short-circuit; supported by clang and gcc, documented as a deliberate
  dependency, and tested for correct error move-out on both compilers.
- `CodecError` is a **flat code-only `enum class`** (port the reference's 10–39
  ranges); no payload, so `expected` stays small. Diagnostic context lives behind
  a logging hook compiled out in release (the reference's `zctx!`/`trace!`).

### D2 — Buffers: span-based, zero-copy; messages parameterized on a `Bytes` policy
- **Reader** is a cursor over `std::span<const std::byte>`; `read_slice(n)`
  returns a *borrowed, contiguous* `std::span<const std::byte>` into the source
  buffer (no copy), exactly like the reference's `read_slice` returning `&'a [u8]`.
  It must also produce a **sub-reader** over a borrowed sub-span whose reads still
  borrow from the *original* buffer — load-bearing for every `ZStruct` extension,
  which length-prefixes then decodes its body from a re-sliced view.
- **Writer** is a cursor over `std::span<std::byte>`. Production encode is
  **two-pass** (`z_len` then write forward), as in the reference, so the
  `write_slot(n, fn)` back-patch primitive is **deferred** — not on the Phase 1
  critical path; added only when streaming/unknown-length encode appears.
- **Message representation: borrow-only (decided).** A decoded message holds
  *views* into the receive buffer and is valid only while that buffer lives;
  messages are plain, non-templated structs (no `Bytes` policy parameter). Opaque
  byte fields are `std::span<const std::byte>` directly (no newtype wrapper — the
  hand-written codecs call `write_prefixed`/`write_remainder` explicitly, so there
  is nothing to disambiguate by type). Genuinely-text fields (keyexpr `suffix`,
  query `parameters`) use `std::string_view`; UTF-8 is only assumed at the keyexpr
  layer. Retaining a message past the buffer is an explicit, by-hand copy added
  later at the API layer when a use case needs it — not built speculatively now.
- Decode entry points carry `[[clang::lifetimebound]]` for free dangling detection
  on clang, since fields borrow from the source buffer.

### D3 — Static polymorphism via concepts, no virtual codec dispatch
Define `Readable`/`Writable` **concepts** and constrain codec templates on them;
no `vtable` in encode/decode. Mandatory `Readable` surface: `remaining()`,
`peek()` (non-consuming — required so the extension loop can decide skip-vs-decode
before consuming the ext header), `read_u8/read_exact`, `read_slice(n)`→borrow,
and sub-reader construction. **Pinned contract:** *frames are contiguous at decode
time* — `read_slice` always returns a contiguous borrow; the transport reassembles
a frame before decode (scatter-gather/`io_uring`/ring buffers live in the
integrated runtime (P3) and never hand it a split slice). Keep the concept minimal — the test-only
`Storeable`/mark-slice trait from the reference is **not** ported.

### D4 — Codec: flat, hand-written per-message functions; no framework
**Decision: each message has two plain hand-written functions, `encode(writer, m)`
and `decode(reader) -> expected<Msg>`, that read top-to-bottom like the message's
wire diagram.** No macros, no generic engine, no codec type-state objects
(`Zenoh080`/`Header`/`Condition`), no per-ID generated type wrappers. The protocol
is stable; when it changes we edit these functions by hand, and that is fine. The
priority is that someone reading `encode`/`decode` for a message sees exactly what
bytes go on the wire, in order, with nothing to chase.

**"Extensions" are just optional message fields.** They are stored as ordinary
members — `std::optional<T>` when truly optional, or a defaulted value when the
protocol always carries one — never wrapped in ID-parameterized types and never
gated at compile time. A v9 implementation knows every field of every message, so
there is no generic extension subsystem; the only forward-compat concession is a
one-line `skip_ext` for an unrecognized id on decode.

**Shared code is limited to genuinely byte-level primitives** (the only place
duplication actually hurts), nothing higher:
- primitive codecs for `u8`, VLE-`u16/u32/u64`, fixed `array<byte,N>`,
  length-prefixed and remainder `span<const std::byte>` / `string_view`,
  `optional<T>` — varint and bounds logic exist once;
- header bit constants/`ByteField` accessors (D5) so flag math is named, not magic;
- three small writers `write_ext_unit/u64/zbuf(w, id, mandatory, more, value)` and
  the matching readers + `skip_ext(reader, header)` — these write/parse the single
  extension-header byte and body for the three wire encodings. They are honest
  one-screen helpers, not a framework.

**The optional-field idiom (no `n_exts` dance).** On encode, the `more` bit is
just "is any later optional field present", written inline and readably:
```cpp
bool ts = m.timestamp.has_value();
bool nid = m.nodeid != NodeId::Default;
if (qos) ZTRY(write_ext_u64 (w, ext::QoS,       /*more=*/ ts || nid, *m.qos));
if (ts)  ZTRY(write_ext_zbuf(w, ext::Timestamp, /*more=*/ nid,       *m.timestamp));
if (nid) ZTRY(write_ext_u64 (w, ext::NodeId,    /*more=*/ false,     m.nodeid));
```
On decode, a short per-message `while (has_ext)` loop reads the header byte,
`switch`es on the id into the known fields, and falls through to `skip_ext` for the
rest. The loop is a handful of lines and lives with the message it belongs to —
duplication here is shallow and *more* readable than hiding it behind a dispatch
callback.

Net: a typical message codec is a flat ~15–30 line function pair with no hidden
control flow. A future migration to C++26 static reflection is possible but
explicitly not a goal — clarity now beats generality later.

### D5 — Header bit-fields as typed descriptors
Port the existing `util::ByteField<shift, bits>` idea into a richer compile-time
header descriptor that encodes the `"Z|M|N|ID:5=0x1d"` semantics: named single-bit
flags, multi-bit fixed-value ID, multi-bit value fields (MODE:2, A:1), and
reserved bits. `constexpr` validation asserts the spec sums to exactly 8 bits.
This replaces the macro's left-to-right DSL parser with a typed, checked C++ form.

### D6 — Varint (VLE) is a standalone, heavily-tested primitive
1–9 byte LEB128-style `u64` with continuation bit, plus the `vle_len` size
calculator. Provide branch-lean encode and a decode with the same 9-byte cap as
the reference. Candidate for `__builtin_clzll`-based length on Linux.

### D7 — Endianness & fixed-width fields
`BatchSize` is little-endian `u16`; most integers are VLE. Provide explicit LE
helpers; never rely on host endianness. Linux/x86-64 and arm64 are both LE so
this is a correctness guard, not a perf cost.

### D8 — Vertically integrated transport/session, NOT sans-IO
**Decision: above the pure codec, do not build a sans-IO state machine.** The
reference's `zenoh-sansio` keeps transport logic free of sockets and drives it
through link/driver/buffer abstraction layers — great for portability across many
executors/embedded targets, but it multiplies indirection: every byte crosses
state-machine ↔ driver ↔ link ↔ socket boundaries, often behind `dyn`/virtual or
generic callback seams. For our **Linux-first, two-target** scope that overhead
buys little. Instead the transport+session is **vertically integrated** with the
I/O so the hot path — `recv → reassemble frame → decode → dispatch → invoke
handler`, and `publish → encode → batch → send` — is one mostly-inlinable flow.

Goals and how integration serves them:
- **Shorter call stack:** the event loop calls directly into framing→decode→
  dispatch without crossing sans-IO/link trait objects; no per-message virtual
  hops. Concrete encodings of the seams (epoll fd, batch buffer) are known to the
  compiler and inline.
- **Cache affinity:** one component owns the connection's receive buffer, TX batch
  buffer, sequence/lease state, and subscriber/query registries — laid out and
  touched together, rather than scattered across separately-allocated state-machine
  / driver / link objects linked by pointers.
- **Smaller code size:** collapsing the layers removes a tier of generic wrappers,
  callback plumbing, and duplicated buffer abstractions; fewer template
  instantiations and trait-object vtables.

What this does **not** change:
- **The codec (`zenoh-proto`) stays a pure, I/O-free leaf** (D1–D7). Vertical
  integration happens strictly above it; messages remain borrow-only views and the
  encode/decode functions are unchanged. The "contiguous frame at decode time"
  contract (D3) still holds — the integrated reader reassembles a frame before
  decoding.

Testability (the main thing sans-IO buys) is preserved without the layering:
- the codec is exhaustively unit/differential/fuzz tested in isolation (Phase 1);
- the integrated transport is tested over an **in-process connected pair**
  (`socketpair(2)` / loopback), and the *only* mockable seam is a thin syscall
  boundary (a `recv`/`send`/`poll` shim) — one narrow interface, not a pervasive
  sans-IO abstraction. We accept losing executor-portability (we have no embedded
  target) in exchange for the call-stack/cache/size wins.

---

## 3. Module & File Layout (Phase 1 scope in **bold**)

C++23 named modules, one logical layer per module, acyclic:

```
src/
  zenoh.cppm                 // umbrella: re-exports public surface, version()
  util/
    **zenoh.util.cppm**      // ByteField, header descriptors, LE helpers, ZTRY
  codec/
    **zenoh.buffer.cppm**    // Reader/Writer concepts + slice-backed impls
    **zenoh.varint.cppm**    // VLE encode/decode/len
    **zenoh.codec.cppm**     // CodecError, codec_traits, encode/decode/len engines,
                             //   field-descriptor + header-descriptor machinery
    **zenoh.codec.ext.cppm** // extension framework (Unit/U64/ZStruct kinds)
  proto/
    **zenoh.proto.fields.cppm** // ZenohId, WhatAmI, Reliability, WireExpr, QoS,
                                //   Encoding, ConsolidationMode, Resolution, ...
    **zenoh.proto.exts.cppm**   // SourceInfo, Timestamp, Attachment, NodeId, ...
    **zenoh.proto.transport.cppm** // Init{Syn,Ack}, Open{Syn,Ack}, Close,
                                    //   KeepAlive, Frame
    **zenoh.proto.network.cppm**   // Push/Put, Request/Query, Response/Reply/Err,
                                    //   ResponseFinal, Declare(+9 bodies), Interest
    **zenoh.proto.cppm**        // umbrella for proto, Message/NetworkBody variants
  ke/      zenoh.ke.cppm        // key-expression (Phase 2)
  endpoint/ zenoh.endpoint.cppm // endpoint parsing (Phase 2)
tests/                          // see §5.7
```

Later phases add a single vertically-integrated `runtime/` layer (D8) — connection
state + framing + epoll/kqueue I/O + dispatch fused together — under the
`api/` (session/pub/sub/get/queryable) surface. Both depend downward on the proto
layer only; there is no separate sans-IO `transport/` tier or `io/` link-manager
tier.

---

## 4. Wire-Format Facts to Preserve (from reference audit)

These are load-bearing and will be encoded as tests:

- **Message header byte:** `Flags(bits 7:5) | MID(bits 4:0)`. Bit 7 `Z` = "optional
  fields follow".
- **VLE u64:** low 7 bits/byte, bit 7 = continuation, max 9 bytes.
- **Optional-field ("extension") header byte:** `MORE(7) | KIND(6:5) | MAND(4) |
  ID(3:0)`. Kinds: `Unit=00`, `U64=01`, `ZStruct=10`. `ZStruct` bodies are
  length-prefixed (VLE) before the body; `U64`/`Unit` are not. We treat these as
  ordinary optional fields; the only generic handling is `skip_ext` for an
  unrecognized id on decode (non-mandatory → skip by kind, mandatory → error).
- **Slices/strings** carry no implicit length: length is either header-flag/VLE
  prefixed (`size = prefixed`) or "rest of buffer" (`size = remain`). The decoder
  for `remain` consumes `reader.remaining()`.
- **Message ID table:** Init 0x01, Open 0x02, Close 0x03, KeepAlive 0x04,
  Frame 0x05; Put 0x01, Query 0x03, Reply 0x04, Err 0x05 (sub-bodies);
  Interest/InterestFinal 0x19, ResponseFinal 0x1a, Response 0x1b, Request 0x1c,
  Push 0x1d, Declare 0x1e. Declare bodies 0x00–0x07 + Final 0x1a.
- **Top-level grouping:** `TransportMessage` {Close, Init{Syn,Ack}, Open{Syn,Ack},
  KeepAlive, Frame}; `NetworkMessage` { reliability, qos, body } where
  `NetworkBody` ∈ {Push, Request, Response, ResponseFinal, Interest,
  InterestFinal, Declare}. Modeled as `std::variant` in C++.
- **Flatten/shift:** `WireExpr` flattens into the parent header at a bit offset
  (e.g. shift=5) — the header descriptor must support sub-field merging.

---

## 5. Phase 1 — Detailed Build Order

### 5.0 Build system
- Convert CMake to **module-first** with `FILE_SET CXX_MODULES`, Ninja generator,
  per-target `cxx_std_23`. Two presets: `linux-clang-libcxx` (primary, `-O3
  -march=native` for release, sanitizers for debug) and `macos-clang`.
- Add a static lib `zenoh-proto` (Phase 1 deliverable) separate from the eventual
  shared `zenoh` lib, so the codec layer can be unit-tested in isolation.
- **Validate the module dependency scanner on BOTH clang and gcc at M0** (BMI is
  compiler/flag-specific and not interchangeable — a stub that builds on clang but
  not gcc is a classic early failure). Use **GMF `#include`** for STL headers, not
  `import std;`, for Phase 1 stability on clang 22 + libc++; revisit later.

### 5.1 `zenoh.util`
`ByteField<shift,bits>` (already started), promote to a **header descriptor**:
a `constexpr` type composing named flags / fixed ID / value-fields / reserved
runs, with `static_assert(total_bits == 8)`. LE load/store helpers. `ZTRY` macro
for `std::expected` short-circuit. `CodecError` enum (port the 10–39 code ranges).

### 5.2 `zenoh.buffer`
`Readable`/`Writable` concepts; a `SliceReader` over `span<const byte>` with
`peek/read_u8/read_slice/read_exact/remaining`, and a `SliceWriter` over
`span<byte>` with `write_u8/write_exact/write_slot/remaining`. `read_slice`
returns a borrowed span (zero-copy). Bounds-checked, returning `CodecError`.

### 5.3 `zenoh.varint`
`vle_len(u64)` via `__builtin_clzll` (branchless: `max(1, (64-clz(x|1)+6)/7)`),
`encode_vle`, `decode_vle` (9-byte cap, continuation bit). Decode uses a **fast/slow
split**: a single up-front bounds check when `remaining() >= 9` drives an unrolled
fast path, falling back to careful byte-at-a-time near the buffer end — this
removes per-byte bounds-check redundancy and dictates the function's shape, so it
is built in M1, not bolted on later. `[[likely]]`/`[[unlikely]]` on the
bounds/error branches. Exhaustive boundary tests (1/2/.../9-byte thresholds,
`u64::MAX`, **the overlong 10th-continuation-byte → error**, truncated inputs).

### 5.4 `zenoh.codec`
- Primitive codecs: free functions `encode/decode/len` (overloaded / ADL) for
  `u8`, `u16/u32/u64`→VLE, fixed `array<byte,N>`, length-prefixed and remainder
  `span<const std::byte>` / `string_view`, `optional<T>`. Varint and bounds logic
  live once here.
- Header bit constants + `ByteField` accessors (§5.1) so message functions name
  their flags (`flag::N`, `flag::Z`, …) instead of open-coding bit math.
- That is the whole shared surface — message `encode`/`decode` (§5.6) call these
  directly and stay flat. No descriptor engine, no field-helper indirection layer.

### 5.5 `zenoh.codec.ext` — minimal optional-field helpers (not a framework)
Just enough to read/write the one extension-header byte and its body for the three
wire encodings: `write_ext_unit/u64/zbuf(w, id, mandatory, more, value)`, the
matching `read_ext_*`, and `skip_ext(reader, header)` for unrecognized ids. Header
bit accessors (`MORE/KIND/MAND/ID`). No kind-as-type, no ID-as-template-param, no
dispatch engine — message `decode` functions own their small `while`/`switch` loop
(§5.6) and call these helpers.

### 5.6 Messages — `proto.fields`, `proto.exts`, `proto.transport`, `proto.network`
Port every field/ext/message catalogued in the audit, in dependency order:
1. fields (enums + `WireExpr`, `Resolution`, `Encoding`, `QoS`, ...).
2. exts (`SourceInfo`, `Timestamp`, `Attachment`, `NodeId`, `QueryableInfo`, ...).
3. transport messages (`Close`, `KeepAlive` first — simplest; then `Init*`,
   `Open*`, `Frame`).
4. network messages (`Put`/`Push`, `Query`/`Request`, `Reply`/`Err`/`Response`,
   `ResponseFinal`, `Declare` + 9 bodies, `Interest`/`InterestFinal`).
5. `Message` / `NetworkBody` variants and their dispatch.

### 5.7 Testing — round-trip + differential fuzz
- **Round-trip property tests** mirroring the reference's `roundtrip!` macro: for
  each type, `encode` then `decode`, assert structural equality, and assert
  `z_len` equals bytes written. Port the `rand()` strategy faithfully
  (`gen_bool(0.5)` for optionals, bounded slices) and **seed-log every failure**
  for reproducibility.
- **Differential testing against the Rust reference (highest value, pulled
  early):** a **minimal harness lands at M2** for the two prototype messages —
  emit byte vectors from the reference's `src/tests/{msgs,struct,ext}.rs` +
  `random.rs` generator (the oracle) and assert byte-identical C++ output. Finding
  wire mismatches on 2 messages at M2 is cheap; finding them across 25 at M5 is
  not. Scale the harness to all messages by M5.
- **Negative tests:** truncated buffers, **overlong 9-byte-capped varint**,
  **unknown *mandatory* extension → `CouldNotReadExtension`** (vs unknown
  non-mandatory → skip-by-kind), oversized length prefixes → correct `CodecError`.
- Framework: **doctest** (fast compile, module-friendly). Sanitizers (ASan/UBSan)
  on the debug preset; **libFuzzer over `decode` on Linux**, corpus seeded from the
  differential vectors.
- **Tests import specific leaf modules** (`zenoh.proto.network`), never the
  umbrella, to avoid full-rebuild fan-out during M2–M4 churn. A layering check
  enforces the one-way `exts → fields` dependency (`WireExpr` lives in `fields`
  but is used as an ext body) to prevent a module cycle.

---

## 6. Later Phases (sketch, not Phase 1)

- **P2 — keyexpr + endpoint** (`zenoh.ke`, `zenoh.endpoint`).
- **P3 — vertically integrated connection/runtime** (D8): one component that owns
  a connection's socket (epoll/kqueue), receive + TX-batch buffers, and protocol
  state (handshake, framing, SN, lease/keepalive), driving
  `recv → reassemble → decode → dispatch` and `encode → batch → send` directly —
  no sans-IO tier, no link-manager tier. Tested over an in-process
  `socketpair`/loopback; the only mock seam is a thin `recv`/`send`/`poll` shim.
  `io_uring` is a later drop-in behind that same shim.
- **P4 — session API** (pub/sub/get/queryable) layered directly on P3, then
  optional broker.

**Explicitly out of Phase 1:** SIMD and prefetch in the codec (fields are small and
irregular; payload copy is already optimal `__builtin_memcpy`). Vectorization and
batching belong in the integrated runtime (P3), not the codec.

---

## 7. Phase 1 Milestones / Deliverables

- **M0 (done):** module-first CMake + `CMakePresets.json` build the `zenoh-proto`
  static lib (C++23 modules) and a test bin, plus a reproducible **Linux Docker
  path** (`docker/Dockerfile`, `scripts/ci.sh`, `scripts/docker-ci.sh`,
  `compose.yaml`, GH Actions). Verified results:
  - **macOS dev — clang 22 + libc++:** full build + tests green under ASan/UBSan.
  - **Linux (Docker) — clang-19 + libstdc++:** full build + tests green under
    ASan/UBSan. **This is the validated Linux primary** (`linux-clang`).
  - **Toolchain limits found** (named modules are still maturing): clang+libc++
    breaks on clang-19 but works on clang 22 (a version issue); **gcc + libstdc++
    is blocked on both g++-14 and g++-15** by a libstdc++ `<type_traits>`
    (`__condres_cvref`/`__copy_cv`) module-GMF bug — unavoidable from our side
    since every std header pulls `<type_traits>`. `linux-gcc` is parked until the
    toolchain is fixed; CI defaults to `linux-clang`. See `docker/README.md`.
- **M1 (done):** `zenoh.util` (`ByteField`, `CodecError`, `__builtin_memcpy` LE
  helpers), `zenoh.buffer` (`Readable`/`Writable` concepts + `SliceReader`/
  `SliceWriter`), `zenoh.varint` (`clzll`-based `vle_len`, `encode_vle`/`decode_vle`
  with the 9-byte cap), plus `ZTRY`. 13 unit tests green under ASan/UBSan on clang.
  Note: the per-byte varint decode is kept simple (correct, one predictable branch
  per byte); a contiguous fast-path is deferred until profiling warrants it.
- **M2 (done):** primitive codecs (`zenoh.codec`) + minimal optional-field helpers
  (`zenoh.codec.ext`) + field/ext types (`zenoh.proto.fields`, `zenoh.proto.exts`)
  + **`Push`/`Put`** (`zenoh.proto.network`) + **`InitAck`** (`zenoh.proto.transport`),
  all flat hand-written `encode`/`decode`.
  - **Differential harness:** `tools/vector-gen` (Rust, path-deps the reference
    `zenoh-proto`) emits 12 golden vectors to `tests/diff_vectors.hpp`;
    `test_diff.cpp` asserts our encode is byte-identical and that we decode the
    reference's bytes and re-encode them identically. **All 12 vectors match
    byte-for-byte.**
  - Caught one real bug — `EntityGlobalId`/`InitIdentifier` store the zid length as
    `len-1` in the header nibble (the `maybe_empty` offset) — fixed.
  - `InitAck` validated the hardest cases: `InitIdentifier` (WhatAmI in header bits
    + zid-sized nibble, incl. the 16-byte → nibble 15 boundary), `InitResolution`
    S-gated with default, prefixed cookie, and **same-`id`/different-`kind`
    extensions** (`qos` Unit vs `qos_link` U64 at id 0x1, disambiguated by kind).
  - **40 tests green** on macOS (clang/libc++) and Linux (clang/libstdc++), ASan+UBSan.
  - The flat hand-written style holds up: even `InitAck` with 7 optional fields is a
    readable linear function; the only per-message "cleverness" is the kind-guarded
    `case 0x1` in its decode loop.
- **M3 (done):** all transport messages hand-written in `zenoh.proto.transport` —
  `InitSyn`/`InitAck`, `OpenSyn`/`OpenAck`, `Close`, `KeepAlive`, `FrameHeader`
  (+ `Duration`, `CloseBehaviour`, `Reliability`, `MultiLinkSyn`/`HasMultiLinkAck`).
  Exercised the flattened `Duration` (seconds-flag in header bit T, both secs and
  millis paths) and a second same-`id`/different-`kind` pair (`mlink_syn` ZStruct
  vs `mlink_ack` Unit at id 0x4). **22 differential vectors total, all byte-for-byte
  identical to the Rust reference; 53 tests green** on macOS (clang/libc++) and
  Linux (clang/libstdc++), ASan+UBSan.
- **M4 (done):** all remaining network messages — `Query`/`Request`,
  `Err`/`Reply`/`Response`, `ResponseFinal` (in `zenoh.proto.network`), the full
  `Declare` family (9 bodies + dispatcher, `zenoh.proto.declare`), and
  `Interest`/`InterestFinal` (`zenoh.proto.interest`). New cases exercised:
  `Duration` as a U64 ext (raw millis) vs flattened lease; `WireExpr` as a
  standalone ZStruct extension (own header byte) in the Undeclare* bodies;
  enum-dispatched payloads (`RequestBody`, `ResponseBody`, `DeclareBody`); the
  `InterestInner` options byte with a folded-in optional `WireExpr` (M/N/R bits).
  Two libc++ defaulted-`==` ambiguities (on `optional<WireExpr>` and the
  9-alternative `variant`) worked around with explicit comparisons.
  **47 differential vectors total, all byte-for-byte identical to the Rust
  reference; 71 tests green** on macOS (clang/libc++) and Linux (clang/libstdc++),
  ASan+UBSan. The full Zenoh v9 message set (PLAN.md §4) is now implemented.
- **M5 (done):** hardening.
  - **Negative/robustness tests** (`tests/test_negative.cpp`): malformed input is
    rejected with a `CodecError`; truncation sweeps decode every prefix of complex
    messages without reading out of bounds (ASan/UBSan-clean).
  - **libFuzzer target** (`fuzz/fuzz_decode.cpp`, `linux-fuzz` preset,
    `scripts/fuzz.sh`): fuzzes every decoder, checking no crash/UB and that
    `decode∘encode∘decode` is stable. Verified: **10.5M executions in ~46s, zero
    crashes** under fuzzer+ASan+UBSan.
  - **Randomized differential**: `tools/vector-gen` emits seeded-random valid
    Put/Push vectors (128); the C++ side decodes each and asserts byte-identical
    re-encode — covering value/option combinations the hand-authored vectors miss.
    Total **175 golden vectors**, all matching.
  - **Docs:** `docs/PROTO.md` (module map, conventions, how to add a message, how
    to run tests/differential/fuzz).
  - **79 tests green** on macOS (clang/libc++) and Linux (clang/libstdc++), ASan+UBSan.
  - **Post-review hardening (Tier 1 strict decoding):** narrowed integer fields
    reject out-of-range VLE values (`get_uint_as<T>`/`read_ext_uint<T>`) instead of
    truncating; out-of-range enums (`ConsolidationMode`/`QueryTarget`/`WhatAmI`)
    are rejected; text fields are UTF-8-validated (matching the reference's `&str`
    decode, so this is alignment, not divergence). Reserved header bits stay
    tolerated for forward-compat. New negative tests cover each; 82 tests green;
    7.4M fuzz executions clean through the new paths.

**Definition of done for Phase 1:** every message in §4 encodes/decodes
byte-identically to `zenoh-nostd`, validated by property + differential tests,
clean under ASan/UBSan, with the codec layer free of any I/O dependency.

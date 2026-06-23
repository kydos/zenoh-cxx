# Restructure Plan: folders, interface/impl split, documentation

Goal (per request):
1. **Folder-grouped modules** — a directory per functional area, instead of the flat `src/zenoh.xxx.yyy.cppm` layout.
2. **Separate public interface (`.cppm`) from implementation (`.cpp`)**.
3. **Full documentation** of public *and* private API.

Phase 1 (codec + messages) is complete and green (57 tests, 432 differential vectors vs zenoh-rust). This is a pure refactor — **no wire-format or behavior change**; the differential + round-trip + fuzz suites guard it.

> This plan was revised after a C++ architect review; the central decision (A) is now a *hybrid*, not "move everything to `.cpp`".

---

## The crux: the codec is template-over-concepts, so today nothing can move to `.cpp`

Every `encode`/`decode`/helper is `template <Writable W>` / `template <Readable R>`. Template definitions in a module **implementation unit are not reachable to importers**, so they must stay in the interface unit. Two independent decisions are tangled here; separate them:

- **(A1) Drop buffer genericity** — use one concrete `ByteReader`/`ByteWriter` (cursor over a contiguous `std::span`) instead of the `Readable`/`Writable` template parameter. Keep the concepts as a `static_assert`ed, documented contract the concrete types satisfy.
- **(A2) Where do bodies live** — interface unit (`inline` in `.cppm`) vs implementation unit (`.cpp`).

### Decision A1 — adopt concrete buffers. **Yes.**
The pinned contract is already *"frames are contiguous at decode time"* (D3); the runtime (D8) reassembles a contiguous frame before decode; `SliceReader`/`SliceWriter` are the only models of the concepts today. The genericity is unused. Collapsing to one concrete pair loses nothing real and makes signatures read better (`auto f(ByteWriter&)` vs `template <Writable W> auto f(W&)`). Rename `SliceReader`/`SliceWriter` → `ByteReader`/`ByteWriter`.

### Decision A2 — **hybrid**, not "everything to `.cpp`".
Moving a definition to a `.cpp` implementation unit makes it invisible to the optimizer across the module boundary. For a byte codec the per-field primitives (`encode_vle`, `read_slice`, `put_uint`, `peek_ext_header`, …) are 5–40 instructions called in tight sequences; **their inlining into `Put::decode` etc. is the entire performance story.** Out-of-lining them turns straight-line code into chains of out-of-line `std::expected`-returning calls, recoverable only with LTO — which is **not enabled here and is off in the ASan/fuzz builds**. An `inline` function defined in a `.cppm` is still a clean "interface unit"; the documented signature is the interface, body visibility is just an optimization.

So:

- **Hot/primitive layer stays `inline` in `.cppm`** (de-templated to concrete buffers, but bodies in the interface): `zenoh.util`, `zenoh.varint`, `zenoh.codec`, `zenoh.codec.ext`.
- **Message layer moves bodies to `.cpp`** — `zenoh.proto.{fields,exts,network,transport,declare,interest}`: the per-message `encode`/`decode`/`*_body` are large, called once per message (not per field), and benefit from BMI stability. This is where the interface/impl split is both feasible and worthwhile, and the cost is negligible because the inner primitives they call are still `inline` from the imported interface units.

**Must stay `inline` even in the message layer** (cannot or should not move to `.cpp`):
- `body_len()` / `encoded_len()` — hot path (encode computes the VLE length *before* writing the body) and trivial arithmetic.
- Anything templated on a non-buffer parameter: `get_uint_as<T>`, `read_ext_uint<T>`, `load_le<T>`/`store_le<T>`, `ByteField<Shift,Bits>`, `vle_len` (constexpr).
- Variant-dispatching `encode`/`decode` (`PushBody`, `ResponseBody`, `DeclareBody`) — the `std::visit` lambda is generic over the alternatives, so these stay inline.
- `put_ext_zstruct` (keep the current lambda form; it's fine when `inline`, and never type-erase the callback).

Net: the message structs' *plain* `encode`/`decode` member bodies move to `.cpp`; everything genuinely generic, hot, or variant-dispatching stays in the `.cppm`. This satisfies all three goals without de-optimizing the codec's only job.

---

## Decision B: folder layout

A directory per functional area; module **name stays dotted** (`zenoh.proto.network`); a folder may hold more than one closely-related module (e.g. the primitive `codec/` layer). Module names are unchanged, so consumers' `import`s don't change.

```
include/zenoh/detail/try.hpp           # ZTRY macro (consumed via GMF #include in .cpp units)
src/
  util/        util.cppm                       # module zenoh.util (inline; tiny)
  buffer/      buffer.cppm                      # module zenoh.buffer (ByteReader/Writer + concepts; inline)
  codec/                                        # the primitive codec layer (>1 module, all inline)
    varint.cppm                                 #   module zenoh.varint
    codec.cppm                                  #   module zenoh.codec
    ext.cppm                                    #   module zenoh.codec.ext
  proto/
    fields/    fields.cppm    fields.cpp        # module zenoh.proto.fields
    exts/      exts.cppm      exts.cpp          # module zenoh.proto.exts
    network/   network.cppm   network.cpp       # module zenoh.proto.network
    transport/ transport.cppm transport.cpp    # module zenoh.proto.transport
    declare/   declare.cppm   declare.cpp       # module zenoh.proto.declare
    interest/  interest.cppm  interest.cpp      # module zenoh.proto.interest
    proto.cppm                                  # module zenoh.proto (umbrella over all message modules)
  zenoh.cppm                                    # module zenoh — reserved for the user-facing API
```

- `.cpp` implementation unit begins `module zenoh.proto.network;` (no `export`) and *defines* members declared (not defined) in its `.cppm`. CMake: `.cppm` → `FILE_SET CXX_MODULES`; `.cpp` → ordinary `target_sources(... PRIVATE)`. **Validate this pattern first (Refactor-M0).**
- **Module partitions rejected:** more CMake/BMI-ordering complexity (primary interface must `export import :part;`) for no gain when `import zenoh.proto.network;` already works.
- **Public umbrella `zenoh.cppm`** (also flagged missing by an earlier review): `export import`s `zenoh.proto.{network,transport,declare,interest}`, plus `CodecError`, `ByteReader`/`ByteWriter`, and the dispatch types. Does **not** re-export the codec primitives (`zenoh.codec`/`zenoh.codec.ext`/`zenoh.varint`) or plumbing (`ByteField`, `ext_header_byte`, LE helpers). `ByteReader`/`ByteWriter` are **public** (they appear in every `encode`/`decode` signature) — keep them in `zenoh.buffer`, re-export from the umbrella. Tests keep importing leaf modules.

---

## Decision C: documentation standard

- **File header** on every `.cppm`/`.cpp`: one paragraph on the area's responsibility.
- **Public API** (exported types, functions, enums, fields): `///` doc comments — purpose, parameters, `expected` error conditions, and wire-format reference (message id / header-flag layout) where applicable.
- **Private API** (`.cpp` internals, file-local helpers): `//` explaining non-obvious logic (bit math, ordering quirks like "nodeid written last", the contiguous-frame assumption).
- Add a `docs/STYLE.md` + a `[[cpp-doc-style]]` memory. `///` is Doxygen-ready.
- **Enforcement:** a manual review checklist, optionally backed by clang `-Wdocumentation` (which actually understands declarations). **No homegrown regex lint** — the `export namespace zenoh { ... }` form (everything exported via the namespace, not per-entity `export`) defeats a "`///` before `export`" grep.

---

## Migration plan (incremental, green after every step)

0. **Refactor-M0 (representative spike, clang only):** one module whose `.cppm` declares an `export`ed class with a member `encode(ByteWriter&)`, defined in a sibling `.cpp` (`module X;`) that *calls a function from another imported module*, built into the static lib and linked by a test exe that `import`s it. Confirms member-def-in-impl-unit linkage, whether the `.cpp` needs its own `import` lines, and the CMake wiring — a trivial module would hide these.
1. **`util`** → folder; de-template (only `load_le`/`store_le` are templates → stay `.cppm`); all inline. Mostly a move.
2. **`buffer`** → concrete `ByteReader`/`ByteWriter` (rename from `Slice*`); keep `Readable`/`Writable` concepts + `static_assert`; inline.
3. **`codec` (varint, codec, ext)** → de-template primitives/helpers onto the concrete buffers; keep inline; keep `put_ext_zstruct` lambda form.
4. **`proto.fields` / `proto.exts`** → de-template; move plain `encode`/`decode`/`*_body` bodies to `.cpp`; keep `body_len`/`<T>`-templated inline.
5. **`proto.network` / `transport` / `declare` / `interest`** → move plain message `encode`/`decode` bodies to `.cpp`; keep variant-dispatch (`PushBody`/`ResponseBody`/`DeclareBody`) inline.
6. **`zenoh.cppm`** umbrella + finalize docs + doc checklist.
7. After each step: macOS clang + Linux Docker (clang) + differential + fuzz, **plus a clean from-scratch rebuild** to surface ODR/double-definition errors (behavioral tests won't catch a body left in both `.cppm` and `.cpp`).

Public module names stay stable throughout, so `tests/` *imports* don't change. **However**, the `SliceReader`→`ByteReader` rename touches source inside the message modules (the `SliceReader sub{...}` sub-readers in decoders) and a few test bodies (`test_buffer.cpp`) — so it is *not* "only CMake paths change". That rename is mechanical and lands in step 2.

### Risks
- **Biggest:** de-templating touches every codec signature; a stray `inline` body left in a `.cppm` while its twin moves to `.cpp` is an ODR landmine that still *links* — the from-scratch rebuild (step 7) and `-Wodr`/LTO check guard it; differential/fuzz do not.
- **CMake module-implementation-unit support** must be validated up front (Refactor-M0).
- Toolchain unchanged: **clang on Linux (Docker) is CI**; gcc named-modules remain blocked upstream and are not gated on.

---

## Status

- **Done & green (macOS clang/libc++ + Linux clang/libstdc++, 57 tests + 432 differential vectors):**
  - LTO wired (`CMAKE_INTERPROCEDURAL_OPTIMIZATION_RELEASE/RELWITHDEBINFO`).
  - Folder layout: `src/{util,buffer,codec,proto/{fields,exts,network,transport,declare,interest}}/` + `src/zenoh.cppm` umbrella; old flat files removed.
  - De-templating onto concrete `ByteReader`/`ByteWriter` (renamed from `Slice*`); `Readable`/`Writable` kept as `static_assert`ed concepts.
  - Hot/primitive layer (`util`/`buffer`/`varint`/`codec`/`codec.ext`) kept `inline` in `.cppm`; docs added.
  - **`.cpp` split validated end-to-end on `proto.fields`** (interface decls in `.cppm`, bodies in `fields.cpp`) — the M0 gate — green on both toolchains.
- **Done (continued):**
  - `.cpp` split completed for **all** message modules (`fields`, `exts`, `network`,
    `transport`, `declare`, `interest`) — interface declarations in `.cppm`,
    bodies in `.cpp`; `body_len`/`encoded_len`, `operator==`, `<T>`-templated and
    variant-dispatch (`PushBody`/`ResponseBody`/`DeclareBody`) kept inline. All
    `.cpp` wired into the CMake `PRIVATE` source list.
  - Documentation: `///` on the public API across modules, `//` on `.cpp`
    internals, file headers, and `docs/STYLE.md` (style + doc standard).
  - Verified: **57 tests + 432 differential vectors green** on macOS (clang/libc++)
    and Linux (clang/libstdc++); **16.7M fuzz executions clean** (LTO-enabled
    RelWithDebInfo build). The restructure is complete.

## Summary of changes from the first draft (post-review)
- Decision A is now the **hybrid** (de-template; hot/primitive/`<T>`/variant bodies stay `inline`; only message-layer plain bodies move to `.cpp`) — not "everything to `.cpp`", which would out-of-line the hot path and depend on LTO.
- Added `zenoh.varint` (was dropped) under `codec/`.
- Called out `body_len`, `<T>`-templated helpers, and variant dispatchers as must-stay-inline.
- M0/CI corrected to clang-only and made representative; added clean-rebuild/ODR gate.
- Corrected the "only CMake/tests unchanged" claim for the `Slice*`→`Byte*` rename.
- `ByteReader`/`ByteWriter` are public; doc enforcement is a checklist / `-Wdocumentation`, not a regex lint.

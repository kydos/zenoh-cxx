# Code style

## Functions
Every function and member function uses **trailing return type**: `auto f(args) -> R`.
Constructors/destructors are exempt. (See `RESTRUCTURE.md`/memory.)

## Module / file organization
- One **folder per functional area** under `src/` (`util/`, `buffer/`, `codec/`,
  `proto/<area>/`), plus the public umbrella `src/zenoh.cppm`.
- Module **interface units** (`.cppm`) hold the public, documented API: types, and
  function/method *declarations*.
- Message-layer `encode`/`decode` *definitions* live in **implementation units**
  (`.cpp`, `module <name>;`). The hot primitive/codec layer (`util`, `buffer`,
  `varint`, `codec`, `codec.ext`) and the inline-critical members — `body_len`/
  `encoded_len`, `operator==`, `<T>`-templated helpers, and the variant-dispatch
  `PushBody`/`ResponseBody`/`DeclareBody` `encode`/`decode` — stay **inline** in
  the `.cppm`. LTO recovers cross-module inlining for the `.cpp` bodies.

## Documentation
- **File header** on every `.cppm`/`.cpp`: one paragraph on the area's responsibility.
- **Public API** (every `export`ed type, function, enum, and notable field): a `///`
  doc comment stating purpose, parameters, the `std::expected` error conditions, and
  the wire-format reference (message id / header-flag layout) where applicable.
- **Private API** (`.cpp` internals, file-local helpers): `//` comments explaining
  non-obvious logic — bit math, ordering quirks (e.g. "nodeid written last"), the
  contiguous-frame assumption.
- The `///` form is Doxygen-ready. Enforcement is a review checklist; optionally
  `clang -Wdocumentation` (which understands declarations) — not a regex lint, since
  the `export namespace { ... }` form exports without per-entity `export` keywords.

## Errors & buffers
`std::expected<T, CodecError>` on the codec path (no exceptions) with the `ZTRY`
short-circuit macro. The codec reads/writes the concrete `ByteReader`/`ByteWriter`
(contiguous-frame contract); messages are borrow-only views into the source buffer.

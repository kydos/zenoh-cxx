# zenoh-proto (C++23) — codec & messages

The `zenoh-proto` static library is the Zenoh **v9** wire codec: message types and
their encode/decode, with no I/O, transport, or runtime dependencies. It is built
entirely from C++23 named modules and is byte-for-byte compatible with the Rust
reference (`../zenoh-nostd`), verified by differential tests.

See `PLAN.md` for the design rationale and decision log.

## Module map

| Module | Contents |
| --- | --- |
| `zenoh.util` | `CodecError`, `ByteField`, little-endian helpers |
| `zenoh.buffer` | `Readable`/`Writable` concepts, `SliceReader`/`SliceWriter` (zero-copy) |
| `zenoh.varint` | VLE (`zint`) `vle_len`/`encode_vle`/`decode_vle` |
| `zenoh.codec` | primitive codecs: uints, length-prefixed/raw bytes & text, LE u16 |
| `zenoh.codec.ext` | optional-field ("extension") helpers: header byte, Unit/U64/ZStruct read/write, `skip_ext` |
| `zenoh.proto.fields` | value types: `WireExpr`, `QoS`, `NodeId`, `Encoding`, `Timestamp`, `ZenohId`, `Duration`, enums |
| `zenoh.proto.exts` | structured extension bodies: `SourceInfo`, `EntityGlobalId`, `Attachment`, `Value`, `QueryableInfo` |
| `zenoh.proto.network` | `Put`/`Del`/`Push`, `Query`/`Request`, `Err`/`Reply`/`Response`, `ResponseFinal` |
| `zenoh.proto.transport` | `Init{Syn,Ack}`, `Open{Syn,Ack}`, `Close`, `KeepAlive`, `FrameHeader` |
| `zenoh.proto.declare` | `Declare` + 9 declaration bodies |
| `zenoh.proto.interest` | `Interest`, `InterestFinal` |
| `zenoh.proto` | umbrella: re-exports all message modules + `CodecError` + `ByteReader`/`ByteWriter`. **Import this for the protocol layer.** |
| `zenoh` | user-facing runtime: the client `Session` (`put`/`try_put`) over a TCP transport to a router. See `docs/RUNTIME.md`. Does not expose protocol messages. |

`include/zenoh/detail/try.hpp` provides the `ZTRY` short-circuit macro.

## Coverage vs. zenoh-rust (protocol v9)

Status as of this writing. "Done" = implemented and **differentially verified
byte-identical** against the full zenoh-rust codec (`tools/vector-gen`).

| Layer | Done ✅ | Not yet implemented ❌ |
| --- | --- | --- |
| Transport | `InitSyn`, `InitAck`, `OpenSyn`, `OpenAck`, `Close`, `KeepAlive`, `FrameHeader` | `Fragment` (0x06), `Oam` (0x00), `Join` (0x07) |
| Scouting | — | `Scout` (0x01), `HelloProto` (0x02) — whole layer |
| Network | `Push`, `Request`, `Response`, `ResponseFinal`, `Declare` (+ all 9 bodies), `Interest`, `InterestFinal` | `Oam` (0x1f) (incl. link-state OAM body) |
| Bodies | `Put`, `Del`, `Query`, `Err`, `Reply` | — |

**Extensions/behaviors not modeled:** Init `RegionName` (0x8), Open `RemoteBound`
(0x7); `ext_unknown` passthrough (unknown non-mandatory exts are decoded-and-
skipped, not preserved); SHM-gated extensions. Note: TCP stream framing prefixes
each transport message with a little-endian u16 length — a transport-layer concern,
not part of any message struct.

**Suggested next order:** `Fragment` + Init/Open exts (small, high interop value) →
scouting (`Scout`/`Hello`) → network/transport `Oam` → `Join`.

## Conventions

- **Style:** every function uses trailing return type (`auto f(args) -> R`).
- **Errors:** `std::expected<T, CodecError>`; no exceptions on the codec path.
- **Borrow-only:** decoded messages hold `std::span`/`std::string_view` views into
  the source buffer and are valid only while it lives.
- **Flat codecs:** each message has hand-written `encode(writer)` and
  `decode(reader) -> std::expected<Msg, CodecError>` that read like the wire
  diagram. "Extensions" are plain optional fields; there is no codec framework.
- **Strict decoding:** malformed/truncated input is rejected, never silently
  accepted. VLE values that exceed a field's width (e.g. a u16 `scope`, u32 id)
  are `malformed`, not truncated; out-of-range enums (`ConsolidationMode`,
  `QueryTarget`, `WhatAmI`) are rejected; text fields (keyexpr suffix, query
  parameters) are UTF-8-validated (`invalid_field`), matching the reference's
  `&str` decode. Use `get_uint_as<T>` / `read_ext_uint<T>` for narrowed fields.
  (Reserved header flag bits are tolerated/ignored for forward compatibility.)
- **Extensions are checked by KIND, not just by id.** The KIND bits determine an
  extension's length, so reading a known id as the shape the decoder *expected*
  rather than the shape the peer *sent* desynchronizes everything after it —
  `read_ext_{unit,u64,zstruct}` therefore reject a header whose KIND is not theirs
  (`take_ext_header`). The reference gets this structurally: its typed readers compare
  the whole header byte bar the Z flag, so id, mandatory bit and encoding must all
  match. This codec deliberately does not police the mandatory bit, which cannot
  affect framing.
- **A body with no extensions of its own still reads the chain.** If the Z bit is set,
  the extensions are consumed (skipping unknown non-mandatory ones, rejecting
  mandatory ones) — otherwise they are left to be misparsed as the next message.
- One deliberate divergence from the reference: an `Encoding` id wider than `u16` is
  `malformed` here, where the reference bounds the VLE to `u32` and then truncates.
  Truncating changes the message and, since `Put`/`Err` elide a default encoding,
  can drop the field entirely when a broker re-encodes on relay.

## Adding a message

1. Add the struct (plain members; `std::optional<T>` for optional fields) in the
   appropriate `zenoh.proto.*` module, with `encode`/`decode` and `operator==`.
2. Build the header byte from named flag bits; gate optional body fields on those
   bits; emit extensions in ascending id with the `more` bit set on all but the
   last present one (see existing messages for the idiom).
3. Add a golden vector in `tools/vector-gen` and a differential test
   (`tests/test_diff.cpp`) plus a round-trip test.

## Building & testing

```sh
# macOS dev (clang, ASan/UBSan):
cmake --preset clang && cmake --build build/clang && ctest --test-dir build/clang

# macOS release — fastest code (O3 + -DNDEBUG + native CPU tuning + ThinLTO):
cmake --preset clang-release && cmake --build build/clang-release
ctest --test-dir build/clang-release

# Linux (Docker, clang + libstdc++): build + full test suite
scripts/docker-ci.sh
```

The release preset sets `ZENOH_NATIVE=ON`, which tunes for the host CPU
(`-mcpu=native` on Apple Silicon / `-march=native` on x86) and is therefore **not
portable**. A bare `-DCMAKE_BUILD_TYPE=Release` stays portable (still O3 + NDEBUG +
ThinLTO); pass `-DZENOH_NATIVE=OFF` to drop host tuning explicitly.

## Test layers

- **Round-trip** (`tests/test_*.cpp`): encode → decode → structural equality, and
  `decode` consumes exactly the encoded length.
- **Primitive** (`tests/test_codec.cpp`, `tests/test_ext.cpp`): direct tests of the
  VLE/length-prefixed/UTF-8/u16 codecs and the extension (Unit/U64/ZStruct) helpers,
  including their error and overflow paths.
- **Runtime integration** (`tests/test_session.cpp`): an in-process *fake router* on
  a loopback socket drives a real `Session` through the handshake, `put`/`try_put`/
  `batch`/`close`, and the error paths (bad endpoint, refused connect, oversize
  payload) — no external `zenohd` required. Coverage: see below.
- **Differential** (`tests/test_diff.cpp` + `tests/diff_vectors.hpp`): the oracle is
  the **authoritative zenoh-rust** codec (`../zenoh-rust/commons/zenoh-protocol` +
  `zenoh-codec`). `tools/vector-gen` uses zenoh-rust's `rand()` constructors (behind
  its `test` feature) to emit random instances of every implemented message type —
  clearing fields we don't yet model (`ext_unknown`, Init `RegionName`, Open
  `RemoteBound`) — and the C++ side decodes each and asserts a byte-identical
  re-encode. `rand()` is non-seedable, so the committed header is a fixed snapshot;
  regenerate with:
  ```sh
  cargo run --manifest-path tools/vector-gen/Cargo.toml > tests/diff_vectors.hpp
  ```
- **Negative** (`tests/test_negative.cpp`): malformed input is rejected with a
  `CodecError`; truncation sweeps decode every prefix without reading out of bounds.
- **Fuzzing** (`fuzz/fuzz_decode.cpp`): libFuzzer over every decoder, checking
  no crash/UB and that `decode∘encode∘decode` is stable. Run in the Linux image:
  ```sh
  docker run --rm -v "$PWD":/work -w /work -u "$(id -u):$(id -g)" -e HOME=/tmp \
      zenoh-linux-ci scripts/fuzz.sh 60
  ```

## Coverage

`scripts/coverage.sh <preset>` (LLVM source-based coverage; tests/examples/fuzz
excluded) emits `build/<preset>/coverage.lcov`, which CI uploads to Codecov. The
library is at **~89% line coverage**; `codecov.yml` gates the project at 80%. The
lowest-covered area is `src/runtime/tcp.cpp` (socket error branches — EINTR/EAGAIN/
partial-write — that need fault injection to exercise).

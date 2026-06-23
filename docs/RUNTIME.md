# zenoh runtime (C++23) — client session

The `zenoh` library is the user-facing runtime, layered on the I/O-free `zenoh.proto`
codec. Its first slice is a **client** `Session`: a TCP transport to a Zenoh router
plus `put` / `try_put`. It follows the vertically-integrated design (PLAN.md D8) — the
session owns the socket, the protocol state, and the encode buffers, and drives
`encode → send` directly, with no sans-IO state machine in between.

A client communicates **only through a router** (`zenohd`); there is no scouting or
peer-to-peer layer here. Verified interoperable against the Rust reference router and
subscriber (`../zenoh-rust`).

## Module map

| Module | Unit | Contents |
| --- | --- | --- |
| `zenoh.runtime.tcp` | `src/runtime/tcp.{cppm,cpp}` | `TcpLink` (RAII POSIX socket), `IoError`. Blocking `write_all`/`read_exact`, non-blocking `write_some`. POSIX headers stay in the `.cpp`. |
| `zenoh.session` | `src/runtime/session.{cppm,cpp}` | `Session`, `ZError`. Endpoint parsing, the 4-way handshake, `put`/`try_put`/`close`. |
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

## TCP framing

Each TCP batch is prefixed by a **2-byte little-endian length** covering the bytes
that follow (per-batch, max 65535). A published sample is one batch:

```
[u16 LE len] [FrameHeader 0x05] [Push 0x1d | M | N] [WireExpr] [Put 0x01] [payload]
```

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

- A `Batch::put` appends a `Push(Put)` to the in-progress frame.
- When the next put would exceed the negotiated batch size, the buffered frame is
  **sent first** (blocking) and the new put begins the next frame — so an arbitrarily
  long run of puts streams out as a sequence of full frames.
- `flush()` sends whatever is buffered; the **destructor flushes** too (best-effort —
  call `flush()` explicitly if you need to observe the result).
- A single put too large to fit one frame on its own returns `ZError::encode_error`.
- `put`/`flush` block like `Session::put`. The batch routes through the session's SN
  counter and pending-buffer, so interleaving `session->put()` and batch puts stays
  wire-consistent. The session must outlive any batch created from it.

### `ZError`

`would_block` (try_put backpressure), `connection_closed`, `io_error`,
`protocol_error` (bad handshake reply), `encode_error` (message exceeds a batch),
`bad_endpoint` (unparseable/unresolvable locator).

## Example & manual interop test

`examples/z_put.cpp` (`z_put`):
`z_put [endpoint] [key] [value] [--try] [--batch] [--count N]`. With `--batch` the
puts are coalesced into API-level batches (one Frame per batch).

`examples/z_pub.cpp` (`z_pub`) — the C++ equivalent of zenoh-rust's `z_pub`:
publishes `"[<idx>] <payload>"` to a key once a second, forever.
`z_pub [-e endpoint] [-k key] [-p payload]`. The reference also sets a TEXT_PLAIN
encoding and an optional attachment and supports a matching listener; `put` carries
no such metadata or subscription matching yet, so those knobs are omitted.

`examples/z_put_float.cpp` (`z_put_float`) — the C++ equivalent of zenoh-rust's
`z_put_float`: publishes a single `double`. The payload is the 8 little-endian bytes
of the value (`f64::to_le_bytes()`), the exact layout zenoh-ext's `z_serialize`
produces, so a reference subscriber can `z_deserialize::<f64>` it.
`z_put_float [-e endpoint] [-k key] [-p value]`.

`examples/z_pub_thr.cpp` (`z_pub_thr`) — the C++ equivalent of zenoh-rust's
`z_pub_thr`: publishes a fixed-size payload to `test/thr` in a tight loop and, with
`-t`, prints `msg/s` every `-n N` messages.
`z_pub_thr [-e endpoint] [-t] [-n N] [--batch B] <payload_size>`. Blocking `put`
matches the reference's `CongestionControl::Block`; `--batch B` coalesces B puts per
flush (markedly higher throughput). Measure with the reference `z_sub_thr`:

```sh
zenohd -l tcp/127.0.0.1:7447 &
z_sub_thr -m client -e tcp/127.0.0.1:7447 &        # from ../zenoh-rust
./build/clang/examples/z_pub_thr -e tcp/127.0.0.1:7447 -t 8            # single puts
./build/clang/examples/z_pub_thr -e tcp/127.0.0.1:7447 -t --batch 256 8  # batched
```

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

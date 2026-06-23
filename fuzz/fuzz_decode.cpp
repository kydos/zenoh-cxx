// libFuzzer target over the message decoders.
//
// For arbitrary input it: (1) decodes the bytes as every top-level message type
// (decoders must never crash or read out of bounds -- enforced by ASan/UBSan),
// and (2) for each successful decode, re-encodes and decodes again, asserting the
// result is structurally equal (decode is stable over encode).
//
// Build/run via scripts/fuzz.sh (Linux clang). Gated by -DZENOH_FUZZ=ON.
import zenoh.proto.network;
import zenoh.proto.transport;
import zenoh.proto.declare;
import zenoh.proto.interest;
import zenoh.buffer;
import zenoh.util;

#include <cstddef>
#include <cstdint>
#include <span>

using namespace zenoh;

namespace {

// Scratch big enough that re-encoding never fails for fuzzer-sized inputs.
std::byte g_scratch[1u << 20];

template <class T>
auto exercise(std::span<const std::byte> in) -> void {
    ByteReader r{in};
    auto decoded = T::decode(r);
    if (!decoded) return;

    ByteWriter w{g_scratch};
    if (!decoded->encode(w)) return; // input larger than scratch; skip the invariant

    ByteReader r2{std::span<const std::byte>{g_scratch, w.written()}};
    auto again = T::decode(r2);
    if (!again || !(*again == *decoded)) {
        __builtin_trap(); // decode(encode(decode(x))) != decode(x)
    }
}

} // namespace

extern "C" auto LLVMFuzzerTestOneInput(const std::uint8_t* data, std::size_t size) -> int {
    std::span<const std::byte> in{reinterpret_cast<const std::byte*>(data), size};

    exercise<Put>(in);
    exercise<Push>(in);
    exercise<Query>(in);
    exercise<Request>(in);
    exercise<Reply>(in);
    exercise<Err>(in);
    exercise<Response>(in);
    exercise<ResponseFinal>(in);
    exercise<Declare>(in);
    exercise<Interest>(in);
    exercise<InterestFinal>(in);
    exercise<InitSyn>(in);
    exercise<InitAck>(in);
    exercise<OpenSyn>(in);
    exercise<OpenAck>(in);
    exercise<Close>(in);
    exercise<KeepAlive>(in);
    exercise<FrameHeader>(in);

    return 0;
}

// Unit tests for the per-subscriber Strand (SUBSCRIBER.md): bounded ordered FIFO and
// the last-value conflation discipline (overwrite most-recent + re-tail), exercised on
// a plain value type so the structure is tested independently of Sample/proto.
import zenoh.runtime.strand;

#include "ztest.hpp"

#include <optional>
#include <string>
#include <vector>

using namespace zenoh;

namespace {

// Drain the whole strand into a vector, head-first.
template <class T> auto drain(Strand<T>& s) -> std::vector<T> {
    std::vector<T> out;
    while (auto v = s.pop()) out.push_back(*v);
    return out;
}

} // namespace

TEST("ordered strand is a FIFO and reports full at capacity") {
    Strand<std::string> s(3, StrandMode::ordered);
    CHECK(s.empty());
    CHECK(s.post("k1", "a") == PostResult::posted);
    CHECK(s.post("k2", "b") == PostResult::posted);
    CHECK(s.post("k3", "c") == PostResult::posted);
    CHECK(s.size() == 3);
    // Full, and ordered never conflates even on a repeated key.
    CHECK(s.post("k1", "d") == PostResult::full);
    CHECK(s.size() == 3);

    auto v = drain(s);
    CHECK(v.size() == 3);
    if (v.size() == 3) CHECK(v[0] == "a" && v[1] == "b" && v[2] == "c");
    CHECK(s.empty());
}

TEST("ordered strand keeps duplicate keys (no conflation)") {
    Strand<std::string> s(4, StrandMode::ordered);
    CHECK(s.post("k", "1") == PostResult::posted);
    CHECK(s.post("k", "2") == PostResult::posted);
    CHECK(s.post("k", "3") == PostResult::posted);
    auto v = drain(s);
    CHECK(v.size() == 3);
    if (v.size() == 3) CHECK(v[0] == "1" && v[1] == "2" && v[2] == "3");
}

TEST("last_value: full fidelity while not full") {
    Strand<std::string> s(4, StrandMode::last_value);
    CHECK(s.post("k", "1") == PostResult::posted);
    CHECK(s.post("k", "2") == PostResult::posted); // room -> appended, not conflated
    CHECK(s.size() == 2);
    auto v = drain(s);
    CHECK(v.size() == 2);
    if (v.size() == 2) CHECK(v[0] == "1" && v[1] == "2");
}

TEST("last_value: conflation overwrites the most-recent occurrence and re-tails") {
    // Capacity 4, fill with K,K,J,L. Arrival order: v1,v2,a,b then a 5th value v3 for K.
    Strand<std::string> s(4, StrandMode::last_value);
    CHECK(s.post("K", "v1") == PostResult::posted);
    CHECK(s.post("K", "v2") == PostResult::posted);
    CHECK(s.post("J", "a") == PostResult::posted);
    CHECK(s.post("L", "b") == PostResult::posted);
    // Full. New K -> conflate the most-recent K (v2), splice to tail.
    CHECK(s.post("K", "v3") == PostResult::conflated);
    CHECK(s.size() == 4);

    // Expect an ordered subsequence of the arrival stream v1,v2,a,b,v3: v1, a, b, v3.
    auto v = drain(s);
    CHECK(v.size() == 4);
    if (v.size() == 4) {
        CHECK(v[0] == "v1"); // older K kept in place
        CHECK(v[1] == "a");
        CHECK(v[2] == "b");
        CHECK(v[3] == "v3"); // newest K delivered last (true newest position)
    }
}

TEST("last_value: blocks (full) when full and the key is absent") {
    Strand<std::string> s(2, StrandMode::last_value);
    CHECK(s.post("K", "1") == PostResult::posted);
    CHECK(s.post("J", "2") == PostResult::posted);
    // Full, new distinct key -> nothing to conflate.
    CHECK(s.post("M", "3") == PostResult::full);
    CHECK(s.size() == 2);
    // But an existing key still conflates.
    CHECK(s.post("K", "1b") == PostResult::conflated);
    auto v = drain(s);
    CHECK(v.size() == 2);
    if (v.size() == 2) CHECK(v[0] == "2" && v[1] == "1b"); // J kept, K re-tailed
}

TEST("last_value: sustained pressure on a hot key converges to one survivor") {
    Strand<int> s(2, StrandMode::last_value);
    CHECK(s.post("a", 1) == PostResult::posted);
    CHECK(s.post("b", 2) == PostResult::posted);
    // 'a' keeps updating under full -> conflates, never grows past capacity.
    for (int i = 3; i < 100; ++i) CHECK(s.post("a", i) == PostResult::conflated);
    CHECK(s.size() == 2);
    auto v = drain(s);
    CHECK(v.size() == 2);
    if (v.size() == 2) CHECK(v[0] == 2 && v[1] == 99); // b (kept), then a's latest
}

TEST("last_value: index is maintained across pops (re-conflation after drain)") {
    Strand<std::string> s(2, StrandMode::last_value);
    CHECK(s.post("K", "1") == PostResult::posted);
    CHECK(s.post("K", "2") == PostResult::posted); // two K nodes pending
    CHECK(s.pop().value() == "1");                 // pop older K; index still valid
    CHECK(s.post("J", "x") == PostResult::posted); // room again
    // Full now (K=2, J=x). Conflating K must still find the surviving K node.
    CHECK(s.post("K", "3") == PostResult::conflated);
    auto v = drain(s);
    CHECK(v.size() == 2);
    if (v.size() == 2) CHECK(v[0] == "x" && v[1] == "3");
}

TEST("capacity is clamped to at least one") {
    Strand<int> s(0, StrandMode::ordered);
    CHECK(s.capacity() == 1);
    CHECK(s.post("k", 7) == PostResult::posted);
    CHECK(s.post("k", 8) == PostResult::full);
}

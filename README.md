# zenoh

[![CI](https://github.com/kydos/zenoh/actions/workflows/ci.yml/badge.svg)](https://github.com/kydos/zenoh/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/kydos/zenoh/branch/main/graph/badge.svg)](https://codecov.io/gh/kydos/zenoh)
[![License: AGPL v3](https://img.shields.io/badge/License-AGPL_v3-blue.svg)](https://www.gnu.org/licenses/agpl-3.0)
[![C++23](https://img.shields.io/badge/C%2B%2B-23-blue.svg)](https://en.cppreference.com/w/cpp/23)
[![code style: clang-format](https://img.shields.io/badge/code%20style-clang--format-blue.svg)](.clang-format)

The specification and a modern C++23 implementation of the Zenoh protocol.

Three static libraries, built from one CMake project:

- **`zenoh-proto`** — the pure, I/O-free wire codec plus key-expression matching
  (`zenoh.ke`). No sockets, no allocation surprises.
- **`zenoh`** — the client runtime (`Session`): connects to a router, put/get,
  subscribe/queryable. See [`docs/RUNTIME.md`](docs/RUNTIME.md).
- **`zenoh-broker`** — `zenohb`, a multithreaded router built on standalone ASIO.
  See [`docs/BROKER.md`](docs/BROKER.md).

## Prerequisites

- **CMake 3.28+** (C++23 named-module support)
- **Ninja** — required; the module dependency scanning relies on it
- **Clang 18+**
  - macOS: `brew install llvm` (use the Homebrew `clang++`)
  - Linux: Clang **with libstdc++** — easiest via the Docker image below

> **GCC is not supported** for this build: a libstdc++ `<type_traits>` bug breaks
> named modules with g++. Use Clang.

## Building

The build uses C++23 named modules, so it always needs Ninja + Clang. The workflow is
the same on every platform: configure a preset, build it, test it.

```sh
cmake --preset <preset>          # configure  -> build/<preset>/
cmake --build --preset <preset>  # build
ctest --preset <preset>          # run the test suite
```

### macOS

| Preset | Build type | Notes |
| --- | --- | --- |
| `clang` | Debug | ASan + UBSan — for development |
| `clang-release` | Release | `-O3 -DNDEBUG`, native CPU tuning (`-mcpu=native`/`-march=native`) and ThinLTO — fastest, host-specific (non-portable) binary |

```sh
# Debug (development)
cmake --preset clang
cmake --build --preset clang
ctest --preset clang

# Release (fastest)
cmake --preset clang-release
cmake --build --preset clang-release
ctest --preset clang-release
```

For a **portable** release (drop the host-CPU tuning, keep `-O3 -DNDEBUG` + ThinLTO):

```sh
cmake -S . -B build/release -G Ninja \
    -DCMAKE_CXX_COMPILER=clang++ -DCMAKE_BUILD_TYPE=Release   # ZENOH_NATIVE defaults OFF
cmake --build build/release
```

### Linux

Linux builds run inside the provided Docker image (Clang + libstdc++). The helper
builds the image and runs configure + build + test; artifacts land in `build/linux-*`.

```sh
scripts/docker-ci.sh                       # Debug  (linux-clang): build + test
scripts/docker-ci.sh linux-clang-release   # Release (linux-clang-release): build + test
scripts/docker-ci.sh linux-tsan            # ThreadSanitizer: the broker's concurrency gate
```

If you already have Clang + libstdc++ on the host, you can use the presets directly
instead of Docker:

```sh
cmake --preset linux-clang-release
cmake --build --preset linux-clang-release
ctest --preset linux-clang-release
```

### Reconfiguring

After editing `CMakeLists.txt` just rebuild — Ninja re-runs CMake automatically. To
wipe a stale cache (e.g. when a flag was removed from a preset), reconfigure fresh:

```sh
cmake --preset <preset> --fresh
```

## Testing

`ctest` registers one case per test, so the suite reports test-by-test and can be
narrowed or run in parallel:

```sh
ctest --preset clang                  # every test, one ctest case each
ctest --preset clang -j8              # ... in parallel
ctest --preset clang -R 'is_canon'    # only cases matching a regex
ctest --preset clang -L '^test_ke$'   # only cases from tests/test_ke.cpp
```

The test binary can also be run directly, which prints an `ok`/`FAIL` line per case
with its elapsed time, grouped by source file:

```sh
build/clang/tests/zenoh-tests                     # everything
build/clang/tests/zenoh-tests --filter test_ke    # cases whose id contains the text
build/clang/tests/zenoh-tests --list              # every case id, one per line
build/clang/tests/zenoh-tests --help
```

## Formatting & linting

Style is enforced with **clang-format** (`.clang-format`) and **clang-tidy**
(`.clang-tidy`); CI runs both. Locally:

```sh
scripts/format.sh          # check formatting (CI mode)
scripts/format.sh --fix    # reformat in place

# clang-tidy needs a built tree for its compile database (and the module BMIs):
cmake --preset clang && cmake --build build/clang
scripts/tidy.sh build/clang
```

The generated `tests/diff_vectors.hpp` is excluded from formatting
(`.clang-format-ignore`).

## Running the broker

`zenohb` is a multithreaded ASIO-based router — every preset builds it to
`build/<preset>/zenohb`, no extra flag needed:

```sh
./build/clang-release/zenohb -l tcp/0.0.0.0:7447 --threads 4
```

- `-l`/`--listen tcp/host:port` — listen endpoint (default `0.0.0.0:7447`).
- `--threads N` — size of the ASIO `io_context` thread pool (default:
  `std::thread::hardware_concurrency()`; `--threads 1` is a valid single-threaded
  configuration).

Point any client (this project's own examples below, or a real `zenoh-rust`
`z_pub`/`z_sub`/`z_get`/`z_queryable` — pass `-m client` to those, since their CLI
defaults to peer mode) at that endpoint instead of a real `zenohd`. Full routing
semantics, the concurrency model, and the wire extensions are documented in
[`docs/BROKER.md`](docs/BROKER.md).

**Use a Release build (`clang-release`/`linux-clang-release`) for anything
throughput-sensitive.** The `clang`/`linux-clang` presets are ASan+UBSan-instrumented
for correctness testing, not speed — running a real max-rate benchmark (e.g.
`zenoh-rust`'s `z_pub_thr`/`z_sub_thr`) against the ASan build can fall behind and
trip the peer's own timeout, which looks like a broker bug but isn't; see
[`docs/BROKER.md`](docs/BROKER.md#performance-testing-use-a-release-build-not-the-asanubsan-dev-preset)
for the full story.

## Examples

Every preset builds runtime example programs under `build/<preset>/examples/`:
`z_put`, `z_pub`, `z_put_float`, `z_pub_thr`, `z_sub`, `z_sub_thr`. See
[`docs/RUNTIME.md`](docs/RUNTIME.md) for how to run them against a Zenoh router (this
project's own `zenohb`, above, or a real `zenohd`). Protocol/codec internals are
documented in [`docs/PROTO.md`](docs/PROTO.md).

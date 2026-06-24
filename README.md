# zenoh

The specification and a modern C++23 implementation of the Zenoh protocol.

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

## Examples

The release/debug builds produce runtime example programs under
`build/<preset>/examples/` (`z_put`, `z_pub`, `z_put_float`, `z_pub_thr`, `z_sub`). See [`docs/RUNTIME.md`](docs/RUNTIME.md)
for how to run them against a Zenoh router. Protocol/codec internals are documented in
[`docs/PROTO.md`](docs/PROTO.md).

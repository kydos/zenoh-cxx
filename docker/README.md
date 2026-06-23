# Linux build & test (Docker)

Linux is the primary target. This image provides a reproducible Linux environment.

| Toolchain          | Preset                 | Status                                      |
| ------------------ | ---------------------- | ------------------------------------------- |
| clang + libstdc++  | `linux-clang`          | **Primary, validated** — Debug, ASan+UBSan  |
| clang + libstdc++  | `linux-clang-release`  | `-O3 -march=native` (perf)                  |
| g++-14             | `linux-gcc`            | **Blocked** (toolchain bug, see below)      |

## Toolchain status (C++23 named modules)

Module support across stdlibs/compilers is still maturing; we tested all four
combinations on this image (clang-19, g++-14):

- **clang + libstdc++ — works.** This is the validated Linux path.
- clang + libc++ — broken on clang-19 (libc++ internal headers such as
  `<__expected/unexpect.h>` clash when pulled into a module's global module
  fragment and re-included by a consumer). Works on newer clang (e.g. clang 22,
  used for the macOS dev build), so this is a clang/libc++ *version* issue.
- g++ + libstdc++ — broken on **both g++-14 and g++-15**: libstdc++'s
  `<type_traits>` (the `common_reference`/`__condres_cvref` machinery) fails to
  compile inside a module global-module-fragment. Since every std header pulls in
  `<type_traits>`, this cannot be worked around from our side. Tracked as an
  upstream gcc/libstdc++ bug; `linux-gcc` will be enabled once it is fixed.

So `scripts/ci.sh` defaults to `linux-clang` only. Pass `linux-gcc` explicitly to
retry the gcc path against a future toolchain.

## Quick start

```sh
# Build the image and run the full Linux CI (linux-clang + linux-gcc):
scripts/docker-ci.sh

# A single preset:
scripts/docker-ci.sh linux-clang

# Or via docker compose:
docker compose run --rm ci
docker compose run --rm dev        # interactive shell in the toolchain
```

`scripts/docker-ci.sh` builds `docker/Dockerfile`, mounts the repo at `/work`, and
runs `scripts/ci.sh` (configure → build → `ctest`) as the host user. Linux build
artifacts go to `build/linux-clang` and `build/linux-gcc`, separate from any local
macOS builds under `build/clang`.

## Inside the container

```sh
# (from `docker compose run --rm dev`, working dir /work)
scripts/ci.sh                 # both presets
cmake --preset linux-clang && cmake --build --preset linux-clang && ctest --preset linux-clang
```

#!/usr/bin/env bash
#
# Configure, build, and test one or more CMake presets.
# Defaults to the Linux pair (clang + gcc) used inside the Docker image.
#
# Usage:
#   scripts/ci.sh                       # linux-clang then linux-gcc
#   scripts/ci.sh linux-clang           # a single preset
#   scripts/ci.sh clang                 # the local macOS preset
#
set -euo pipefail
cd "$(dirname "$0")/.."

# Default to clang only: gcc named-module builds are currently blocked by a
# libstdc++ <type_traits> bug (see docker/README.md). Pass "linux-gcc" explicitly
# to attempt it once the toolchain is fixed.
presets=("$@")
if [ ${#presets[@]} -eq 0 ]; then
    presets=(linux-clang)
fi

for p in "${presets[@]}"; do
    # --fresh wipes any stale CMakeCache so a removed/changed flag can't linger
    # (cache vars dropped from a preset otherwise persist across reconfigures).
    echo "::group::[$p] configure"
    cmake --preset "$p" --fresh
    echo "::endgroup::"

    echo "::group::[$p] build"
    cmake --build --preset "$p"
    echo "::endgroup::"

    echo "::group::[$p] test"
    ctest --preset "$p"
    echo "::endgroup::"
done

echo "OK - all presets passed: ${presets[*]}"

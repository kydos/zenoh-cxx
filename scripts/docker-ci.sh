#!/usr/bin/env bash
#
# Build the Linux toolchain image and run the CI script inside it.
# Linux build artifacts land in build/linux-* (separate from any host builds).
#
# Usage:
#   scripts/docker-ci.sh                 # build image, run linux-clang + linux-gcc
#   scripts/docker-ci.sh linux-clang     # a single preset
#
set -euo pipefail
cd "$(dirname "$0")/.."

IMAGE=zenoh-linux-ci

# Retried on failure — apt.llvm.org outages otherwise fail the run (see the script).
scripts/docker-image.sh "$IMAGE"

# Mount the repo and run as the host user so generated files aren't root-owned.
docker run --rm \
    -v "$PWD":/work -w /work \
    -u "$(id -u):$(id -g)" \
    -e HOME=/tmp \
    "$IMAGE" \
    scripts/ci.sh "$@"

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

docker build -t "$IMAGE" -f docker/Dockerfile .

# Mount the repo and run as the host user so generated files aren't root-owned.
docker run --rm \
    -v "$PWD":/work -w /work \
    -u "$(id -u):$(id -g)" \
    -e HOME=/tmp \
    "$IMAGE" \
    scripts/ci.sh "$@"

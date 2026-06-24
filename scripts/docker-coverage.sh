#!/usr/bin/env bash
#
# Build the Linux toolchain image and produce an lcov coverage report at
# build/linux-coverage/coverage.lcov by running scripts/coverage.sh inside it.
# The report lands on the host (the repo is bind-mounted), where CI uploads it to
# Codecov.
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
    scripts/coverage.sh linux-coverage

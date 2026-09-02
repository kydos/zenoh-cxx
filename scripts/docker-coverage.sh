#!/usr/bin/env bash
#
# Build the Linux toolchain image and produce an lcov coverage report at
# build/linux-coverage/coverage.lcov by running scripts/coverage.sh inside it.
# The report lands on the host (the repo is bind-mounted), where CI uploads it to
# Codecov.
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
    scripts/coverage.sh linux-coverage

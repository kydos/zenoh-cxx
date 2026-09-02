#!/usr/bin/env bash
#
# Build the Linux toolchain image (docker/Dockerfile), retrying a few times before
# giving up. Shared by scripts/docker-ci.sh and scripts/docker-coverage.sh so both
# CI and local runs get the same behaviour.
#
# Why retry: the image installs clang from apt.llvm.org, which is periodically
# unreachable -- llvm.sh's own preflight then fails in under a second ("GPG key not
# reachable at https://apt.llvm.org/llvm-snapshot.gpg.key") and takes the whole CI
# run with it, even though nothing in the repo is broken (this failed a run on
# 2026-09-02 twelve minutes after the identical image had built fine). Layer caching
# makes a retry cheap: everything up to the failing RUN is reused, and an unchanged
# Dockerfile that is already built is a no-op.
#
# Usage: scripts/docker-image.sh [image-tag]   # default zenoh-linux-ci
set -euo pipefail
cd "$(dirname "$0")/.."

IMAGE="${1:-zenoh-linux-ci}"
ATTEMPTS="${DOCKER_BUILD_ATTEMPTS:-3}"
RETRY_DELAY="${DOCKER_BUILD_RETRY_DELAY:-60}"

for attempt in $(seq 1 "${ATTEMPTS}"); do
    if docker build -t "${IMAGE}" -f docker/Dockerfile .; then
        exit 0
    fi
    if [ "${attempt}" -lt "${ATTEMPTS}" ]; then
        echo "docker build failed (attempt ${attempt}/${ATTEMPTS}) — retrying in ${RETRY_DELAY}s" >&2
        sleep "${RETRY_DELAY}"
    fi
done

echo "error: docker build failed after ${ATTEMPTS} attempts" >&2
exit 1

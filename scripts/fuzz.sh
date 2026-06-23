#!/usr/bin/env bash
#
# Build and run the libFuzzer decode target. Intended to run inside the Linux
# toolchain image (clang has the fuzzer runtime); on the host use:
#   docker run --rm -v "$PWD":/work -w /work -u "$(id -u):$(id -g)" -e HOME=/tmp \
#       zenoh-linux-ci scripts/fuzz.sh 30
#
# Arg 1: max total seconds to fuzz (default 30).
set -euo pipefail
cd "$(dirname "$0")/.."

SECONDS_TO_RUN="${1:-30}"

cmake --preset linux-fuzz --fresh
cmake --build --preset linux-fuzz

mkdir -p build/fuzz-corpus
./build/linux-fuzz/zenoh-fuzz \
    -max_total_time="$SECONDS_TO_RUN" \
    -max_len=4096 \
    -print_final_stats=1 \
    build/fuzz-corpus

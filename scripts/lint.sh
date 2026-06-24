#!/usr/bin/env bash
#
# Run all linters: clang-format (check) then clang-tidy. Used by CI after the build
# step, and runnable locally once a build dir exists.
#
# Usage: scripts/lint.sh [build-dir]   # default build/linux-clang
set -euo pipefail
cd "$(dirname "$0")/.."

scripts/format.sh
scripts/tidy.sh "${1:-build/linux-clang}"

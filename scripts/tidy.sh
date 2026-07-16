#!/usr/bin/env bash
#
# Run clang-tidy over the library translation units (src/*.cpp, src/*.cppm,
# broker/src/*.cpp, broker/src/*.cppm) using a CMake build's compile database;
# headers under src/, include/, and broker/ are checked via the HeaderFilterRegex
# in .clang-tidy. Any diagnostic fails the run.
#
# The build dir must already be configured and built (clang-tidy replays the compiled
# module flags and needs the imported BMIs to exist).
#
# Usage: scripts/tidy.sh [build-dir]   # default build/linux-clang
set -euo pipefail
cd "$(dirname "$0")/.."

BUILD="${1:-build/linux-clang}"

LLVM_BIN="${LLVM_BIN:-}"
if [ -z "${LLVM_BIN}" ] && [ -d /opt/homebrew/opt/llvm/bin ]; then
    LLVM_BIN=/opt/homebrew/opt/llvm/bin
fi
TIDY="${CLANG_TIDY:-${LLVM_BIN:+${LLVM_BIN}/}clang-tidy}"

if [ ! -f "${BUILD}/compile_commands.json" ]; then
    echo "error: ${BUILD}/compile_commands.json not found — configure & build first" >&2
    exit 1
fi

status=0
while IFS= read -r f; do
    echo "tidy: ${f}"
    "${TIDY}" -p "${BUILD}" --quiet --warnings-as-errors='*' "${f}" || status=1
done < <(find src broker/src \( -name '*.cpp' -o -name '*.cppm' \) | sort)

if [ "${status}" -eq 0 ]; then echo "clang-tidy: clean"; fi
exit "${status}"

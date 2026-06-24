#!/usr/bin/env bash
#
# Run clang-format over the C++ sources. Default mode checks formatting and fails if
# any file is non-conforming; pass --fix to reformat in place. The generated
# tests/diff_vectors.hpp is excluded (see .clang-format-ignore).
#
# Usage:
#   scripts/format.sh         # check (CI)
#   scripts/format.sh --fix   # reformat in place
set -euo pipefail
cd "$(dirname "$0")/.."

# Prefer an explicit override, then Homebrew's LLVM on macOS, else PATH (the Docker
# image aliases the versioned binary to this name).
LLVM_BIN="${LLVM_BIN:-}"
if [ -z "${LLVM_BIN}" ] && [ -d /opt/homebrew/opt/llvm/bin ]; then
    LLVM_BIN=/opt/homebrew/opt/llvm/bin
fi
FMT="${CLANG_FORMAT:-${LLVM_BIN:+${LLVM_BIN}/}clang-format}"

find_sources() {
    find src include tests examples fuzz -type f \
        \( -name '*.cpp' -o -name '*.cppm' -o -name '*.hpp' -o -name '*.h' \) \
        -not -name 'diff_vectors.hpp'
}

if [ "${1:-}" = "--fix" ]; then
    find_sources | xargs "${FMT}" -i
    echo "clang-format: reformatted in place"
else
    find_sources | xargs "${FMT}" --dry-run --Werror
    echo "clang-format: all sources conform"
fi

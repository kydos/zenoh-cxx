#!/usr/bin/env bash
#
# Build with LLVM source-based coverage, run the test suite, and emit an lcov report
# at build/<preset>/coverage.lcov covering the library sources only (tests, examples,
# fuzz targets, and vendored third-party headers excluded). Runs anywhere the clang
# toolchain plus llvm-cov / llvm-profdata are available; in CI it runs inside the
# Docker image via scripts/docker-coverage.sh.
#
# Usage:
#   scripts/coverage.sh                 # default preset: linux-coverage
#   scripts/coverage.sh clang-coverage  # local macOS build
set -euo pipefail
cd "$(dirname "$0")/.."

PRESET="${1:-linux-coverage}"
BUILD="build/${PRESET}"
TEST_BIN="${BUILD}/tests/zenoh-tests"

# Pick llvm tools that match the clang that produced the profiles: an explicit
# override wins, then Homebrew's LLVM on macOS, else whatever is on PATH (the Docker
# image aliases the versioned binaries to these unversioned names).
LLVM_BIN="${LLVM_BIN:-}"
if [ -z "${LLVM_BIN}" ] && [ -d /opt/homebrew/opt/llvm/bin ]; then
    LLVM_BIN=/opt/homebrew/opt/llvm/bin
fi
PROFDATA="${LLVM_PROFDATA:-${LLVM_BIN:+${LLVM_BIN}/}llvm-profdata}"
COV="${LLVM_COV:-${LLVM_BIN:+${LLVM_BIN}/}llvm-cov}"

echo "::group::[${PRESET}] configure + build"
cmake --preset "${PRESET}" --fresh
cmake --build --preset "${PRESET}"
echo "::endgroup::"

echo "::group::[${PRESET}] test"
rm -f "${BUILD}"/cov-*.profraw "${BUILD}/coverage.profdata" "${BUILD}/coverage.lcov"
# One raw profile per test process (%p), written under the build dir.
export LLVM_PROFILE_FILE="${PWD}/${BUILD}/cov-%p.profraw"
ctest --preset "${PRESET}" --output-on-failure
echo "::endgroup::"

echo "::group::[${PRESET}] coverage report"
"${PROFDATA}" merge -sparse "${BUILD}"/cov-*.profraw -o "${BUILD}/coverage.profdata"
"${COV}" export "${TEST_BIN}" \
    -instr-profile="${BUILD}/coverage.profdata" \
    -format=lcov \
    -ignore-filename-regex='(tests|examples|fuzz|third_party)/' \
    > "${BUILD}/coverage.lcov"
# Make source paths repo-relative (strip the absolute working-dir prefix) so Codecov
# maps them onto the repo tree. -i.bak is portable across GNU and BSD sed.
sed -i.bak "s#SF:${PWD}/#SF:#" "${BUILD}/coverage.lcov" && rm -f "${BUILD}/coverage.lcov.bak"
echo "::endgroup::"

files=$(grep -c '^SF:' "${BUILD}/coverage.lcov" || true)
echo "Wrote ${BUILD}/coverage.lcov (${files} source files)"

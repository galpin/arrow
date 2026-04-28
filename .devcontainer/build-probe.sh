#!/usr/bin/env bash
# Configure and build arrow-flight-concurrency-probe inside the devcontainer.
# First run downloads and builds bundled gRPC / Protobuf / gflags and takes
# 15-30 minutes; subsequent runs are fast thanks to ccache.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${ROOT}/cpp/build"

mkdir -p "${BUILD_DIR}"
cd "${BUILD_DIR}"

cmake "${ROOT}/cpp" \
    -GNinja \
    -DCMAKE_BUILD_TYPE=Release \
    -DARROW_FLIGHT=ON \
    -DARROW_BUILD_BENCHMARKS=ON \
    -DARROW_BUILD_TESTS=OFF \
    -DARROW_BUILD_SHARED=ON \
    -DARROW_DEPENDENCY_SOURCE=BUNDLED \
    -DARROW_USE_CCACHE=ON

cmake --build . --target arrow-flight-concurrency-probe

BIN="${BUILD_DIR}/release/arrow-flight-concurrency-probe"
echo
echo "Built: ${BIN}"
echo
echo "Smoke test:"
echo "  ${BIN} --num_threads=1 --handler_sleep_ms=500"
echo
echo "Diagnostic matrix (out-of-the-box vs tuned):"
echo "  ${BIN} --num_threads=8 --shared_client=true"
echo "  ${BIN} --num_threads=8 --shared_client=false"
echo "  ${BIN} --num_threads=8 --num_cqs=4 --max_pollers=32"
echo "  ${BIN} --num_threads=32 --shared_client=false --num_cqs=4 --max_pollers=64"

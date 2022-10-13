#!/usr/bin/env bash

set -euo pipefail

if [[ "$(uname -m)" =~ (arm64|aarch64)$ ]]; then
  export CROSSLINUX_CONFIG="crosslinuxarm"
else
  export CROSSLINUX_CONFIG="crosslinux"
fi

bazel build --config=$CROSSLINUX_CONFIG --config=ci //pkg/cmd/cockroach-short \
      //pkg/cmd/roachtest \
      //pkg/cmd/workload

BAZEL_BIN=$(bazel info bazel-bin --config=$CROSSLINUX_CONFIG --config=ci)
$BAZEL_BIN/pkg/cmd/roachtest/roachtest_/roachtest run acceptance kv/splits cdc/bank \
  --local \
  --parallelism=1 \
  --cockroach "$BAZEL_BIN/pkg/cmd/cockroach-short/cockroach-short_/cockroach-short" \
  --workload "$BAZEL_BIN/pkg/cmd/workload/workload_/workload" \
  --artifacts /artifacts \
  --teamcity

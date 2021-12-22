#!/usr/bin/env bash

set -euo pipefail

bazel build --config=crosslinux --config=ci //pkg/cmd/cockroach-short \
      //pkg/cmd/roachtest \
      //pkg/cmd/workload

BAZEL_BIN=$(bazel info bazel-bin --config=crosslinux --config=ci)
$BAZEL_BIN/pkg/cmd/roachtest/roachtest_/roachtest run acceptance kv/splits cdc/bank \
  --local \
  --parallelism=1 \
  --cockroach "$BAZEL_BIN/pkg/cmd/cockroach-short/cockroach-short_/cockroach-short" \
  --workload "$BAZEL_BIN/pkg/cmd/workload/workload_/workload" \
  --artifacts /artifacts \
  --teamcity

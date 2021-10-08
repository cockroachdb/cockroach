#!/usr/bin/env bash

set -euo pipefail

bazel build --config=crosslinux //pkg/cmd/cockroach-short \
      //pkg/cmd/roachprod \
      //pkg/cmd/roachtest \
      //pkg/cmd/workload

BAZEL_BIN=$(bazel info bazel-bin --config=crosslinux)
$BAZEL_BIN/pkg/cmd/roachtest/roachtest_/roachtest run acceptance kv/splits cdc/bank \
  --local \
  --parallelism=1 \
  --cockroach "$BAZEL_BIN/pkg/cmd/cockroach-short/cockroach-short_/cockroach-short" \
  --roachprod "$BAZEL_BIN/pkg/cmd/roachprod/roachprod_/roachprod" \
  --workload "$BAZEL_BIN/pkg/cmd/workload/workload_/workload" \
  --artifacts /artifacts \
  --teamcity

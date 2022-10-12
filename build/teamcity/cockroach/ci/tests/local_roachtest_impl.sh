#!/usr/bin/env bash

set -euo pipefail

if [[ "$(uname -m)" =~ (arm64|aarch64)$ ]]; then
  export CROSSLINUX_CONFIG="crosslinuxarm"
else
  export CROSSLINUX_CONFIG="crosslinux"
fi

bazel build --config=$CROSSLINUX_CONFIG --config=ci //pkg/cmd/cockroach-short \
      //pkg/cmd/roachtest \
      //pkg/cmd/roachprod \
      //pkg/cmd/workload

BAZEL_BIN=$(bazel info bazel-bin --config=$CROSSLINUX_CONFIG --config=ci)
ROACHPROD=$BAZEL_BIN/pkg/cmd/roachprod/roachprod_/roachprod

# if there is already a local cluster on this host, stop it before we
# attempt to run acceptance tests. While this is generally not the
# case, if a previous `roachtest` run was abruptly killed, the local
# cluster would remain active and cause every test below to fail.
if $ROACHPROD cached-hosts | grep -q '^local'; then
  $ROACHPROD destroy --all-local
fi

$BAZEL_BIN/pkg/cmd/roachtest/roachtest_/roachtest run acceptance kv/splits cdc/bank \
  --local \
  --parallelism=1 \
  --cockroach "$BAZEL_BIN/pkg/cmd/cockroach-short/cockroach-short_/cockroach-short" \
  --workload "$BAZEL_BIN/pkg/cmd/workload/workload_/workload" \
  --artifacts /artifacts \
  --teamcity

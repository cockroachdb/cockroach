#!/usr/bin/env bash

# Copyright 2021 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -euox pipefail

if [[ "$(uname -m)" =~ (arm64|aarch64)$ ]]; then
  export CROSSLINUX_CONFIG="crosslinuxarm"
else
  export CROSSLINUX_CONFIG="crosslinux"
fi

BAZEL_BIN=$(bazel info bazel-bin --config=$CROSSLINUX_CONFIG)

bazel build --config=$CROSSLINUX_CONFIG //pkg/cmd/cockroach-short \
      //pkg/cmd/roachtest \
      //pkg/cmd/roachprod \
      //pkg/cmd/workload

bazel build --config=$CROSSLINUX_CONFIG --config=force_build_cdeps //c-deps:libgeos

mkdir -p lib
cp $BAZEL_BIN/c-deps/libgeos_foreign/lib/libgeos.so lib/libgeos.so
cp $BAZEL_BIN/c-deps/libgeos_foreign/lib/libgeos_c.so lib/libgeos_c.so
chmod a+w lib/libgeos.so lib/libgeos_c.so

# if there are any local clusters on this host, stop them before we
# attempt to run acceptance tests. While this is generally not the
# case, if a previous `roachtest` run was abruptly killed, the local
# cluster would remain active and cause every test below to fail.
$BAZEL_BIN/pkg/cmd/roachprod/roachprod_/roachprod destroy --all-local

$BAZEL_BIN/pkg/cmd/roachtest/roachtest_/roachtest run acceptance kv/splits cdc/bank \
  --local \
  --parallelism=1 \
  --cockroach "$BAZEL_BIN/pkg/cmd/cockroach-short/cockroach-short_/cockroach-short" \
  --workload "$BAZEL_BIN/pkg/cmd/workload/workload_/workload" \
  --artifacts /artifacts \
  --artifacts-literal="${LITERAL_ARTIFACTS_DIR:-}" \
  --teamcity

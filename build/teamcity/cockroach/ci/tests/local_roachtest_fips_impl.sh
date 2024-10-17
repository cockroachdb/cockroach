#!/usr/bin/env bash

# Copyright 2023 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -euox pipefail

fips_enabled=$(cat /proc/sys/crypto/fips_enabled)

if [[ $fips_enabled != "1" ]]; then
  echo "FIPS mode is not enabled. Exiting."
  exit 1
fi

export CROSSLINUX_CONFIG="crosslinuxfips"

bazel build --config=$CROSSLINUX_CONFIG --config=ci //pkg/cmd/cockroach-short \
      //pkg/cmd/roachtest \
      //pkg/cmd/roachprod \
      //pkg/cmd/workload

BAZEL_BIN=$(bazel info bazel-bin --config=$CROSSLINUX_CONFIG --config=ci)

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

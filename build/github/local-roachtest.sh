#!/usr/bin/env bash

# Copyright 2024 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -euxo pipefail

CROSSCONFIG=$1

if [ $CROSSCONFIG == "crosslinuxfips" ]
then
    fips_enabled=$(cat /proc/sys/crypto/fips_enabled)

    if [[ $fips_enabled != "1" ]]; then
        echo "FIPS mode is not enabled. Exiting."
        exit 1
    fi
fi

BAZEL_BIN=$(bazel info bazel-bin --config=$CROSSCONFIG)
EXECROOT=$(bazel info execution_root --config=$CROSSCONFIG)

set +x
export COCKROACH_DEV_LICENSE=$(gcloud secrets versions access 1 --secret=cockroach-dev-license)
set -x

bazel build --config=$CROSSCONFIG $(./build/github/engflow-args.sh) \
      --bes_keywords integration-test-artifact-build \
      --jobs 100 \
      //pkg/cmd/cockroach-short \
      //pkg/cmd/roachtest \
      //pkg/cmd/roachprod \
      //pkg/cmd/workload \
      //c-deps:libgeos

mkdir -p lib artifacts
cp $EXECROOT/external/archived_cdep_libgeos_linux/lib/libgeos.so lib/libgeos.so
cp $EXECROOT/external/archived_cdep_libgeos_linux/lib/libgeos_c.so lib/libgeos_c.so
chmod a+w lib/libgeos.so lib/libgeos_c.so

$BAZEL_BIN/pkg/cmd/roachtest/roachtest_/roachtest run \
  --local \
  --parallelism=1 \
  --cockroach "$BAZEL_BIN/pkg/cmd/cockroach-short/cockroach-short_/cockroach-short" \
  --workload "$BAZEL_BIN/pkg/cmd/workload/workload_/workload" \
  --artifacts $PWD/artifacts \
  --github \
  --suite acceptance


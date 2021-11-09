#!/usr/bin/env bash

set -exuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"

source "$dir/teamcity-support.sh"

if [[ ! -f ~/.ssh/id_rsa.pub ]]; then
  ssh-keygen -q -C "roachtest-nightly-bazel $(date)" -N "" -f ~/.ssh/id_rsa
fi

bazel build --config crosslinux --config ci --config with_ui -c opt \
      //pkg/cmd/cockroach //pkg/cmd/workload //pkg/cmd/roachtest \
      //pkg/cmd/roachprod //c-deps:libgeos
BAZEL_BIN=$(bazel info bazel-bin --config crosslinux --config ci --config with_ui -c opt)
# Move this stuff to bin for simplicity.
mkdir -p bin
chmod o+rwx bin
cp $BAZEL_BIN/pkg/cmd/cockroach/cockroach_/cockroach bin
cp $BAZEL_BIN/pkg/cmd/roachprod/roachprod_/roachprod bin
cp $BAZEL_BIN/pkg/cmd/roachtest/roachtest_/roachtest bin
cp $BAZEL_BIN/pkg/cmd/workload/workload_/workload    bin
chmod a+w bin/cockroach bin/roachprod bin/roachtest bin/workload
# Stage the geos libs in the appropriate spot.
mkdir -p lib.docker_amd64
chmod o+rwx lib.docker_amd64
cp $BAZEL_BIN/c-deps/libgeos/lib/libgeos.so   lib.docker_amd64
cp $BAZEL_BIN/c-deps/libgeos/lib/libgeos_c.so lib.docker_amd64
chmod a+w lib.docker_amd64/libgeos.so lib.docker_amd64/libgeos_c.so

artifacts=/artifacts
source $root/build/teamcity/util/roachtest_util.sh

build/teamcity-roachtest-invoke.sh \
  --cloud="${CLOUD}" \
  --count="${COUNT-1}" \
  --parallelism="${PARALLELISM}" \
  --cpu-quota="${CPUQUOTA}" \
  --cluster-id="${TC_BUILD_ID}" \
  --build-tag="${BUILD_TAG}" \
  --cockroach="${PWD}/bin/cockroach" \
  --artifacts=/artifacts \
  --artifacts-literal="${LITERAL_ARTIFACTS_DIR:-}" \
  --slack-token="${SLACK_TOKEN}" \
  "${TESTS}"

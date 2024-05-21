#!/usr/bin/env bash

set -exuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname $(dirname "${0}")))))"

source "$dir/teamcity-support.sh"
if [[ ! -f ~/.ssh/id_rsa.pub ]]; then
  ssh-keygen -q -C "private-roachtest-nightly-bazel $(date)" -N "" -f ~/.ssh/id_rsa
fi

$root/build/teamcity/cockroach/nightlies/roachtest_compile_bits.sh amd64

artifacts=/artifacts
source $root/build/teamcity/util/roachtest_util.sh

build/teamcity-roachtest-invoke.sh \
  --cloud="${CLOUD}" \
  --count="${COUNT-1}" \
  --parallelism="${PARALLELISM}" \
  --cpu-quota="${CPUQUOTA}" \
  --cluster-id="${TC_BUILD_ID}" \
  --artifacts=/artifacts \
  --artifacts-literal="${LITERAL_ARTIFACTS_DIR:-}" \
  "${TESTS}"

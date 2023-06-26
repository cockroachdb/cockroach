#!/usr/bin/env bash

set -exuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"

source "$dir/teamcity-support.sh"

if [[ ! -f ~/.ssh/id_rsa.pub ]]; then
  ssh-keygen -q -C "roachtest-nightly-bazel $(date)" -N "" -f ~/.ssh/id_rsa
fi

source $root/build/teamcity/cockroach/nightlies/roachtest_compile_bits.sh

artifacts=/artifacts
source $root/build/teamcity/util/roachtest_util.sh

build/teamcity-roachtest-invoke.sh \
  --metamorphic-encryption-probability=0.5 \
  --select-probability="${SELECT_PROBABILITY:-1.0}" \
  --cloud="${CLOUD}" \
  --count="${COUNT-1}" \
  --parallelism="${PARALLELISM}" \
  --cpu-quota="${CPUQUOTA}" \
  --cluster-id="${TC_BUILD_ID}" \
  --artifacts=/artifacts \
  --artifacts-literal="${LITERAL_ARTIFACTS_DIR:-}" \
  --slack-token="${SLACK_TOKEN}" \
  "${TESTS}" ${FILTER}

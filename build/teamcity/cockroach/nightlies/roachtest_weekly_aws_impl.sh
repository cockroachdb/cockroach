#!/usr/bin/env bash

set -exuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"

source "$dir/teamcity-support.sh"

if [[ ! -f ~/.ssh/id_rsa.pub ]]; then
  ssh-keygen -q -C "roachtest-weekly-bazel $(date)" -N "" -f ~/.ssh/id_rsa
fi

arch=amd64
if [[ ${FIPS_ENABLED:-0} == 1 ]]; then
  arch=amd64-fips
fi
$root/build/teamcity/cockroach/nightlies/roachtest_compile_bits.sh $arch

artifacts=/artifacts
source $root/build/teamcity/util/roachtest_util.sh

build/teamcity-roachtest-invoke.sh \
  --suite weekly \
  --cloud="${CLOUD}" \
  --cluster-id "${TC_BUILD_ID}" \
  --artifacts=/artifacts \
  --artifacts-literal="${LITERAL_ARTIFACTS_DIR:-}" \
  --slack-token="${SLACK_TOKEN}"

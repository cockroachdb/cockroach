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
$root/build/teamcity/cockroach/nightlies/roachtest_compile_bits.sh arm64

artifacts=/artifacts
source $root/build/teamcity/util/roachtest_util.sh

# NB: Teamcity has a 7920 minute timeout that, when reached,
# kills the process without a stack trace (probably SIGKILL).
# We'd love to see a stack trace though, so after 7800 minutes,
# kill with SIGINT which will allow roachtest to fail tests and
# cleanup.
#
timeout -s INT $((7800*60)) build/teamcity-roachtest-invoke.sh \
  --suite weekly \
  --cloud="${CLOUD}" \
  --cluster-id "${TC_BUILD_ID}" \
  --artifacts=/artifacts \
  --artifacts-literal="${LITERAL_ARTIFACTS_DIR:-}" \
  --parallelism="${PARALLELISM}" \
  --cpu-quota="${CPUQUOTA}" \
  --metamorphic-encryption-probability=0.5 \
  --metamorphic-arm64-probability="${ARM_PROBABILITY:-0.5}" \
  --use-spot="${USE_SPOT:-true}" \
  --use-spot-always=${USE_SPOT_ALWAYS:-false}" \
  --slack-token="${SLACK_TOKEN}"

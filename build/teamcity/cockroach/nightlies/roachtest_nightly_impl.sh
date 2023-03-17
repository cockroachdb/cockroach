#!/usr/bin/env bash

set -exuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"

source "$dir/teamcity-support.sh"

if [[ ! -f ~/.ssh/id_rsa.pub ]]; then
  ssh-keygen -q -C "roachtest-nightly-bazel $(date)" -N "" -f ~/.ssh/id_rsa
fi

if [[ ${FIPS_ENABLED:-0} == 1 ]]; then
  platform=crosslinuxfips
  fips_flag="--fips"
else
  platform=crosslinux
  fips_flag=""
fi

source $root/build/teamcity/cockroach/nightlies/roachtest_compile_bits.sh $platform

artifacts=/artifacts
source $root/build/teamcity/util/roachtest_util.sh

build/teamcity-roachtest-invoke.sh \
  --metamorphic-encryption-probability=0.5 \
  --cloud="${CLOUD}" \
  --count="${COUNT-1}" \
  --parallelism="${PARALLELISM}" \
  --cpu-quota="${CPUQUOTA}" \
  --cluster-id="${TC_BUILD_ID}" \
  --cockroach="${PWD}/bin/cockroach" \
  --cockroach-short="${PWD}/bin/cockroach-short-ea" \
  --artifacts=/artifacts \
  --artifacts-literal="${LITERAL_ARTIFACTS_DIR:-}" \
  --slack-token="${SLACK_TOKEN}" \
  $fips_flag \
  "${TESTS}"

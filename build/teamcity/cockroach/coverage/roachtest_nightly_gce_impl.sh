#!/usr/bin/env bash

set -exuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"

source "$dir/teamcity-support.sh"

if [[ ! -f ~/.ssh/id_rsa.pub ]]; then
  ssh-keygen -q -C "roachtest-nightly-bazel $(date)" -N "" -f ~/.ssh/id_rsa
fi

$root/build/teamcity/cockroach/nightlies/roachtest_compile_bits.sh --with-code-coverage amd64

echo "$GOOGLE_EPHEMERAL_CREDENTIALS" > creds.json
gcloud auth activate-service-account --key-file=creds.json
export ROACHPROD_USER=teamcity

# See build/teamcity/util/roachtest_util.sh.
PARALLELISM=16
CPUQUOTA=1024
FILTER="tag:aws tag:default"

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
  --slack-token="${SLACK_TOKEN:-}" \
  --go-cover \
  ${TESTS:-} ${FILTER}

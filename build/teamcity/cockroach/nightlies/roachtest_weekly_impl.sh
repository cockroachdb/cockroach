#!/usr/bin/env bash

set -exuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"

source "$dir/teamcity-support.sh"

if [[ ! -f ~/.ssh/id_rsa.pub ]]; then
  ssh-keygen -q -C "roachtest-weekly-bazel $(date)" -N "" -f ~/.ssh/id_rsa
fi

source $root/build/teamcity/cockroach/nightlies/roachtest_compile_bits.sh

artifacts=/artifacts
source $root/build/teamcity/util/roachtest_util.sh

# NB: Teamcity has a 7920 minute timeout that, when reached,
# kills the process without a stack trace (probably SIGKILL).
# We'd love to see a stack trace though, so after 7800 minutes,
# kill with SIGINT which will allow roachtest to fail tests and
# cleanup.
#
# NB(2): We specify --zones below so that nodes are created in us-central1-b
# by default. This reserves us-east1-b (the roachprod default zone) for use
# by manually created clusters.
#
# NB(3): If you make changes here, you should probably make the same change in
# build/teamcity-weekly-roachtest.sh
timeout -s INT $((7800*60)) build/teamcity-roachtest-invoke.sh \
  tag:weekly \
  --cluster-id "${TC_BUILD_ID}" \
  --zones "us-central1-b,us-west1-b,europe-west2-b" \
  --cockroach "$PWD/bin/cockroach" \
  --artifacts=/artifacts \
  --artifacts-literal="${LITERAL_ARTIFACTS_DIR:-}" \
  --parallelism 5 \
  --metamorphic-encryption-probability=0.5 \
  --slack-token="${SLACK_TOKEN}"

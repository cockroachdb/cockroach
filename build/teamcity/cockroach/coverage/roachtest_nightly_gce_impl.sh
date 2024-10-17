#!/usr/bin/env bash

# Copyright 2023 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -exuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"

source "$dir/teamcity-support.sh"

if [[ ! -f ~/.ssh/id_rsa.pub ]]; then
  ssh-keygen -q -C "roachtest-nightly-bazel $(date)" -N "" -f ~/.ssh/id_rsa
fi

# We will only spin up AMD64 clusters. Note that the TeamCity runner (this host)
# doesn't need to be AMD64; the roachtest binary is always built for the host
# architecture.
$root/build/teamcity/cockroach/nightlies/roachtest_compile_bits.sh --with-code-coverage amd64

echo "$GOOGLE_EPHEMERAL_CREDENTIALS" > creds.json
gcloud auth activate-service-account --key-file=creds.json
export ROACHPROD_USER=teamcity

# Values taken from build/teamcity/util/roachtest_util.sh.
PARALLELISM=16
CPUQUOTA=1024

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
  --suite nightly \
  --selective-tests="${SELECTIVE_TESTS:-false}" \
  ${TESTS:-}

#!/usr/bin/env bash

# Copyright 2024 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

set -exuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"
set -a
source "$dir/teamcity-support.sh"
set +a

if [[ ! -f ~/.ssh/id_rsa.pub ]]; then
  ssh-keygen -q -C "roachtest-nightly-bazel $(date)" -N "" -f ~/.ssh/id_rsa
fi

source $root/build/teamcity/util/roachtest_util.sh

artifacts=/artifacts

arch=amd64
$root/build/teamcity/cockroach/nightlies/roachtest_compile_bits.sh $arch

build/teamcity-roachtest-invoke.sh \
  --suite perturbation \
  --cloud "gce" \
  --cluster-id "${TC_BUILD_ID}" \
  --artifacts=/artifacts \
  --artifacts-literal="${LITERAL_ARTIFACTS_DIR:-}" \
  --parallelism="${PARALLELISM}" \
  --cpu-quota="${CPUQUOTA}" \
  --use-spot="${USE_SPOT:-auto}" \
  --slack-token="${SLACK_TOKEN}" \
  --side-eye-token="${SIDE_EYE_API_TOKEN}"

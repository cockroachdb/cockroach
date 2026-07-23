#!/usr/bin/env bash

# Copyright 2023 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -exuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname $(dirname "${0}")))))"

# N.B. export variables like `root` s.t. they can be used by scripts called below.
set -a
source "$dir/teamcity-support.sh"
set +a

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

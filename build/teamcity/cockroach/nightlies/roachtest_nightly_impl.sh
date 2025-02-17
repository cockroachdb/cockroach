#!/usr/bin/env bash

# Copyright 2021 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -exuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"

# N.B. export variables like `root` s.t. they can be used by scripts called below.
set -a
source "$dir/teamcity-support.sh"
set +a

if [[ ! -f ~/.ssh/id_rsa.pub ]]; then
  ssh-keygen -q -C "roachtest-nightly-bazel $(date)" -N "" -f ~/.ssh/id_rsa
fi

arch=amd64
if [[ ${FIPS_ENABLED:-0} == 1 ]]; then
  arch=amd64-fips
fi
$root/build/teamcity/cockroach/nightlies/roachtest_compile_bits.sh $arch
$root/build/teamcity/cockroach/nightlies/roachtest_compile_bits.sh arm64

artifacts=/artifacts
source $root/build/teamcity/util/roachtest_util.sh

# Standard release branches are in the format `release-24.1` for the
# 24.1 release, for example.
release_branch_regex="^release-[0-9][0-9]\.[0-9]"
# Test selection is enabled only on release branches.
selective_tests="false"

if [[ "${TC_BUILD_BRANCH}" == "master" ]]; then
  # We default to using test selection on master, unless explicitly
  # overriden in the TeamCity UI.
  selective_tests="${SELECTIVE_TESTS:-true}"
elif [[ "${TC_BUILD_BRANCH}" =~ ${release_branch_regex}$ ]]; then
  # Same for release branches.
  selective_tests="${SELECTIVE_TESTS:-true}"
elif [[ "${TC_BUILD_BRANCH}" =~ ${release_branch_regex}\.[0-9]{1,2}-rc$ ]]; then
  # If we are running an `-rc` branch for a specific patch release
  # (for instance, `release-24.1.1-rc`), then only run 40% of the test
  # suite by default. This is to avoid a high volume of concurrent
  # cluster creation attempts across all existing release branches.
  #
  # NOTE: in the future, instead of choosing the tests randomly as we
  # do here, we plan to utilize a smarter test selection strategy (see
  # #119630).
  select_probability="--select-probability=0.4"
elif [[ "${TC_BUILD_BRANCH}" =~ ^release- && "${ROACHTEST_FORCE_RUN_INVALID_RELEASE_BRANCH}" != "true" ]]; then
  # The only valid release branches are the ones handled above. That
  # said, from time to time we might have cases where a branch with
  # the `release-` prefix is created by accident, activating the
  # TeamCity trigger for the Roachtest Nightly build. We abort
  # execution in these cases to avoid unnecessarily running a test
  # suite on these branches.
  echo "Refusing to run roachtest nightly suite on invalid release branch: ${TC_BUILD_BRANCH}."
  exit 1
else
  # Use a 0.1 default in all other branches, to reduce the chances of
  # an accidental full-suite run on feature branches.
  select_probability="--select-probability=0.1"
fi

# Special handling for the select-probability is needed because it is
# incompatible with the selective-tests flag. If it isn't overriden in the
# TeamCity UI or set by the logic above, we need to omit the flag entirely.
if [[ "${SELECT_PROBABILITY:-}"  != "" ]]; then
  select_probability=--select-probability="${SELECT_PROBABILITY}"
fi

# Fail early if both selective-tests=true and select-probability are set.
if [[ "${selective_tests}" == "true" && "${select_probability:-}" != "" ]]; then
  echo "SELECTIVE_TESTS=true and SELECT_PROBABILITY are incompatible. Disable one of them."
  exit 1
fi

build/teamcity-roachtest-invoke.sh \
  --metamorphic-encryption-probability=0.5 \
  --metamorphic-arm64-probability="${ARM_PROBABILITY:-0.5}" \
  --metamorphic-cockroach-ea-probability="${COCKROACH_EA_PROBABILITY:-0.2}" \
  ${select_probability:-} \
  --use-spot="${USE_SPOT:-auto}" \
  --cloud="${CLOUD}" \
  --count="${COUNT-1}" \
  --clear-cluster-cache="${CLEAR_CLUSTER_CACHE:-true}" \
  --auto-kill-threshold="${AUTO_KILL_THRESHOLD:-0.10}" \
  --parallelism="${PARALLELISM}" \
  --cpu-quota="${CPUQUOTA}" \
  --cluster-id="${TC_BUILD_ID}" \
  --artifacts=/artifacts \
  --artifacts-literal="${LITERAL_ARTIFACTS_DIR:-}" \
  --slack-token="${SLACK_TOKEN}" \
  --suite nightly \
  --selective-tests="${selective_tests:-false}" \
  --side-eye-token="${SIDE_EYE_API_TOKEN}" \
  --export-openmetrics="${EXPORT_OPENMETRICS:-false}" \
  --openmetrics-labels="branch=$(tc_build_branch), cpu-arch=${arch}, suite=nightly" \
  ${EXTRA_ROACHTEST_ARGS:+$EXTRA_ROACHTEST_ARGS} \
  "${TESTS}"

#!/usr/bin/env bash

set -exuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"

source "$dir/teamcity-support.sh"

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

if [[ "${TC_BUILD_BRANCH}" == "master" ]]; then
  # We default to running all tests on master, unless explicitly
  # overriden in the TeamCity UI.
  select_probability="${SELECT_PROBABILITY:-1.0}"
elif [[ "${TC_BUILD_BRANCH}" =~ ${release_branch_regex}$ ]]; then
  # Same for release branches.
  select_probability="${SELECT_PROBABILITY:-1.0}"
elif [[ "${TC_BUILD_BRANCH}" =~ ${release_branch_regex}\.[0-9]{1,2}-rc$ ]]; then
  # If we are running an `-rc` branch for a specific patch release
  # (for instance, `release-24.1.1-rc`), then only run 40% of the test
  # suite by default. This is to avoid a high volume of concurrent
  # cluster creation attempts across all existing release branches.
  #
  # NOTE: in the future, instead of choosing the tests randomly as we
  # do here, we plan to utilize a smarter test selection strategy (see
  # #119630).
  select_probability="${SELECT_PROBABILITY:-0.4}"
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
  select_probability="${SELECT_PROBABILITY:-0.1}"
fi

build/teamcity-roachtest-invoke.sh \
  --metamorphic-encryption-probability=0.5 \
  --metamorphic-arm64-probability="${ARM_PROBABILITY:-0.5}" \
  --select-probability="${select_probability}" \
  --cloud="${CLOUD}" \
  --count="${COUNT-1}" \
  --clear-cluster-cache="${CLEAR_CLUSTER_CACHE:-true}" \
  --auto-kill-threshold="${AUTO_KILL_THRESHOLD:-0.05}" \
  --parallelism="${PARALLELISM}" \
  --cpu-quota="${CPUQUOTA}" \
  --cluster-id="${TC_BUILD_ID}" \
  --artifacts=/artifacts \
  --artifacts-literal="${LITERAL_ARTIFACTS_DIR:-}" \
  --slack-token="${SLACK_TOKEN}" \
  --suite nightly \
  "${TESTS}"

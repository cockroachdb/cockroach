#!/usr/bin/env bash

set -euo pipefail

dir="$(dirname $(dirname $(dirname $(dirname $(dirname "${0}")))))"

source "$dir/teamcity-support.sh"  # For $root
source "$dir/teamcity-bazel-support.sh"  # For run_bazel

tc_start_block "Prepare environment"
# Grab a testing license good for one hour.
COCKROACH_DEV_LICENSE=$(curl --retry 3 --retry-connrefused -f "https://register.cockroachdb.com/api/prodtest")
tc_end_block "Prepare environment"

tc_start_block "Run local roachtests"
run_bazel env \
    COCKROACH_DEV_LICENSE="${COCKROACH_DEV_LICENSE}" \
    GITHUB_API_TOKEN="${GITHUB_API_TOKEN-}" \
    BUILD_VCS_NUMBER="${BUILD_VCS_NUMBER-}" \
    TC_BUILD_ID="${TC_BUILD_ID-}" \
    TC_SERVER_URL="${TC_SERVER_URL-}" \
    TC_BUILD_BRANCH="${TC_BUILD_BRANCH-}" \
    build/teamcity/cockroach/ci/tests/local_roachtest_impl.sh
tc_end_block "Run local roachtests"

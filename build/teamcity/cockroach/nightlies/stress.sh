#!/usr/bin/env bash

set -euo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"

source "$dir/teamcity-support.sh"  # For $root
source "$dir/teamcity-bazel-support.sh"  # For run_bazel

tc_start_block "Run stress"
BAZEL_SUPPORT_EXTRA_DOCKER_ARGS="-e BUILD_TAG -e BUILD_VCS_NUMBER -e GITHUB_API_TOKEN -e GITHUB_ORG -e GITHUB_REPO -e TC_BUILD_BRANCH -e TC_BUILD_ID -e TC_SERVER_URL -e PKG -e TAGS -e STRESSFLAGS -e TESTTIMEOUTSECS -e EXTRA_BAZEL_FLAGS" \
  run_bazel build/teamcity/cockroach/nightlies/stress_impl.sh
tc_end_block "Run stress"

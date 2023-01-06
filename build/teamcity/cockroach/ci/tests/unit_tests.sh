#!/usr/bin/env bash

set -euo pipefail

dir="$(dirname $(dirname $(dirname $(dirname $(dirname "${0}")))))"

source "$dir/teamcity-support.sh"  # For $root
source "$dir/teamcity-bazel-support.sh"  # For run_bazel

tc_start_block "Run unit tests"
BAZEL_SUPPORT_EXTRA_DOCKER_ARGS="-e TC_BUILD_BRANCH -e GITHUB_API_TOKEN -e BUILD_VCS_NUMBER -e TC_BUILD_ID -e TC_SERVER_URL -e TC_BUILDTYPE_ID -e GITHUB_REPO" run_bazel build/teamcity/cockroach/ci/tests/unit_tests_impl.sh
tc_end_block "Run unit tests"

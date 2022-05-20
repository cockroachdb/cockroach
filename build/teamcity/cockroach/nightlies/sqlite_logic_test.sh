#!/usr/bin/env bash

set -euo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"

source "$dir/teamcity-support.sh"  # For $root
source "$dir/teamcity-bazel-support.sh"  # For run_bazel

tc_start_block "Run SQLite logic tests"
BAZEL_SUPPORT_EXTRA_DOCKER_ARGS="-e BUILD_VCS_NUMBER -e GITHUB_API_TOKEN -e GITHUB_REPO -e TC_BUILDTYPE_ID -e TC_BUILD_BRANCH -e TC_BUILD_ID -e TC_SERVER_URL" \
                               run_bazel build/teamcity/cockroach/nightlies/sqlite_logic_test_impl.sh
tc_end_block "Run SQLite logic tests"

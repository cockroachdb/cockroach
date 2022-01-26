#!/usr/bin/env bash

set -exuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"

source "$dir/teamcity-support.sh"  # For $root
source "$dir/teamcity-bazel-support.sh"  # For run_bazel

BAZEL_SUPPORT_EXTRA_DOCKER_ARGS="-e CLOUD -e COCKROACH_DEV_LICENSE -e COUNT -e GITHUB_API_TOKEN -e GOOGLE_EPHEMERAL_CREDENTIALS -e SLACK_TOKEN -e TC_BUILD_BRANCH -e TC_BUILD_ID" \
			       run_bazel build/teamcity/cockroach/nightlies/roachtest_weekly_impl.sh

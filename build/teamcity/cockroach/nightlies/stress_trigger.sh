#!/usr/bin/env bash

set -euo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"

source "$dir/teamcity-support.sh"  # For $root
source "$dir/teamcity-bazel-support.sh"  # For run_bazel

tc_start_block "Run stress trigger"
BAZEL_SUPPORT_EXTRA_DOCKER_ARGS="-e TC_API_USER -e TC_API_PASSWORD -e TC_SERVER_URL -e TC_BUILDTYPE_ID -e TC_BUILD_BRANCH" \
  run_bazel build/teamcity/cockroach/nightlies/stress_trigger_impl.sh "$@"
tc_end_block "Run stress trigger"

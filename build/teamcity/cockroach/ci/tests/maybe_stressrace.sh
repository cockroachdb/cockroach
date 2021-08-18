#!/usr/bin/env bash

set -euo pipefail

dir="$(dirname $(dirname $(dirname $(dirname $(dirname "${0}")))))"

source "$dir/teamcity-support.sh"  # For $root, would_stress
source "$dir/teamcity-bazel-support.sh"  # For run_bazel

tc_prepare

if would_stress; then
    tc_start_block "Run stress tests"
    run_bazel env BUILD_VCS_NUMBER="$BUILD_VCS_NUMBER" build/teamcity/cockroach/ci/tests/maybe_stress_impl.sh stressrace
    tc_end_block "Run stress tests"
fi

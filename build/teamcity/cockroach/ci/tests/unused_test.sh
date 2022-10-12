#!/usr/bin/env bash

set -euo pipefail

dir="$(dirname $(dirname $(dirname $(dirname $(dirname "${0}")))))"

source "$dir/teamcity-support.sh"  # For $root
source "$dir/teamcity-bazel-support.sh"  # For run_bazel

tc_start_block "Run unused test"
echo "##teamcity[testStarted name='UnusedLint' captureStandardOutput='true']"
exit_status=0
run_bazel build/teamcity/cockroach/ci/tests/unused_test_impl.sh || exit_status=$?
if [ "$exit_status" -ne 0 ]; then
    echo "##teamcity[testFailed name='UnusedLint']"
fi
echo "##teamcity[testFinished name='UnusedLint']"
tc_end_block "Run unused test"

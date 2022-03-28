#!/usr/bin/env bash

set -euo pipefail

dir="$(dirname $(dirname $(dirname $(dirname $(dirname "${0}")))))"

source "$dir/teamcity-support.sh"  # For $root
source "$dir/teamcity-bazel-support.sh"  # For run_bazel

tc_start_block "Run UI tests"
run_bazel build/teamcity/cockroach/ci/tests/ui_lint_impl.sh
tc_end_block "Run UI tests"

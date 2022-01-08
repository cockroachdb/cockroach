#!/usr/bin/env bash

set -xeuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname $(dirname "${0}")))))"

source "$dir/teamcity-support.sh"  # For $root
source "$dir/teamcity-bazel-support.sh"  # For run_bazel

tc_start_block "Ensure generated code is up to date"
run_bazel build/teamcity/cockroach/ci/tests/check_generated_code_impl.sh
tc_end_block "Ensure generated code is up to date"

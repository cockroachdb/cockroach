#!/usr/bin/env bash

set -euo pipefail

dir="$(dirname $(dirname $(dirname $(dirname $(dirname "${0}")))))"

source "$dir/teamcity-support.sh"  # For $root
source "$dir/teamcity-bazel-support.sh"  # For run_bazel

tc_start_block "GcAssert"
run_bazel build/teamcity/cockroach/ci/tests/gcassert_impl.sh
tc_end_block "GcAssert"

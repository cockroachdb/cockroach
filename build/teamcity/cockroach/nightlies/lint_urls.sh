#!/usr/bin/env bash

set -euo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"

source "$dir/teamcity-support.sh"  # For $root
source "$dir/teamcity-bazel-support.sh"  # For run_bazel

tc_start_block "lint urls"
run_bazel build/teamcity/cockroach/nightlies/lint_urls_impl.sh
tc_end_block "lint urls"

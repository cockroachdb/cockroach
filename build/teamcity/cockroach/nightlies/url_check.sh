#!/usr/bin/env bash

set -exuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"

source "$dir/teamcity-support.sh"  # For $root
source "$dir/teamcity-bazel-support.sh"  # For run_bazel

tc_start_block "Run url check"
run_bazel build/teamcity/cockroach/nightlies/url_check_impl.sh
tc_end_block "Run url check"

#!/usr/bin/env bash

set -xeuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname $(dirname "${0}")))))"
source "$dir/teamcity-support.sh"

tc_prepare

export ARTIFACTSDIR=$PWD/artifacts/acceptance
mkdir -p "$ARTIFACTSDIR"

remove_files_on_exit() {
  # Some unit tests test automatic ballast creation. These ballasts can be
  # larger than the maximum artifact size. Remove any artifacts with the
  # EMERGENCY_BALLAST filename.
  find "$ARTIFACTSDIR" -name "EMERGENCY_BALLAST" -delete
}
trap remove_files_on_exit EXIT

tc_start_block "Run acceptance tests"
bazel run \
  //pkg/acceptance:acceptance_test \
  --config=crosslinux --config=test \
  --test_arg=-l="$ARTIFACTSDIR" \
  --test_env=TZ=America/New_York \
  --test_timeout=1800
tc_end_block "Run acceptance tests"

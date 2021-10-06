#!/usr/bin/env bash

set -xeuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname $(dirname "${0}")))))"
source "$dir/teamcity-support.sh"

tc_prepare

export ARTIFACTSDIR=$PWD/artifacts/acceptance
mkdir -p "$ARTIFACTSDIR"

tc_start_block "Run acceptance tests"
bazel run \
  //pkg/acceptance:acceptance_test \
  --config=crosslinux --config=test \
  --test_arg=-l="$ARTIFACTSDIR" \
  --test_env=TZ=America/New_York \
  --test_timeout=1800
tc_end_block "Run acceptance tests"

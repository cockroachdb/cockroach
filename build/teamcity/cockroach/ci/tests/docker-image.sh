#!/usr/bin/env bash

set -euo pipefail

dir="$(dirname $(dirname $(dirname $(dirname $(dirname "${0}")))))"
source "$dir/teamcity-support.sh"

tc_prepare

export ARTIFACTSDIR=$PWD/artifacts/docker
mkdir -p "$ARTIFACTSDIR"

tc_start_block "Run docker image tests"
bazel run \
  //pkg/docker:docker_test \
  --config=crosslinux --config=test \
  --test_arg=-l="$ARTIFACTSDIR" \
  --test_env=TZ=America/New_York \
  --test_timeout=1800
tc_end_block "Run docker image tests"

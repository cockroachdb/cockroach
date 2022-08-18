#!/usr/bin/env bash

set -euo pipefail

dir="$(dirname $(dirname $(dirname $(dirname $(dirname "${0}")))))"
source "$dir/teamcity-support.sh"

tc_prepare

tc_start_block "Run docker image tests"

# Skip for now: #82747
bazel run \
  //pkg/testutils/docker:docker_test \
  --config=crosslinux --config=test \
  --test_timeout=3000

tc_end_block "Run docker image tests"

#!/usr/bin/env bash

# Copyright 2022 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -exuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname $(dirname "${0}")))))"

source "$dir/teamcity-support.sh"  # For $root
source "$dir/teamcity-bazel-support.sh"  # For run_bazel

tc_start_block "Run local roachtests"
BAZEL_SUPPORT_EXTRA_DOCKER_ARGS="-e LITERAL_ARTIFACTS_DIR=$root/artifacts -e COCKROACH_DEV_LICENSE -e BUILD_VCS_NUMBER -e TC_BUILD_ID -e TC_BUILD_BRANCH" \
  run_bazel build/teamcity/cockroach/ci/tests/local_roachtest_impl.sh
tc_end_block "Run local roachtests"

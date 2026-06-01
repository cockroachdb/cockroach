#!/usr/bin/env bash

# Copyright 2026 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

# Docker launcher for the standalone roachtest compile-and-cache step.
# Analogous to roachtest_nightly_gce.sh but only compiles — no test execution.

set -exuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"

source "$dir/teamcity-support.sh"  # For $root
source "$dir/teamcity-bazel-support.sh"  # For run_bazel

BAZEL_SUPPORT_EXTRA_DOCKER_ARGS="-e BUILD_VCS_NUMBER -e GOOGLE_EPHEMERAL_CREDENTIALS -e TC_BUILD_BRANCH" \
  run_bazel build/teamcity/cockroach/nightlies/roachtest_compile_and_cache.sh

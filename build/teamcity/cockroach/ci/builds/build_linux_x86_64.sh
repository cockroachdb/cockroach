#!/usr/bin/env bash

set -euo pipefail

dir="$(dirname $(dirname $(dirname $(dirname $(dirname "${0}")))))"

source "$dir/teamcity-support.sh"  # For $root
source "$dir/teamcity-bazel-support.sh"  # For run_bazel

tc_start_block "Run Bazel build"
BAZEL_SUPPORT_EXTRA_DOCKER_ARGS="-e BUILDBUDDY_API_KEY" \
                               run_bazel build/teamcity/cockroach/ci/builds/build_impl.sh crosslinux
tc_end_block "Run Bazel build"

set +e

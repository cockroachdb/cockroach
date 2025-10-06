#!/usr/bin/env bash

# Copyright 2022 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -euo pipefail

dir="$(dirname $(dirname $(dirname $(dirname $(dirname $(dirname "${0}"))))))"

source "$dir/teamcity-support.sh"  # For $root
source "$dir/teamcity-bazel-support.sh"  # For run_bazel

tc_start_block "Run Update Versions Release Phase"
BAZEL_SUPPORT_EXTRA_DOCKER_ARGS="-e DRY_RUN -e RELEASED_VERSION -e NEXT_VERSION -e SMTP_PASSWORD -e GH_TOKEN" \
  run_bazel build/teamcity/internal/cockroach/release/process/update_versions_impl.sh
tc_end_block "Run Update Versions Release Phase"

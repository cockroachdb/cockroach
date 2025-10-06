#!/usr/bin/env bash

# Copyright 2022 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -euo pipefail

dir="$(dirname $(dirname $(dirname $(dirname $(dirname $(dirname "${0}"))))))"

source "$dir/teamcity-support.sh"  # For $root
source "$dir/teamcity-bazel-support.sh"  # For run_bazel

tc_start_block "Run Post Blockers Release Phase"
BAZEL_SUPPORT_EXTRA_DOCKER_ARGS="-e DRY_RUN -e RELEASE_SERIES -e SMTP_PASSWORD -e SMTP_USER -e GITHUB_TOKEN -e PUBLISH_DATE -e PREP_DATE -e NEXT_VERSION -e DAYS_BEFORE_PREP_DATE" \
  run_bazel build/teamcity/internal/cockroach/release/process/post_blockers_impl.sh
tc_end_block "Run Post Blockers Release Phase"

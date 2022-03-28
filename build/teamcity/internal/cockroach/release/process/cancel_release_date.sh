#!/usr/bin/env bash

set -euo pipefail

dir="$(dirname $(dirname $(dirname $(dirname $(dirname $(dirname "${0}"))))))"

source "$dir/teamcity-support.sh"  # For $root
source "$dir/teamcity-bazel-support.sh"  # For run_bazel

tc_start_block "Run Post Blockers Cancel Release Date Phase"
BAZEL_SUPPORT_EXTRA_DOCKER_ARGS="-e DRY_RUN -e RELEASE_SERIES -e SMTP_PASSWORD -e SMTP_USER -e PUBLISH_DATE -e NEXT_PUBLISH_DATE" \
  run_bazel build/teamcity/internal/cockroach/release/process/cancel_release_date_impl.sh
tc_end_block "Run Post Blockers Cancel Release Date Phase"

#!/usr/bin/env bash

set -euo pipefail

dir="$(dirname $(dirname $(dirname $(dirname $(dirname $(dirname "${0}"))))))"

source "$dir/teamcity-support.sh"  # For $root
source "$dir/teamcity-bazel-support.sh"  # For run_bazel

tc_start_block "Run Pick SHA Release Phase"
BAZEL_SUPPORT_EXTRA_DOCKER_ARGS="-e DRY_RUN -e JIRA_TOKEN -e JIRA_USERNAME -e METADATA_PUBLISHER_GOOGLE_CREDENTIALS_DEV -e METADATA_PUBLISHER_GOOGLE_CREDENTIALS_PROD -e RELEASE_SERIES -e SMTP_PASSWORD -e SMTP_USER" \
  run_bazel build/teamcity/internal/cockroach/release/process/pick_sha_impl.sh
tc_end_block "Run Pick SHA Release Phase"

#!/usr/bin/env bash

# Copyright 2026 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

# GitHub Actions wrapper for posting the "binaries have been blessed"
# notification on Jira and #db-release-status after a successful release
# publish. Runs the release binary inside the bazel docker container (via
# run_bazel) so the workflow doesn't depend on the host runner having a
# compatible bazel / Go toolchain.
#
# Expects to be called by a step that has fetched the release secrets via
# google-github-actions/get-secretmanager-secrets and bound them to the
# env vars below. The :? defaults fail fast if the calling workflow
# drops one.
#
# NOTE: This script intentionally does NOT use set -x. It handles
# secrets that must never appear in build logs.

set -euo pipefail

: "${JIRA_API_TOKEN:?must be set by the workflow}"
: "${JIRA_EMAIL:?must be set by the workflow}"
: "${SLACK_BOT_TOKEN:?must be set by the workflow}"
: "${BUILD_VCS_NUMBER:?must be set by the workflow (the publish SHA)}"

dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "$dir/build/teamcity-support.sh"        # for $root
source "$dir/build/teamcity-bazel-support.sh"  # for run_bazel

# The impl script writes its publish-notification summary here (container path
# /artifacts maps to $root/artifacts on the host). Clear any stale copy from
# an earlier step so we only fold this run's result into the job summary.
summary_file="$root/artifacts/notify-publish-summary.md"
rm -f "$summary_file"

# Forward the secrets and the publish SHA into the container.
# IS_PRODUCTION_REPO is read by the binary's isProductionRepo() helper to
# pick the Slack channel; without it the binary defaults to #db-release-test.
#
# Capture the exit code rather than letting `set -e` abort here: we must fold
# the summary (below) before propagating the failure so a failed notification
# is surfaced in the job summary.
rc=0
BAZEL_SUPPORT_EXTRA_DOCKER_ARGS="-e JIRA_API_TOKEN -e JIRA_EMAIL -e SLACK_BOT_TOKEN -e BUILD_VCS_NUMBER -e IS_PRODUCTION_REPO" \
  run_bazel build/github/release-notify-publish-impl.sh || rc=$?

# Fold the container's summary into the GitHub Actions job summary so the
# success/failure outcome is visible there, not just in the logs. Done on both
# success and failure — a failed notification exits non-zero, and that's
# exactly the case we most want surfaced.
if [[ -n "${GITHUB_STEP_SUMMARY:-}" && -f "$summary_file" ]]; then
  # On a write failure, warn rather than `|| true`: a silently-empty job
  # summary defeats the purpose of this fold. The echo succeeds, so `set -e`
  # won't abort and the captured notify-publish exit code is preserved.
  cat "$summary_file" >> "$GITHUB_STEP_SUMMARY" \
    || echo "warning: could not write to GITHUB_STEP_SUMMARY" >&2
fi

exit "$rc"

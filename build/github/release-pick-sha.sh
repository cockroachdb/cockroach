#!/usr/bin/env bash

# Copyright 2026 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

# GitHub Actions wrapper for the daily pick-SHA workflow. Runs the
# release binary inside the bazel docker container (via run_bazel) so
# the workflow doesn't depend on the host runner having a compatible
# bazel / Go toolchain.
#
# Expects to be called by a step that has fetched the release secrets
# via google-github-actions/get-secretmanager-secrets and bound them
# to the env vars below. The :? defaults fail fast if the calling
# workflow drops one.
#
# NOTE: This script intentionally does NOT use set -x. It handles
# secrets that must never appear in build logs.

set -euo pipefail

: "${JIRA_API_TOKEN:?must be set by the workflow}"
: "${JIRA_EMAIL:?must be set by the workflow}"
: "${GITHUB_TOKEN:?must be set by the workflow}"
: "${SLACK_BOT_TOKEN:?must be set by the workflow}"
: "${RELEASE_NOTES_API_KEY:?must be set by the workflow}"

dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "$dir/build/teamcity-support.sh"        # for $root
source "$dir/build/teamcity-bazel-support.sh"  # for run_bazel

# The impl script writes its version-check summary here (container path
# /artifacts maps to $root/artifacts on the host). Clear any stale copy from
# an earlier step so we only fold this run's results into the job summary.
summary_file="$root/artifacts/pick-sha-summary.md"
rm -f "$summary_file"

# Forward the secrets and the workflow-supplied DRY_RUN / TEST_ISSUE_KEY
# toggles into the container. Everything the impl script and the
# release binary read must be enumerated here. GITHUB_REPOSITORY is
# auto-set by the GHA runner and read by the binary's defaultRepo()
# helper to pick which GitHub repo holds the staging branches; without
# it the binary falls back to cockroachdb/cockroach and looks for the
# staging branch in the wrong place.
#
# Capture the exit code rather than letting `set -e` abort here: we must
# fold the summary (below) before propagating the failure, and the workflow's
# `if: failure()` Slack step relies on this script preserving the non-zero
# exit on a version mismatch or any other error.
rc=0
BAZEL_SUPPORT_EXTRA_DOCKER_ARGS="-e DRY_RUN -e TEST_ISSUE_KEY -e JIRA_API_TOKEN -e JIRA_EMAIL -e GITHUB_TOKEN -e SLACK_BOT_TOKEN -e RELEASE_NOTES_API_KEY -e GITHUB_REPOSITORY -e IS_PRODUCTION_REPO" \
  run_bazel build/github/release-pick-sha-impl.sh || rc=$?

# Fold the container's version-check summary into the GitHub Actions job
# summary so a version.txt/ticket mismatch is visible there, not just in the
# logs. Done on both success and failure — a mismatch exits non-zero, and
# that's exactly the case we most want surfaced.
if [[ -n "${GITHUB_STEP_SUMMARY:-}" && -f "$summary_file" ]]; then
  # `|| true` so a cat/write failure here can't abort under `set -e` and
  # clobber the captured pick-sha exit code with cat's.
  cat "$summary_file" >> "$GITHUB_STEP_SUMMARY" || true
fi

exit "$rc"

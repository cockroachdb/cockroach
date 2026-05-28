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

# Forward the secrets and the publish SHA into the container.
# IS_PRODUCTION_REPO is read by the binary's isProductionRepo() helper to
# pick the Slack channel; without it the binary defaults to #db-release-test.
BAZEL_SUPPORT_EXTRA_DOCKER_ARGS="-e JIRA_API_TOKEN -e JIRA_EMAIL -e SLACK_BOT_TOKEN -e BUILD_VCS_NUMBER -e IS_PRODUCTION_REPO" \
  run_bazel build/github/release-notify-publish-impl.sh

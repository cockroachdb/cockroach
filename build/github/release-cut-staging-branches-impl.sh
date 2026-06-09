#!/usr/bin/env bash

# Copyright 2026 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

# Inner half of the branch-cut wrapper. Runs INSIDE the bazel docker
# container that run_bazel sets up: builds the release binary against
# the workspace mounted at /go/src/github.com/cockroachdb/cockroach
# and invokes its `cut-staging-branches` subcommand with the args
# derived from the calling workflow's DRY_RUN / TEST_ISSUE_KEY env.
#
# Secrets (JIRA_API_TOKEN, GITHUB_TOKEN, SLACK_BOT_TOKEN, etc.) are
# forwarded by the wrapper via the docker -e passthrough; the binary
# reads them from its own env.

set -euo pipefail

bazel build --config=crosslinux //pkg/cmd/release

args=()
if [[ "${DRY_RUN:-}" == "true" ]]; then
  args+=(--dry-run)
fi
if [[ -n "${TEST_ISSUE_KEY:-}" ]]; then
  args+=(--test-issue-key "$TEST_ISSUE_KEY")
fi

# Write per-ticket branch-cut results to a file under the mounted /artifacts
# dir (host: $root/artifacts). The host wrapper folds this into
# $GITHUB_STEP_SUMMARY after the container exits so the cut/resumed/failed
# outcomes show up in the job summary, not just the logs.
args+=(--summary-file /artifacts/cut-staging-summary.md)

$(bazel info --config=crosslinux bazel-bin)/pkg/cmd/release/release_/release \
  cut-staging-branches "${args[@]}"

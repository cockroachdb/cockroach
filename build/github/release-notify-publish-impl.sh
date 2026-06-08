#!/usr/bin/env bash

# Copyright 2026 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

# Inner half of the notify-publish wrapper. Runs INSIDE the bazel docker
# container that run_bazel sets up: builds the release binary against
# the workspace mounted at /go/src/github.com/cockroachdb/cockroach and
# invokes its `notify-publish` subcommand with --sha=$BUILD_VCS_NUMBER.
#
# Secrets (JIRA_API_TOKEN, JIRA_EMAIL, SLACK_BOT_TOKEN) are forwarded by
# the wrapper via docker -e passthrough; the binary reads them from its
# own env. pkg/build/version.txt is read at the workspace path the
# workflow's checkout step left it at — the publish SHA's version.

set -euo pipefail

bazel build --config=crosslinux //pkg/cmd/release

args=(--sha "$BUILD_VCS_NUMBER")

# Write the publish-notification result to a file under the mounted /artifacts
# dir (host: $root/artifacts). The host wrapper folds this into
# $GITHUB_STEP_SUMMARY after the container exits so the success/failure outcome
# shows up in the job summary, not just the logs.
args+=(--summary-file /artifacts/notify-publish-summary.md)

$(bazel info --config=crosslinux bazel-bin)/pkg/cmd/release/release_/release \
  notify-publish "${args[@]}"

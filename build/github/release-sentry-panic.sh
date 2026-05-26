#!/usr/bin/env bash

# Copyright 2026 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

# GitHub Actions wrapper for triggering a Sentry panic on a new release.
# Expects SENTRY_AUTH_TOKEN to be set by the calling workflow step (fetched
# via google-github-actions/get-secretmanager-secrets, which auto-masks the
# value with ::add-mask::).
#
# NOTE: This script intentionally does NOT use set -x. It handles secrets
# that must never appear in build logs.

set -euo pipefail

if [[ -n "${DRY_RUN:-}" ]] ; then
  echo "Skipping this step in dry-run mode"
  exit
fi

: "${SENTRY_AUTH_TOKEN:?must be set by the workflow}"

export VERSION=$(grep -v "^#" "pkg/build/version.txt" | head -n1)
echo "Triggering Sentry panic for $VERSION..."

# Call the existing script which runs the sentry tool via Bazel in Docker.
exec build/teamcity/internal/release/process/trigger-sentry-panic.sh

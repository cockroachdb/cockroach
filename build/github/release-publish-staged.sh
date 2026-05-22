#!/usr/bin/env bash

# Copyright 2026 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

# GitHub Actions wrapper for publishing a staged cockroach release. Expects
# DOCKER_ACCESS_TOKEN, DOCKER_ID, and (for non-dry-run) GH_TOKEN for git
# tagging over HTTPS to be set by the calling workflow step (fetched via
# google-github-actions/get-secretmanager-secrets, which auto-masks the
# values with ::add-mask::).
#
# NOTE: This script intentionally does NOT use set -x. It handles secrets
# that must never appear in build logs.

set -euo pipefail

: "${DOCKER_ACCESS_TOKEN:?must be set by the workflow}"
: "${DOCKER_ID:?must be set by the workflow}"

if [[ -z "${DRY_RUN:-}" ]]; then
  : "${GH_TOKEN:?must be set by the workflow for non-dry-run}"
fi

# Call the existing script. It sources teamcity-support.sh which provides
# log_into_gcloud (no-op with WIF), docker_login_gcr (uses gcloud credential
# helper with WIF), tc_start_block/tc_end_block, etc.
exec build/teamcity/internal/cockroach/release/publish/publish-staged-cockroach-release.sh

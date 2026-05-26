#!/usr/bin/env bash

# Copyright 2026 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

# GitHub Actions wrapper for the post-release "create version-update PRs"
# step. Expects GH_TOKEN, SMTP_PASSWORD, and RELEASED_VERSION to be set by
# the calling workflow step (secrets fetched via
# google-github-actions/get-secretmanager-secrets, which auto-masks the
# values with ::add-mask::).
#
# NOTE: This script intentionally does NOT use set -x. It handles secrets
# that must never appear in build logs.

set -euo pipefail

: "${GH_TOKEN:?must be set by the workflow}"
: "${SMTP_PASSWORD:?must be set by the workflow}"
: "${RELEASED_VERSION:?must be set by the workflow}"

# NEXT_VERSION is optional; the release tool computes it from RELEASED_VERSION
# when empty. Export an explicit empty value so the impl script's `-e
# NEXT_VERSION` passthrough sees it.
export NEXT_VERSION="${NEXT_VERSION:-}"

# IS_PRODUCTION_REPO is forwarded from a GHA repository variable (set only
# on the production release repo). Export an explicit empty default so the
# impl script's `-e IS_PRODUCTION_REPO` passthrough sees it on forks where
# the variable is unset.
export IS_PRODUCTION_REPO="${IS_PRODUCTION_REPO:-}"

exec build/teamcity/internal/cockroach/release/process/update_versions.sh

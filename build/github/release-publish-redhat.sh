#!/usr/bin/env bash

# Copyright 2026 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

# GitHub Actions wrapper for publishing to Red Hat. Expects QUAY_REGISTRY_KEY
# and REDHAT_API_TOKEN to be set by the calling workflow step (fetched via
# google-github-actions/get-secretmanager-secrets, which auto-masks the values
# with ::add-mask::).
#
# NOTE: This script intentionally does NOT use set -x. It handles secrets
# that must never appear in build logs.

set -euo pipefail

: "${QUAY_REGISTRY_KEY:?must be set by the workflow}"
: "${REDHAT_API_TOKEN:?must be set by the workflow}"

# Call the existing script.
exec build/teamcity/internal/cockroach/release/publish/publish-redhat-release.sh

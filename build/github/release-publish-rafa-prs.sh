#!/usr/bin/env bash

# Copyright 2026 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

# GitHub Actions wrapper for creating RAFA release pull requests. Expects
# GH_TOKEN to be set by the calling workflow step (fetched via
# google-github-actions/get-secretmanager-secrets, which auto-masks the value
# with ::add-mask::).
#
# NOTE: This script intentionally does NOT use set -x. It handles the
# GitHub PAT which must never appear in build logs.

set -euo pipefail

: "${GH_TOKEN:?must be set by the workflow}"

version=$(grep -v "^#" "pkg/build/version.txt" | head -n1)
echo "Creating RAFA release PRs for $version..."

# Clone rafa-production using a credential helper to avoid leaking the PAT.
# When authenticating with a PAT, GitHub identifies the caller from the
# token alone; the username is a syntactic placeholder, conventionally
# "x-access-token".
echo "Cloning rafa-production..."
git -c "credential.https://github.com.helper=!f(){ echo username=x-access-token; echo password=${GH_TOKEN}; };f" \
  clone https://github.com/cockroachlabs/rafa-production.git rafa-production

# Write version file for the RAFA script.
mkdir -p cockroach/pkg/build
echo "$version" > cockroach/pkg/build/version.txt

# Run the rafa-production release PR script.
echo "Running RAFA release PR script..."
./rafa-production/scripts/release/teamcity-create-release-prs.sh

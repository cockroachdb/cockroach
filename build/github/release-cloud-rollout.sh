#!/usr/bin/env bash

# Copyright 2026 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

# GitHub Actions wrapper for creating cloud rollout RAFA pull requests.
# Expects GH_TOKEN to be set by the calling workflow step (fetched via
# google-github-actions/get-secretmanager-secrets, which auto-masks the value
# with ::add-mask::).
#
# NOTE: This script intentionally does NOT use set -x. It handles the
# GitHub PAT which must never appear in build logs.

set -euo pipefail

if [[ "${ER_RELEASE:-}" == "true" ]]; then
  echo "ER_RELEASE is set, skipping cloud rollout"
  exit 0
fi

# Short-circuit on dry-run. The rafa script doesn't honor DRY_RUN and
# always does a real `git push` to cockroachlabs/rafa-production —
# something a dry run must never do, and something a non-prod fork
# can't do anyway (no PAT scope for the rafa repo). Skip the entire
# rollout so dry-run completes cleanly without touching prod.
if [[ "${DRY_RUN:-}" == "true" ]]; then
  echo "[DRY RUN] would clone cockroachlabs/rafa-production, branch, commit metadata, and push."
  echo "[DRY RUN] skipping cloud rollout."
  exit 0
fi

: "${GH_TOKEN:?must be set by the workflow}"

version=$(grep -v "^#" "pkg/build/version.txt" | head -n1)
echo "Creating cloud rollout PRs for $version..."

# Clone rafa-production using a credential helper to avoid leaking the PAT.
# When authenticating with a PAT, GitHub identifies the caller from the
# token alone; the username is a syntactic placeholder, conventionally
# "x-access-token". Shallow clone (--depth=1 + --single-branch) — the
# rafa script only needs to branch from HEAD, commit, and push; it
# never reads history.
echo "Cloning rafa-production..."
git -c "credential.https://github.com.helper=!f(){ echo username=x-access-token; echo password=${GH_TOKEN}; };f" \
  clone --depth=1 --single-branch \
  https://github.com/cockroachlabs/rafa-production.git rafa-production

# metadata.json is downloaded by the GHA workflow via actions/download-artifact
# into the workspace root. Verify it exists.
if [[ ! -f metadata.json ]]; then
  echo "ERROR: metadata.json not found. It should be downloaded from the publish-cloud-only job."
  exit 1
fi

# Write version file for the rollout script.
mkdir -p cockroach/pkg/build
echo "$version" > cockroach/pkg/build/version.txt

# rafa-production's teamcity-create-cloud-rollout-prs.sh assumes:
#   - its cwd is the PARENT of rafa-production/ (it does its own
#     `cd rafa-production` early), and
#   - metadata.json is inside rafa-production/ (read after that cd).
# So drop a copy of metadata.json into rafa-production/ and invoke the
# script with a path that doesn't change our cwd.
cp metadata.json rafa-production/metadata.json

# The rafa script's git push uses the URL form
#   https://$GH_USERNAME:$GH_TOKEN@github.com/cockroachlabs/rafa-production
# When authenticating with a PAT, the username is a syntactic placeholder
# (the PAT alone identifies the caller); GitHub's documented value is
# "x-access-token". Export it so the rafa child script picks it up —
# without it the URL becomes https://:$GH_TOKEN@... and auth fails.
export GH_USERNAME="x-access-token"

echo "Running cloud rollout script..."
./rafa-production/scripts/release/teamcity-create-cloud-rollout-prs.sh

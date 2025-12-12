#!/usr/bin/env bash

# Copyright 2025 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -xeuo pipefail

dry_run=true
# override dev defaults with production values
if [[ -z "${DRY_RUN:-}" ]] ; then
  echo "Setting production values"
  dry_run=false
fi

# run git fetch in order to get all remote branches
git fetch --tags -q origin

# Ensure we're starting from a clean master branch
git checkout master
git reset --hard origin/master

# install gh CLI tool
curl -fsSL -o /tmp/gh.tar.gz https://github.com/cli/cli/releases/download/v2.32.1/gh_2.32.1_linux_amd64.tar.gz
echo "5c9a70b6411cc9774f5f4e68f9227d5d55ca0bfbd00dfc6353081c9b705c8939  /tmp/gh.tar.gz" | sha256sum -c -
tar --strip-components 1 -xf /tmp/gh.tar.gz
export PATH=$PWD/bin:$PATH

# Build the release tool
bazel build --config=crosslinux //pkg/cmd/release

# Run the update-workflow-branches command and capture output
echo "Running release update-workflow-branches command..."
OUTPUT=$($(bazel info --config=crosslinux bazel-bin)/pkg/cmd/release/release_/release update-workflow-branches 2>&1)
echo "$OUTPUT"

# Extract the branch name from output (line like "Latest release branch: release-26.1")
RELEASE_BRANCH=$(echo "$OUTPUT" | grep "Latest release branch:" | sed 's/Latest release branch: //')

if [[ -z "$RELEASE_BRANCH" ]]; then
  echo "ERROR: Could not determine release branch from command output"
  exit 1
fi

# Check if any changes were made
if git diff --quiet .github/workflows/update_releases.yaml; then
  echo "No changes to workflow file, nothing to commit"
  exit 0
fi

echo "Changes detected in workflow file"

if [[ "$dry_run" == "true" ]]; then
  echo "DRY RUN: Would create PR with the following changes:"
  git diff .github/workflows/update_releases.yaml
  exit 0
fi

# Set git user for commits
export GIT_AUTHOR_NAME="Justin Beaver"
export GIT_COMMITTER_NAME="Justin Beaver"
export GIT_AUTHOR_EMAIL="teamcity@cockroachlabs.com"
export GIT_COMMITTER_EMAIL="teamcity@cockroachlabs.com"

# Check for GitHub token
if [[ -z "${GH_TOKEN:-}" ]]; then
  echo "ERROR: GH_TOKEN environment variable must be set"
  exit 1
fi

# Create a branch for the PR
BRANCH_NAME="update-workflow-branches-$(date +%Y%m%d-%H%M%S)"
git checkout -b "$BRANCH_NAME"

# Commit the changes
git add .github/workflows/update_releases.yaml
git commit -m "workflows: run \`update_releases\` on \`$RELEASE_BRANCH\`

Epic: None
Release note: None
Release justification: non-production (release infra) change."

# Push the branch to cockroach-teamcity fork (like update_releases.yaml workflow does)
git push "https://oauth2:${GH_TOKEN}@github.com/cockroach-teamcity/cockroach" "$BRANCH_NAME"

# Create the pull request from the fork
gh pr create \
  --repo cockroachdb/cockroach \
  --base master \
  --head "cockroach-teamcity:$BRANCH_NAME" \
  --title "workflows: run \`update_releases\` on \`$RELEASE_BRANCH\`" \
  --body "Epic: None
Release note: None
Release justification: non-production (release infra) change."

echo "Pull request created successfully"

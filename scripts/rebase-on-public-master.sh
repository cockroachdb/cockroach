#!/usr/bin/env bash

# Copyright 2026 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

# This script rebases the current branch onto the master branch of the public
# cockroachdb/cockroach repository. It fetches directly from the upstream URL
# without adding a remote.
#
# Usage:
#   scripts/rebase-on-public-master.sh [--push]
#
# Options:
#   --push  After a successful rebase, force-push the current branch to
#           origin/<branch> using --force-with-lease.
#
# The working tree must be clean (no uncommitted changes) before running.

set -euo pipefail

UPSTREAM_URL="https://github.com/cockroachdb/cockroach.git"
UPSTREAM_BRANCH="master"
PUSH=false

for arg in "$@"; do
  case "$arg" in
    --push) PUSH=true ;;
    *)
      echo "Unknown argument: $arg" >&2
      echo "Usage: $0 [--push]" >&2
      exit 1
      ;;
  esac
done

BRANCH=$(git rev-parse --abbrev-ref HEAD)

if ! git diff-index --quiet HEAD -- || ! git diff --staged --quiet; then
  echo "Error: working tree is not clean. Commit or stash your changes first." >&2
  exit 1
fi

echo "Fetching $UPSTREAM_BRANCH from $UPSTREAM_URL..."
git fetch "$UPSTREAM_URL" "$UPSTREAM_BRANCH"

echo "Rebasing $BRANCH onto upstream $UPSTREAM_BRANCH..."
if ! git rebase --rebase-merges FETCH_HEAD; then
  echo
  echo "Rebase encountered conflicts. Resolve them, then run:"
  echo "  git rebase --continue"
  echo
  echo "Once the rebase is complete, push with:"
  echo "  git push --force-with-lease --no-verify origin $BRANCH"
  exit 1
fi

if [ "$PUSH" = true ]; then
  echo "Pushing $BRANCH to origin..."
  git push --force-with-lease --no-verify origin "$BRANCH"
fi

echo "Done."

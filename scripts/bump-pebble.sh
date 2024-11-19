#!/usr/bin/env bash

# Copyright 2021 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


# NOTE: After a new release has been cut, update this to the appropriate
# Cockroach branch name (i.e. release-23.2, etc.), and corresponding Pebble
# branch name (e.g. crl-release-23.2, etc.). Also update pebble nightly scripts
# in build/teamcity/cockroach/nightlies to use `@crl-release-xy.z` instead of
# `@master`.
BRANCH=master
PEBBLE_BRANCH=master

# This script may be used to produce a branch bumping the Pebble version. The
# storage team bumps CockroachDB's Pebble dependency frequently, and this script
# automates some of that work.
#
# To bump the Pebble dependency to the corresponding pebble branch HEAD, run:
#
#   ./scripts/bump-pebble.sh [<pebble-sha>]
#
# If <pebble-sha> is not provided, the latest sha on $PEBBLE_BRANCH is used.
#
# The script must be run from the cockroach repo root, and the repo should be up
# to date. If $BRANCH is checked out, the script first creates a new branch and
# checks it out. Otherwise, the current branch is used.

set -euo pipefail

pushd() { builtin pushd "$@" > /dev/null; }
popd() { builtin popd "$@" > /dev/null; }

# Grab the current Pebble SHA.
OLD_SHA=$(grep 'github.com/cockroachdb/pebble' go.mod | grep -o -E '[a-f0-9]{12}$')
echo "Current pebble SHA: $OLD_SHA"

git submodule update --init --recursive

PEBBLE_UPSTREAM_URL="https://github.com/cockroachdb/pebble.git"

# Check out the pebble repo in a temporary directory.
PEBBLE_DIR=$(mktemp -d)
trap "rm -rf $PEBBLE_DIR" EXIT

echo
git clone --no-checkout "$PEBBLE_UPSTREAM_URL" "$PEBBLE_DIR"
echo

pushd "$PEBBLE_DIR"
if [ -z "${1-}" ]; then
  # Use the latest commit in the correct branch.
  NEW_SHA=$(git rev-parse "origin/$PEBBLE_BRANCH")
  echo "Using latest pebble $PEBBLE_BRANCH SHA $NEW_SHA"
else
  NEW_SHA=$(git rev-parse "$1")
  # Verify that the given commit is in the correct pebble branch.
  if ! git merge-base --is-ancestor $NEW_SHA "origin/$PEBBLE_BRANCH"; then
    echo "Error: $NEW_SHA is not an ancestor of the pebble branch $PEBBLE_BRANCH" >&2
    exit 1
  fi
  echo "Using provided SHA $NEW_SHA."
fi

# Sanity check: the old SHA should be an ancestor of the new SHA.
if ! git merge-base --is-ancestor $OLD_SHA $NEW_SHA; then
  echo "Error: current pebble SHA $OLD_SHA is not an ancestor of $NEW_SHA (?!)" >&2
  exit 1
fi

COMMITS=$(git log --no-merges --pretty='format: * [`%h`](https://github.com/cockroachdb/pebble/commit/%h) %s' "$OLD_SHA..$NEW_SHA")
popd

echo
echo "$COMMITS"
echo

# If the script is run from $BRANCH, create a new local branch.
CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
if [ "$CURRENT_BRANCH" == "$BRANCH" ]; then
  COCKROACH_BRANCH="$USER/pebble-${BRANCH}-${NEW_SHA:0:12}"
  echo "Creating and switching to new branch $COCKROACH_BRANCH"
  git branch -D "$COCKROACH_BRANCH" || true
  git checkout -b $COCKROACH_BRANCH
else
  echo "Using current branch $CURRENT_BRANCH."
fi

# Pull in the Pebble module at the desired SHA.
./dev generate go
go get "github.com/cockroachdb/pebble@${NEW_SHA}"
go mod tidy

# Create the branch and commit on the CockroachDB repository.
./dev generate bazel --mirror
git add go.mod go.sum DEPS.bzl build/bazelutil/distdir_files.bzl
git commit -m "go.mod: bump Pebble to ${NEW_SHA:0:12}

Changes:

$COMMITS

Release note: none.
Epic: none.
"
# Open an editor to allow the user to set the release note.
git commit --amend

#!/usr/bin/env bash

# This script may be used to produce a branch bumping the Pebble
# version. The storage team bumps CockroachDB's Pebble dependency
# frequently, and this script automates some of that work.
#
# To bump the Pebble dependency to the corresponding pebble branch HEAD, run:
#
#   ./scripts/bump-pebble.sh
#
# Note that this script has different behaviour based on the branch on which it
# is run.

set -euo pipefail

echoerr() { printf "%s\n" "$*" >&2; }
pushd() { builtin pushd "$@" > /dev/null; }
popd() { builtin popd "$@" > /dev/null; }

# NOTE: After a new release has been cut, update this to the appropriate
# Cockroach branch name (i.e. release-22.2, etc.), and corresponding Pebble
# branch name (e.g. crl-release-21.2, etc.).
BRANCH=master
PEBBLE_BRANCH=master

# The script can only be run from a specific branch.
if [ "$(git rev-parse --abbrev-ref HEAD)" != "$BRANCH" ]; then
  echo "This script must be run from the $BRANCH branch."
  exit 1
fi

COCKROACH_DIR="$(go env GOPATH)/src/github.com/cockroachdb/cockroach"
PEBBLE_DIR="$(go env GOPATH)/src/github.com/cockroachdb/pebble"
UPSTREAM="upstream"

# Using count since error code is ignored
MATCHING_UPSTREAMS=$(git remote | grep -c ${UPSTREAM} || true )
if [ $MATCHING_UPSTREAMS = 0 ]; then
  echo This script expects the upstream to point to github.com/cockroachdb/cockroach.
  echo However no remote matches \"$UPSTREAM\".
  echo The available remotes are:
  git remote
  read -p "Please enter a remote to use: " UPSTREAM
fi

# Make sure that the cockroachdb remotes match what we expect. The
# `upstream` remote must point to github.com/cockroachdb/cockroach.
pushd "$COCKROACH_DIR"
git submodule update --init --recursive
popd

COCKROACH_UPSTREAM_URL="https://github.com/cockroachdb/cockroach.git"
PEBBLE_UPSTREAM_URL="https://github.com/cockroachdb/pebble.git"

# Ensure the local CockroachDB release branch is up-to-date with
# upstream and grab the current Pebble SHA.
pushd "$COCKROACH_DIR"
git fetch "$COCKROACH_UPSTREAM_URL" "$BRANCH"
git checkout "$BRANCH"
git rebase "$UPSTREAM/$BRANCH"
OLD_SHA=$(grep 'github.com/cockroachdb/pebble' go.mod | grep -o -E '[a-f0-9]{12}$')
popd

# Ensure the local Pebble release branch is up-to-date with upstream,
# and grab the desired Pebble SHA.
pushd "$PEBBLE_DIR"
git fetch "$PEBBLE_UPSTREAM_URL" "$PEBBLE_BRANCH"
NEW_SHA=$(git rev-parse "$UPSTREAM/$PEBBLE_BRANCH")
COMMITS=$(git log --pretty='format:%h %s' "$OLD_SHA..$NEW_SHA" | grep -v 'Merge pull request')
echo "$COMMITS"
popd

COCKROACH_BRANCH="$USER/pebble-${BRANCH}-${NEW_SHA:0:12}"

# Pull in the Pebble module at the desired SHA.
pushd "$COCKROACH_DIR"
./dev generate go
go get "github.com/cockroachdb/pebble@${NEW_SHA}"
go mod tidy
popd

# Create the branch and commit on the CockroachDB repository.
pushd "$COCKROACH_DIR"
./dev generate bazel --mirror
git add go.mod go.sum DEPS.bzl build/bazelutil/distdir_files.bzl
git branch -D "$COCKROACH_BRANCH" || true
git checkout -b "$COCKROACH_BRANCH"
git commit -m "go.mod: bump Pebble to ${NEW_SHA:0:12}

$COMMITS

Release note:
"
# Open an editor to allow the user to set the release note.
git commit --amend
popd

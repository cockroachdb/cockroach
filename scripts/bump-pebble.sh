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
BRANCH=release-22.2
PEBBLE_BRANCH=crl-release-22.2

# The script can only be run from a specific branch.
if [ "$(git rev-parse --abbrev-ref HEAD)" != "$BRANCH" ]; then
  echo "This script must be run from the $BRANCH branch."
  exit 1
fi

COCKROACH_DIR="$(go env GOPATH)/src/github.com/cockroachdb/cockroach"
PEBBLE_DIR="$(go env GOPATH)/src/github.com/cockroachdb/pebble"
VENDORED_DIR="$COCKROACH_DIR/vendor"

# Make sure that the cockroachdb remotes match what we expect. The
# `upstream` remote must point to github.com/cockroachdb/cockroach.
pushd "$COCKROACH_DIR"
git submodule update --init --recursive
popd

COCKROACH_UPSTREAM_URL="https://github.com/cockroachdb/cockroach.git"
PEBBLE_UPSTREAM_URL="https://github.com/cockroachdb/pebble.git"
VENDORED_UPSTREAM_URL="git@github.com:cockroachdb/vendored.git"

# Ensure the local CockroachDB release branch is up-to-date with
# upstream and grab the current Pebble SHA.
pushd "$COCKROACH_DIR"
git fetch "$COCKROACH_UPSTREAM_URL" "$BRANCH"
git checkout "$BRANCH"
git rebase "upstream/$BRANCH"
OLD_SHA=$(grep 'github.com/cockroachdb/pebble' go.mod | grep -o -E '[a-f0-9]{12}$')
popd

# Ensure the local Pebble release branch is up-to-date with upstream,
# and grab the desired Pebble SHA.
pushd "$PEBBLE_DIR"
git fetch "$PEBBLE_UPSTREAM_URL" "$PEBBLE_BRANCH"
NEW_SHA=$(git rev-parse "upstream/$PEBBLE_BRANCH")
COMMITS=$(git log --pretty='format:%h %s' "$OLD_SHA..$NEW_SHA" | grep -v 'Merge pull request')
echo "$COMMITS"
popd

VENDORED_BRANCH="$USER/pebble-${BRANCH}-${NEW_SHA:0:12}"
COCKROACH_BRANCH="$USER/pebble-${BRANCH}-${NEW_SHA:0:12}"

# Pull in the Pebble module at the desired SHA and rebuild the vendor
# directory.
pushd "$COCKROACH_DIR"
go get "github.com/cockroachdb/pebble@${NEW_SHA}"
go mod tidy
make -k vendor_rebuild
popd

# Commit all the pending vendor directory changes to a new
# github.com/cockroachdb/vendored repository branch, including the
# commit history.
pushd "$VENDORED_DIR"
git branch -D "$VENDORED_BRANCH" || true
git checkout -b "$VENDORED_BRANCH"
git add --all
git commit -m "bump Pebble to ${NEW_SHA:0:12}

$COMMITS"
git push -u --force "$VENDORED_UPSTREAM_URL" "$VENDORED_BRANCH"
popd

# Create the branch and commit on the CockroachDB repository.
pushd "$COCKROACH_DIR"
./dev generate bazel --mirror
git add go.mod go.sum DEPS.bzl build/bazelutil/distdir_files.bzl
git add vendor
git branch -D "$COCKROACH_BRANCH" || true
git checkout -b "$COCKROACH_BRANCH"
git commit -m "vendor: bump Pebble to ${NEW_SHA:0:12}

$COMMITS

Release note:
"
# Open an editor to allow the user to set the release note.
git commit --amend
popd

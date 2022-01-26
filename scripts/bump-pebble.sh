#!/usr/bin/env bash

# This script may be used to produce a branch bumping the Pebble
# version. The storage team bumps CockroachDB's Pebble dependency
# frequently, and this script automates some of that work.
#
# To bump the master branch's Pebble dependency to the current pebble
# repository's master branch HEAD, run:
#
#   ./scripts/bump-pebble.sh master
#
# If you'd like to bump the Pebble version in a release branch, pass the
# release branch name as the sole argument. The script expects there to
# exist a corresponding branch on the Pebble repository, prefixed with
# 'crl-'. To bump the `release-21.1` branch's Pebble version, run:
#
#   ./scripts/bump-pebble.sh release-21.1
#
# This command will construct a pull request bumping the Pebble version
# to the current HEAD of the `crl-release-21.1` Pebble branch on the
# Pebble repository.

set -euo pipefail

echoerr() { printf "%s\n" "$*" >&2; }
pushd() { builtin pushd "$@" > /dev/null; }
popd() { builtin popd "$@" > /dev/null; }

BRANCH=${1}
PEBBLE_BRANCH=${BRANCH}
if [ "$BRANCH" != "master" ]; then
    PEBBLE_BRANCH="crl-$BRANCH"
fi
COCKROACH_DIR="$(go env GOPATH)/src/github.com/cockroachdb/cockroach"
PEBBLE_DIR="$(go env GOPATH)/src/github.com/cockroachdb/pebble"
VENDORED_DIR="$COCKROACH_DIR/vendor"

# Make sure that the cockroachdb remotes match what we expect. The
# `upstream` remote must point to github.com/cockroachdb/cockroach.
pushd "$COCKROACH_DIR"
git submodule update --init --recursive
set +e
COCKROACH_UPSTREAM_URL="$(git config --get remote.upstream.url)"
set -e
if [ "$COCKROACH_UPSTREAM_URL" != "git@github.com:cockroachdb/cockroach.git" ]; then
    echoerr "Error: Expected the upstream remote to be the primary cockroachdb repository"
    echoerr "at git@github.com:cockroachdb/cockroach.git. Found:"
    echoerr ""
    echoerr "  $COCKROACH_UPSTREAM_URL"
    echoerr ""
    exit 1
fi
popd

# Make sure that the cockroachdb remotes match what we expect. The
# `upstream` remote must point to github.com/cockroachdb/pebble.
pushd "$PEBBLE_DIR"
set +e
PEBBLE_UPSTREAM_URL="$(git config --get remote.upstream.url)"
set -e
if [ "$PEBBLE_UPSTREAM_URL" != "git@github.com:cockroachdb/pebble.git" ]; then
    echoerr "Error: Expected the upstream remote to be the cockroachdb/pebble repository"
    echoerr "at git@github.com:cockroachdb/pebble.git. Found:"
    echoerr ""
    echoerr "  $PEBBLE_UPSTREAM_URL"
    echoerr ""
    exit 1
fi
popd

# Ensure the local CockroachDB release branch is up-to-date with
# upstream and grab the current Pebble SHA.
pushd "$COCKROACH_DIR"
git fetch upstream "$BRANCH"
git checkout "$BRANCH"
git rebase "upstream/$BRANCH"
OLD_SHA=$(grep 'github.com/cockroachdb/pebble' go.mod | grep -o -E '[a-f0-9]{12}$')
popd

# Ensure the local Pebble release branch is up-to-date with upstream,
# and grab the desired Pebble SHA.
pushd "$PEBBLE_DIR"
git fetch upstream "$PEBBLE_BRANCH"
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
git push -u --force origin "$VENDORED_BRANCH"
popd

# Create the branch and commit on the CockroachDB repository.
pushd "$COCKROACH_DIR"
./dev generate bazel --mirror
git add go.mod go.sum DEPS.bzl
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

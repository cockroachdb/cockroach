#!/usr/bin/env bash

# Copyright 2022 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

#
# This script is run by pebble_nightly_metamorphic_crossversion.sh to build test
# binaries of the pebble metamorphic package at different branches. This script
# takes two argments:
#   - The crdb branch to use; the major release must match the pebble branch.
#   - The pebble branch to build, eg "crl-release-22.1" or "master"
#   - A destination directory into which the binary will be copied with the
#     filename <SHA>.test.
# This script prints the pebble SHA that was built.

set -euo pipefail

CRDB_BRANCH="$1"
PEBBLE_BRANCH="$2"
DEST="$3"

RESTORE_COMMIT=$(git rev-parse HEAD)
git fetch origin "$CRDB_BRANCH"
git checkout "origin/$CRDB_BRANCH"

BAZEL_BIN=$(bazel info bazel-bin)

bazel run @go_sdk//:bin/go get "github.com/cockroachdb/pebble@$PEBBLE_BRANCH"

NEW_DEPS_BZL_CONTENT=$(bazel run //pkg/cmd/mirror/go:mirror)
echo "$NEW_DEPS_BZL_CONTENT" > DEPS.bzl

# Use the Pebble SHA from the version in the modified go.mod file.
# Note that we need to pluck the Git SHA from the go.sum-style version, i.e.
# v0.0.0-20220214174839-6af77d5598c9SUM => 6af77d5598c9
# In some cases if there's no Git SHA because we're right at a tag (eg. v1.1.0),
# we have the second cut to grab the entire tag name as the SHA.
PEBBLE_SHA=$(grep 'github\.com/cockroachdb/pebble' go.mod | cut -d'-' -f3 | cut -d' ' -f2)

bazel build --define gotags=bazel,invariants \
      @com_github_cockroachdb_pebble//internal/metamorphic:metamorphic_test

cp $BAZEL_BIN/external/com_github_cockroachdb_pebble/internal/metamorphic/metamorphic_test_/metamorphic_test \
    "$DEST/$PEBBLE_SHA.test"
chmod a+w "$DEST/$PEBBLE_SHA.test"

# Discard changes.
git checkout .

# Go back. This is important because the current tree is used for the next
# invocation of this script.
git checkout -q "$RESTORE_COMMIT"

echo "$PEBBLE_SHA"

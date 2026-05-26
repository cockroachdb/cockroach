#!/usr/bin/env bash

# Copyright 2024 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

#
# This script is run by the Pebble Nightly Metamorphic Two Instance - TeamCity
# build configuration.

set -euo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"

source "$dir/teamcity-support.sh"  # For $root
source "$dir/teamcity-bazel-support.sh"  # For run_bazel

mkdir -p artifacts

# Pull in the latest version of Pebble from upstream. The benchmarks run
# against the tip of the 'master' branch. We do this by `go get`ting the
# latest version of the module, and then running `mirror` to update `DEPS.bzl`
# accordingly.
bazel run @go_sdk//:bin/go get github.com/cockroachdb/pebble@master
# Just dump the diff to see what, if anything, has changed.
git diff
NEW_DEPS_BZL_CONTENT=$(bazel run //pkg/cmd/mirror/go:mirror)
echo "$NEW_DEPS_BZL_CONTENT" > DEPS.bzl

# Use the Pebble SHA from the version in the modified go.mod file.
# Note that we need to pluck the Git SHA from the go.sum-style version, i.e.
# v0.0.0-20220214174839-6af77d5598c9SUM => 6af77d5598c9
PEBBLE_SHA=$(grep 'github\.com/cockroachdb/pebble' go.mod | cut -d'-' -f3)
echo "Pebble module Git SHA: $PEBBLE_SHA"

BAZEL_SUPPORT_EXTRA_DOCKER_ARGS="-e BUILD_VCS_NUMBER=$PEBBLE_SHA -e GITHUB_API_TOKEN -e GITHUB_REPO -e TC_BUILDTYPE_ID -e TC_BUILD_BRANCH -e TC_BUILD_ID -e TC_SERVER_URL" \
                               run_bazel build/teamcity/cockroach/nightlies/pebble_nightly_metamorphic_two_instance_impl.sh

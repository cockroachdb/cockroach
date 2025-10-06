#!/usr/bin/env bash

# Copyright 2022 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

#
# This script is run by the Pebble Nightly Crossversion Metamorphic - TeamCity
# build configuration.

set -euo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"

source "$dir/teamcity-support.sh"  # For $root
source "$dir/teamcity-bazel-support.sh"  # For run_bazel

mkdir -p bin
chmod o+rwx bin
mkdir -p $root/artifacts

VERSIONS=""
LAST_SHA=""
for branch in "$@"
do
    tc_start_block "Compile Pebble $branch metamorphic test binary"
    SHA=$("$dir/teamcity/cockroach/nightlies/pebble_nightly_build_test_binary.sh" "$branch" "bin" | tail -n1)
    VERSIONS="$VERSIONS -version $branch,$SHA,/test-bin/$SHA.test"
    LAST_SHA="$SHA"
    echo "$PWD/bin/$SHA.test"
    stat "$PWD/bin/$SHA.test"
    tc_end_block "Compile Pebble $branch metamorphic test binary"
done

ls -l "$PWD/bin/"

BAZEL_SUPPORT_EXTRA_DOCKER_ARGS="-e BUILD_VCS_NUMBER=$LAST_SHA -e GITHUB_API_TOKEN -e GITHUB_REPO -e TC_BUILDTYPE_ID -e TC_BUILD_BRANCH -e TC_BUILD_ID -e TC_SERVER_URL --mount type=bind,source=$PWD/bin,target=/test-bin" \
                               run_bazel \
                               build/teamcity/cockroach/nightlies/pebble_nightly_metamorphic_crossversion_impl.sh \
                               "$VERSIONS"

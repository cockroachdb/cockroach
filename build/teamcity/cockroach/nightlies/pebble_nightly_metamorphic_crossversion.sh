#!/usr/bin/env bash
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

bazel build //pkg/cmd/bazci --config=ci

VERSIONS=""
LAST_SHA=""
for branch in "$@"
do
    tc_start_block "Compile Pebble $branch metamorphic test binary"
    SHA=$($dir/teamcity/cockroach/nightlies/pebble_nightly_build_test_binary.sh "$branch" "bin")
    VERSIONS="$VERSIONS -version $branch,$SHA,./bin/$SHA.test"
    LAST_SHA="$SHA"
    tc_end_block "Compile Pebble $branch metamorphic test binary"
done

ls -l bin/

BAZEL_SUPPORT_EXTRA_DOCKER_ARGS="-e BUILD_VCS_NUMBER=$LAST_SHA -e GITHUB_API_TOKEN -e GITHUB_REPO -e TC_BUILDTYPE_ID -e TC_BUILD_BRANCH -e TC_BUILD_ID -e TC_SERVER_URL" \
                               run_bazel
                               build/teamcity/cockroach/nightlies/pebble_nightly_metamorphic_crossversion_impl.sh \
                               "$VERSIONS"

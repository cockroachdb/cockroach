#!/usr/bin/env bash
#
# This script runs the Pebble Nightly write-throughput benchmarks.
#
# It is run by the Pebble Nightly Write Throughput build
# configuration.

set -eo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"

source "$dir/teamcity-support.sh"  # For $root
source "$dir/teamcity-bazel-support.sh"  # For run_bazel

BAZEL_SUPPORT_EXTRA_DOCKER_ARGS="-e LITERAL_ARTIFACTS_DIR=$root/artifacts -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e GOOGLE_EPHEMERAL_CREDENTIALS -e TC_BUILDTYPE_ID -e TC_BUILD_BRANCH -e TC_BUILD_ID -e TC_SERVER_URL" \
                               run_bazel build/teamcity/cockroach/nightlies/pebble_nightly_write_throughput_impl.sh

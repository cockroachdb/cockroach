#!/usr/bin/env bash

# Copyright 2024 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

set -xeuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname $(dirname $(dirname "${0}"))))))"

source "$dir/teamcity-support.sh"  # For $root
source "$dir/teamcity-bazel-support.sh"  # For run_bazel

cleanup() {
    git clean -dfx
}
trap cleanup EXIT

BAZEL_SUPPORT_EXTRA_DOCKER_ARGS="-e GOOGLE_CREDENTIALS -e TC_API_PASSWORD -e TC_API_USER -e TC_BUILD_BRANCH -e TC_SERVER_URL" \
  run_bazel build/teamcity/internal/cockroach/build/ci/generate-profile-impl.sh

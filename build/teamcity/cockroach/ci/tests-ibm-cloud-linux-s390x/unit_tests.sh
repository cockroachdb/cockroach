#!/usr/bin/env bash

# Copyright 2022 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -euo pipefail

dir="$(dirname $(dirname $(dirname $(dirname $(dirname "${0}")))))"

source "$dir/teamcity-support.sh"  # For $root
source "$dir/teamcity-bazel-support.sh"  # For run_bazel

# Mint a token so Bazel can fetch private Go module dependencies from the
# cockroach-godeps bucket; forwarded into the container via the -e below. s390x
# agents have no gcloud, so this takes the JWT fallback path internally.
configure_bazel_storage_access_token

tc_start_block "Run unit tests"
BAZEL_SUPPORT_EXTRA_DOCKER_ARGS="-e TC_BUILD_BRANCH -e GITHUB_API_TOKEN -e BUILD_VCS_NUMBER -e TC_BUILD_ID -e TC_SERVER_URL -e TC_BUILDTYPE_ID -e GITHUB_REPO -e BAZEL_STORAGE_ACCESS_TOKEN" run_bazel build/teamcity/cockroach/ci/tests-ibm-cloud-linux-s390x/unit_tests_impl.sh
tc_end_block "Run unit tests"

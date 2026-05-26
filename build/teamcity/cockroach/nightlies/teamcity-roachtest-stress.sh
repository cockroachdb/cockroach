#!/usr/bin/env bash

# Copyright 2023 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -exuo pipefail

source "$(dirname "${0}")/../../../teamcity-support.sh"
# All COCKROACH_* env vars are automatically captured and passed into the
# Docker container by DOCKER_EXPORT_COCKROACH_VARS in teamcity-bazel-support.sh.
# Any COCKROACH_* env vars you want captured must be set before this source.
source "$(dirname "${0}")/../../../teamcity-bazel-support.sh" # For run_bazel

BAZEL_SUPPORT_EXTRA_DOCKER_ARGS="-e LITERAL_ARTIFACTS_DIR=$root/artifacts -e BUILD_VCS_NUMBER -e CLOUD -e TESTS -e COUNT -e GCE_ZONES -e GITHUB_API_TOKEN -e GITHUB_ORG -e GITHUB_REPO -e GOOGLE_CREDENTIALS -e GOOGLE_KMS_KEY_A -e GOOGLE_KMS_KEY_B -e SLACK_TOKEN -e TC_BUILDTYPE_ID -e TC_BUILD_BRANCH -e TC_BUILD_ID -e TC_SERVER_URL -e DEBUG" \
  run_bazel build/teamcity/cockroach/nightlies/roachtest_stress_impl.sh

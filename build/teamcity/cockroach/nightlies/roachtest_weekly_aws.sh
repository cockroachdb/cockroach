#!/usr/bin/env bash

# Copyright 2023 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -exuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"

source "$dir/teamcity-support.sh"  # For $root
source "$dir/teamcity-bazel-support.sh"  # For run_bazel

BAZEL_SUPPORT_EXTRA_DOCKER_ARGS="-e LITERAL_ARTIFACTS_DIR=$root/artifacts -e AWS_ACCESS_KEY_ID -e AWS_ACCESS_KEY_ID_ASSUME_ROLE -e AWS_KMS_KEY_ARN_A -e AWS_KMS_KEY_ARN_B -e AWS_KMS_REGION_A -e AWS_KMS_REGION_B -e AWS_ROLE_ARN -e AWS_SECRET_ACCESS_KEY -e AWS_SECRET_ACCESS_KEY_ASSUME_ROLE -e BUILD_VCS_NUMBER -e CLOUD -e COCKROACH_DEV_LICENSE -e TESTS -e COUNT -e GITHUB_API_TOKEN -e GITHUB_ORG -e GITHUB_REPO -e GOOGLE_EPHEMERAL_CREDENTIALS -e SLACK_TOKEN -e TC_BUILDTYPE_ID -e TC_BUILD_BRANCH -e TC_BUILD_ID -e TC_SERVER_URL -e ARM_PROBABILITY -e USE_SPOT -e SIDE_EYE_API_TOKEN -e EXPORT_OPENMETRICS -e ROACHPERF_OPENMETRICS_CREDENTIALS" \
			       run_bazel build/teamcity/cockroach/nightlies/roachtest_weekly_impl.sh

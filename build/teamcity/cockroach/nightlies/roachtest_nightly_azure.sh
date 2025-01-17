#!/usr/bin/env bash

# Copyright 2023 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -exuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"

source "$dir/teamcity-support.sh"  # For $root
source "$dir/teamcity-bazel-support.sh"  # For run_bazel

BAZEL_SUPPORT_EXTRA_DOCKER_ARGS="-e LITERAL_ARTIFACTS_DIR=$root/artifacts -e AZURE_CLIENT_ID -e AZURE_CLIENT_SECRET -e AZURE_SUBSCRIPTION_ID -e AZURE_TENANT_ID -e BUILD_VCS_NUMBER -e CLOUD -e COCKROACH_DEV_LICENSE -e TESTS -e COUNT -e GITHUB_API_TOKEN -e GITHUB_ORG -e GITHUB_REPO -e GOOGLE_EPHEMERAL_CREDENTIALS -e SLACK_TOKEN -e TC_BUILDTYPE_ID -e TC_BUILD_BRANCH -e TC_BUILD_ID -e TC_SERVER_URL -e SELECT_PROBABILITY -e COCKROACH_RANDOM_SEED -e ROACHTEST_ASSERTIONS_ENABLED_SEED -e ROACHTEST_FORCE_RUN_INVALID_RELEASE_BRANCH -e CLEAR_CLUSTER_CACHE -e ARM_PROBABILITY -e USE_SPOT -e SELECTIVE_TESTS -e SFUSER -e SFPASSWORD -e SIDE_EYE_API_TOKEN -e COCKROACH_EA_PROBABILITY -e EXPORT_OPENMETRICS -e ROACHPERF_OPENMETRICS_CREDENTIALS -e MVT_UPGRADE_PATH -e MVT_DEPLOYMENT_MODE" \
			       run_bazel build/teamcity/cockroach/nightlies/roachtest_nightly_impl.sh

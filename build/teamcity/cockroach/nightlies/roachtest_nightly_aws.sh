#!/usr/bin/env bash

set -exuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"

source "$dir/teamcity-support.sh"  # For $root
source "$dir/teamcity-bazel-support.sh"  # For run_bazel

BAZEL_SUPPORT_EXTRA_DOCKER_ARGS="-e LITERAL_ARTIFACTS_DIR=$root/artifacts -e AWS_ACCESS_KEY_ID -e AWS_KMS_KEY_ARN_A -e AWS_KMS_KEY_ARN_B -e AWS_KMS_REGION_A -e AWS_KMS_REGION_B -e AWS_SECRET_ACCESS_KEY -e BUILD_TAG -e BUILD_VCS_NUMBER -e CLOUD -e COCKROACH_DEV_LICENSE -e COUNT -e GITHUB_API_TOKEN -e GITHUB_ORG -e GITHUB_REPO -e GOOGLE_EPHEMERAL_CREDENTIALS -e SLACK_TOKEN -e TC_BUILD_BRANCH -e TC_BUILD_ID -e TC_SERVER_URL" \
			       run_bazel build/teamcity/cockroach/nightlies/roachtest_nightly_impl.sh

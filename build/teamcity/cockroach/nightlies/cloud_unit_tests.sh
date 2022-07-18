#!/usr/bin/env bash

set -euo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"

source "$dir/teamcity-support.sh"  # For $root
source "$dir/teamcity-bazel-support.sh"  # For run_bazel

tc_start_block "Run cloud unit tests"
BAZEL_SUPPORT_EXTRA_DOCKER_ARGS="-e GITHUB_API_TOKEN -e GITHUB_REPO -e TC_BUILDTYPE_ID -e TC_BUILD_BRANCH -e TC_BUILD_ID -e TC_SERVER_URL -e GOOGLE_EPHEMERAL_CREDENTIALS -e GOOGLE_KMS_KEY_NAME -e GOOGLE_LIMITED_KEY_ID -e ASSUME_SERVICE_ACCOUNT -e GOOGLE_LIMITED_BUCKET -e ASSUME_SERVICE_ACCOUNT_CHAIN -e AWS_S3_BUCKET -e AWS_ASSUME_ROLE -e AWS_ROLE_ARN_CHAIN -e AWS_KMS_KEY_ARN -e AWS_KMS_REGION -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY" \
  run_bazel build/teamcity/cockroach/nightlies/cloud_unit_tests_impl.sh "$@"
tc_end_block "Run cloud unit tests"

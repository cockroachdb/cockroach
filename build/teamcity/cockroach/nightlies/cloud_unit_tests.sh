#!/usr/bin/env bash

# Copyright 2022 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -euo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"

source "$dir/teamcity-support.sh"  # For $root
source "$dir/teamcity-bazel-support.sh"  # For run_bazel

tc_start_block "Run cloud unit tests"
BAZEL_SUPPORT_EXTRA_DOCKER_ARGS="-e GITHUB_API_TOKEN -e GITHUB_REPO -e TC_BUILDTYPE_ID -e TC_BUILD_BRANCH -e TC_BUILD_ID -e TC_SERVER_URL -e GOOGLE_EPHEMERAL_CREDENTIALS -e GOOGLE_KMS_KEY_NAME -e GOOGLE_LIMITED_KEY_ID -e ASSUME_SERVICE_ACCOUNT -e GOOGLE_LIMITED_BUCKET -e ASSUME_SERVICE_ACCOUNT_CHAIN -e AWS_DEFAULT_REGION -e AWS_SHARED_CREDENTIALS_FILE -e AWS_CONFIG_FILE -e AWS_S3_BUCKET -e AWS_ASSUME_ROLE -e AWS_ROLE_ARN_CHAIN -e AWS_KMS_KEY_ARN -e AWS_S3_ENDPOINT -e AWS_KMS_ENDPOINT -e AWS_KMS_REGION -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AZURE_ACCOUNT_KEY -e AZURE_ACCOUNT_NAME -e AZURE_CONTAINER -e AZURE_CLIENT_ID -e AZURE_CLIENT_SECRET -e AZURE_TENANT_ID -e AZURE_VAULT_NAME -e AZURE_LIMITED_VAULT_NAME -e AZURE_KMS_KEY_NAME -e AZURE_KMS_KEY_VERSION -e BUILD_VCS_NUMBER -e TESTS" \
  run_bazel build/teamcity/cockroach/nightlies/cloud_unit_tests_impl.sh "$@"
tc_end_block "Run cloud unit tests"

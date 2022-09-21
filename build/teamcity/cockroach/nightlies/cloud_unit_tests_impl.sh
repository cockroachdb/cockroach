#!/usr/bin/env bash

set -xeuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"
source "$dir/teamcity-bazel-support.sh"  # For process_test_json
source "$dir/teamcity-support.sh"  # For log_into_gcloud

bazel build //pkg/cmd/bazci //pkg/cmd/github-post //pkg/cmd/testfilter --config=ci
BAZEL_BIN=$(bazel info bazel-bin --config=ci)

ARTIFACTS_DIR=/artifacts
GO_TEST_JSON_OUTPUT_FILE=$ARTIFACTS_DIR/test.json.txt

google_credentials="$GOOGLE_EPHEMERAL_CREDENTIALS"
log_into_gcloud
export GOOGLE_APPLICATION_CREDENTIALS="$PWD/.google-credentials.json"

aws_access_key_id="$AWS_ACCESS_KEY_ID"
aws_secret_access_key="$AWS_SECRET_ACCESS_KEY"
aws_default_region="$AWS_DEFAULT_REGION"
mkdir "$PWD/.aws"
export AWS_SHARED_CREDENTIALS_FILE="$PWD/.aws/credentials"
export AWS_CONFIG_FILE="$PWD/.aws/config"
log_into_aws

exit_status=0
$BAZEL_BIN/pkg/cmd/bazci/bazci_/bazci test -- --config=ci \
    //pkg/cloud/gcp:gcp_test //pkg/cloud/amazon:amazon_test //pkg/ccl/cloudccl/gcp:gcp_test //pkg/ccl/cloudccl/amazon:amazon_test \
    --test_env=GO_TEST_WRAP_TESTV=1 \
    --test_env=GO_TEST_WRAP=1 \
    --test_env=GO_TEST_JSON_OUTPUT_FILE=$GO_TEST_JSON_OUTPUT_FILE \
    --test_env=GOOGLE_CREDENTIALS_JSON="$GOOGLE_EPHEMERAL_CREDENTIALS" \
    --test_env=GOOGLE_APPLICATION_CREDENTIALS="$GOOGLE_APPLICATION_CREDENTIALS" \
    --test_env=GOOGLE_BUCKET="nightly-cloud-unit-tests" \
    --test_env=GOOGLE_LIMITED_BUCKET="$GOOGLE_LIMITED_BUCKET" \
    --test_env=GOOGLE_KMS_KEY_NAME="$GOOGLE_KMS_KEY_NAME" \
    --test_env=GOOGLE_LIMITED_KEY_ID="$GOOGLE_LIMITED_KEY_ID" \
    --test_env=ASSUME_SERVICE_ACCOUNT_CHAIN="$ASSUME_SERVICE_ACCOUNT_CHAIN" \
    --test_env=ASSUME_SERVICE_ACCOUNT="$ASSUME_SERVICE_ACCOUNT" \
    --test_env=AWS_S3_BUCKET="$AWS_S3_BUCKET" \
    --test_env=AWS_ASSUME_ROLE="$AWS_ASSUME_ROLE" \
    --test_env=AWS_ROLE_ARN_CHAIN="$AWS_ROLE_ARN_CHAIN" \
    --test_env=AWS_KMS_KEY_ARN="$AWS_KMS_KEY_ARN" \
    --test_env=AWS_KMS_REGION="$AWS_KMS_REGION" \
    --test_env=AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" \
    --test_env=AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" \
    --test_env=AWS_DEFAULT_REGION="$AWS_DEFAULT_REGION" \
    --test_env=AWS_SHARED_CREDENTIALS_FILE="$AWS_SHARED_CREDENTIALS_FILE" \
    --test_env=AWS_CONFIG_FILE="$AWS_CONFIG_FILE" \
    --test_timeout=900 \
    || exit_status=$?

process_test_json \
  $BAZEL_BIN/pkg/cmd/testfilter/testfilter_/testfilter \
  $BAZEL_BIN/pkg/cmd/github-post/github-post_/github-post \
  $ARTIFACTS_DIR \
  $GO_TEST_JSON_OUTPUT_FILE \
  $exit_status

exit $exit_status

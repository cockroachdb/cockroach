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

exit_status=0
$BAZEL_BIN/pkg/cmd/bazci/bazci_/bazci --config=ci \
    test //pkg/cloud/gcp:gcp_test //pkg/cloud/amazon:amazon_test -- \
    --test_env=GO_TEST_WRAP_TESTV=1 \
    --test_env=GO_TEST_WRAP=1 \
    --test_env=GO_TEST_JSON_OUTPUT_FILE=$GO_TEST_JSON_OUTPUT_FILE \
    --test_env=GOOGLE_CREDENTIALS_JSON="$GOOGLE_EPHEMERAL_CREDENTIALS" \
    --test_env=GOOGLE_APPLICATION_CREDENTIALS="$GOOGLE_APPLICATION_CREDENTIALS" \
    --test_env=GOOGLE_BUCKET="nightly-cloud-unit-tests" \
    --test_env=GOOGLE_LIMITED_BUCKET="$GOOGLE_LIMITED_BUCKET" \
    --test_env=GOOGLE_KMS_KEY_NAME="$GOOGLE_KMS_KEY_NAME" \
    --test_env=GOOGLE_LIMITED_KEY_ID="$GOOGLE_LIMITED_KEY_ID" \
    --test_env=ASSUME_SERVICE_ACCOUNT="$ASSUME_SERVICE_ACCOUNT" \
    --test_timeout=60 \
    || exit_status=$?

process_test_json \
  $BAZEL_BIN/pkg/cmd/testfilter/testfilter_/testfilter \
  $BAZEL_BIN/pkg/cmd/github-post/github-post_/github-post \
  $ARTIFACTS_DIR \
  $GO_TEST_JSON_OUTPUT_FILE \
  $exit_status

exit $exit_status

#!/usr/bin/env bash

set -xeuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"
source "$dir/teamcity-bazel-support.sh"  # For process_test_json

bazel build //pkg/cmd/bazci //pkg/cmd/github-post //pkg/cmd/testfilter --config=ci
BAZEL_BIN=$(bazel info bazel-bin --config=ci)

ARTIFACTS_DIR=/artifacts
GO_TEST_JSON_OUTPUT_FILE=$ARTIFACTS_DIR/test.json.txt

exit_status=0
$BAZEL_BIN/pkg/cmd/bazci/bazci_/bazci --config=ci \
    test //pkg/sql/logictest:logictest_test -- \
    --test_arg=--vmodule=*=10 \
    --test_arg=-show-sql \
    --test_filter='^TestLogic$' \
    --test_env=GO_TEST_WRAP_TESTV=1 \
    --test_env=GO_TEST_WRAP=1 \
    --test_env=GO_TEST_JSON_OUTPUT_FILE=$GO_TEST_JSON_OUTPUT_FILE \
    --test_timeout=7200 \
    || exit_status=$?

process_test_json \
  $BAZEL_BIN/pkg/cmd/testfilter/testfilter_/testfilter \
  $BAZEL_BIN/pkg/cmd/github-post/github-post_/github-post \
  $ARTIFACTS_DIR \
  $GO_TEST_JSON_OUTPUT_FILE \
  $exit_status

exit $exit_status

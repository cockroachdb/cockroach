#!/usr/bin/env bash

set -xeuo pipefail

bazel build //pkg/cmd/bazci //pkg/cmd/github-post //pkg/cmd/testfilter --config=ci
BAZEL_BIN=$(bazel info bazel-bin --config=ci)

ARTIFACTS_DIR=/artifacts
export GO_TEST_JSON_OUTPUT_FILE=$ARTIFACTS_DIR/test.json

set +e
GO_TEST_WRAP=1 $BAZEL_BIN/pkg/cmd/bazci/bazci_/bazci --config=ci \
    test //pkg/sql/logictest:logictest_test -- \
    --test_arg=--vmodule=*=10 \
    --test_arg=-show-sql \
    --test_filter='^TestLogic$' \
    --test_timeout=7200
status=$?
set -e

$BAZEL_BIN/pkg/cmd/testfilter/testfilter_/testfilter -mode=strip < $GO_TEST_JSON_OUTPUT_FILE > $ARTIFACTS_DIR/stripped.txt
$BAZEL_BIN/pkg/cmd/testfilter/testfilter_/testfilter -mode=omit < $ARTIFACTS_DIR/stripped.txt > $ARTIFACTS_DIR/failures.txt
# TODO: github-submit and wrap it as a function
exit $status

#!/usr/bin/env bash

set -xeuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"
source "$dir/teamcity-bazel-support.sh"
source "$dir/teamcity/util.sh"

bazel build //pkg/cmd/bazci //pkg/cmd/github-post //pkg/cmd/testfilter --config=ci
BAZEL_BIN=$(bazel info bazel-bin --config=ci)

tc_start_block "Run opt tests with fast_int_set_large"
ARTIFACTS_DIR=/artifacts/fast_int_set_large
mkdir $ARTIFACTS_DIR
GO_TEST_JSON_OUTPUT_FILE=$ARTIFACTS_DIR/test.json.txt
exit_status_large=0
$BAZEL_BIN/pkg/cmd/bazci/bazci_/bazci --config=ci --artifacts $ARTIFACTS_DIR \
    test //pkg/sql/opt:opt_test -- \
    --define gotags=bazel,crdb_test,fast_int_set_large \
    --test_env=GO_TEST_JSON_OUTPUT_FILE=$GO_TEST_JSON_OUTPUT_FILE || exit_status_large=$?
process_test_json \
        $BAZEL_BIN/pkg/cmd/testfilter/testfilter_/testfilter \
        $BAZEL_BIN/pkg/cmd/github-post/github-post_/github-post \
        $ARTIFACTS_DIR \
        $GO_TEST_JSON_OUTPUT_FILE \
        $exit_status_large
tc_end_block "Run opt tests with fast_int_set_large"

# NOTE(ricky): Running both tests in the same configuration with different
# gotags thrashes the cache. These tests are pretty quick so it shouldn't
# matter now but it is something to keep an eye on.
tc_start_block "Run opt tests with fast_int_set_small"
ARTIFACTS_DIR=/artifacts/fast_int_set_small
mkdir $ARTIFACTS_DIR
GO_TEST_JSON_OUTPUT_FILE=$ARTIFACTS_DIR/test.json.txt
exit_status_small=0
$BAZEL_BIN/pkg/cmd/bazci/bazci_/bazci --config=ci \
    test //pkg/sql/opt:opt_test -- \
    --define gotags=bazel,crdb_test,fast_int_set_small \
    --test_env=GO_TEST_JSON_OUTPUT_FILE=$GO_TEST_JSON_OUTPUT_FILE || exit_status_small=$?
process_test_json \
        $BAZEL_BIN/pkg/cmd/testfilter/testfilter_/testfilter \
        $BAZEL_BIN/pkg/cmd/github-post/github-post_/github-post \
        $ARTIFACTS_DIR \
        $GO_TEST_JSON_OUTPUT_FILE \
        $exit_status_large
tc_end_block "Run opt tests with fast_int_set_small"

if [ $exit_status_large -ne 0 ]
then
    exit $exit_status_large
fi

if [ $exit_status_small -ne 0 ]
then
    exit $exit_status_small
fi

#!/usr/bin/env bash

set -xeuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"
source "$dir/teamcity-bazel-support.sh"

bazel build //pkg/cmd/bazci //pkg/cmd/github-post //pkg/cmd/testfilter --config=ci
BAZEL_BIN=$(bazel info bazel-bin --config=ci)
GO_TEST_JSON_OUTPUT_FILE=/artifacts/test.json.txt
exit_status=0
$BAZEL_BIN/pkg/cmd/bazci/bazci_/bazci --config=ci --config=crdb_test_off \
    test //pkg/sql/sqlitelogictest/tests/... -- \
    --test_arg -bigtest --test_arg -flex-types --test_timeout 86400 \
    --test_env=GO_TEST_JSON_OUTPUT_FILE=$GO_TEST_JSON_OUTPUT_FILE || exit_status=$?
process_test_json \
        $BAZEL_BIN/pkg/cmd/testfilter/testfilter_/testfilter \
        $BAZEL_BIN/pkg/cmd/github-post/github-post_/github-post \
        /artifacts \
        $GO_TEST_JSON_OUTPUT_FILE \
        $exit_status
exit $exit_status

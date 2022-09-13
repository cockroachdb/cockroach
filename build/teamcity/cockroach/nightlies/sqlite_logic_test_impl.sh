#!/usr/bin/env bash

set -xeuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"

bazel build //pkg/cmd/bazci --config=ci
BAZEL_BIN=$(bazel info bazel-bin --config=ci)
GO_TEST_JSON_OUTPUT_FILE=/artifacts/test.json.txt
exit_status=0
$BAZEL_BIN/pkg/cmd/bazci/bazci_/bazci --go_test_json_output_file=$GO_TEST_JSON_OUTPUT_FILE -- test --config=ci --config=crdb_test_off \
    //pkg/sql/sqlitelogictest/tests/... \
    --test_arg -bigtest --test_arg -flex-types --test_timeout 86400 \
    --test_env=GO_TEST_JSON_OUTPUT_FILE=$GO_TEST_JSON_OUTPUT_FILE || exit_status=$?

exit $exit_status

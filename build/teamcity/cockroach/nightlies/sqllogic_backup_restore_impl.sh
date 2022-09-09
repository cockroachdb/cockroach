#!/usr/bin/env bash

set -xeuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"
source "$dir/teamcity-bazel-support.sh"  # For process_test_json

bazel build //pkg/cmd/bazci //pkg/cmd/github-post //pkg/cmd/testfilter --config=ci
BAZEL_BIN=$(bazel info bazel-bin --config=ci)
GO_TEST_JSON_OUTPUT_FILE=/artifacts/test.json.txt
exit_status=0

$BAZEL_BIN/pkg/cmd/bazci/bazci_/bazci test -- --config=ci \
    //pkg/ccl/logictestccl/tests/3node-backup-restore/... \
    --test_arg=-show-sql \
    --test_env=COCKROACH_LOGIC_TEST_BACKUP_RESTORE_PROBABILITY=1.0 \
    --test_env=GO_TEST_WRAP_TESTV=1 \
    --test_env=GO_TEST_WRAP=1 \
    --test_env=GO_TEST_JSON_OUTPUT_FILE=$GO_TEST_JSON_OUTPUT_FILE.3node-backup-restore \
    --test_timeout=7200 \
    || exit_status=$?

process_test_json \
  $BAZEL_BIN/pkg/cmd/testfilter/testfilter_/testfilter \
  $BAZEL_BIN/pkg/cmd/github-post/github-post_/github-post \
  $ARTIFACTS_DIR \
  $GO_TEST_JSON_OUTPUT_FILE \
  $exit_status

exit $exit_status

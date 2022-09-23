#!/usr/bin/env bash

set -xeuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"
source "$dir/teamcity-bazel-support.sh"  # For process_test_json

bazel build //pkg/cmd/bazci //pkg/cmd/github-post //pkg/cmd/testfilter --config=ci
BAZEL_BIN=$(bazel info bazel-bin --config=ci)
GO_TEST_JSON_OUTPUT_FILE=/artifacts/test.json.txt
exit_status=0
XML_OUTPUT_FILE=/artifacts/test.xml GO_TEST_WRAP_TESTV=1 GO_TEST_WRAP=1 GO_TEST_JSON_OUTPUT_FILE=$GO_TEST_JSON_OUTPUT_FILE bazel \
    run --config=ci --config=test --define gotags=bazel,gss,nightly \
    //build/bazelutil:lint || exit_status=$?
# The schema of the output test.xml will be slightly wrong -- ask `bazci` to fix
# it up.
$BAZEL_BIN/pkg/cmd/bazci/bazci_/bazci munge-test-xml /artifacts/test.xml
process_test_json \
    $BAZEL_BIN/pkg/cmd/testfilter/testfilter_/testfilter \
    $BAZEL_BIN/pkg/cmd/github-post/github-post_/github-post \
    /artifacts \
    $GO_TEST_JSON_OUTPUT_FILE \
    $exit_status
exit $exit_status

#!/usr/bin/env bash

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"
source "$dir/teamcity-bazel-support.sh"  # For process_test_json

set -euxo pipefail
ARTIFACTS_DIR=/artifacts/meta
mkdir -p $ARTIFACTS_DIR
GO_TEST_JSON_OUTPUT_FILE=/artifacts/test.json.txt

echo "TC_SERVER_URL is $TC_SERVER_URL"

bazel build //pkg/cmd/bazci //pkg/cmd/github-post //pkg/cmd/testfilter --config=ci

BAZEL_BIN=$(bazel info bazel-bin --config ci)

exit_status=0
# NB: If adjusting the metamorphic test flags below, be sure to also update
# pkg/cmd/github-post/main.go to ensure the GitHub issue poster includes the
# correct flags in the reproduction command.
$BAZEL_BIN/pkg/cmd/bazci/bazci_/bazci --config=ci test \
                                      @com_github_cockroachdb_pebble//internal/metamorphic:metamorphic_test -- \
                                      --test_timeout=14400 '--test_filter=TestMeta$' \
                                      --define gotags=bazel,invariants \
                                      "--test_env=GO_TEST_JSON_OUTPUT_FILE=$GO_TEST_JSON_OUTPUT_FILE" \
                                      --run_under "@com_github_cockroachdb_stress//:stress -bazel -shardable-artifacts 'GO_TEST_JSON_OUTPUT_FILE=cat,XML_OUTPUT_FILE=$BAZEL_BIN/pkg/cmd/bazci/bazci_/bazci merge-test-xmls' -maxtime 3h -maxfails 1 -stderr -p 1" \
                                      --test_arg -dir --test_arg $ARTIFACTS_DIR \
                                      --test_arg -ops --test_arg "uniform:5000-10000" \
    || exit_status=$?

BAZEL_SUPPORT_EXTRA_GITHUB_POST_ARGS=--formatter=pebble-metamorphic process_test_json \
    $BAZEL_BIN/pkg/cmd/testfilter/testfilter_/testfilter \
    $BAZEL_BIN/pkg/cmd/github-post/github-post_/github-post \
    /artifacts $GO_TEST_JSON_OUTPUT_FILE $exit_status

exit $exit_status

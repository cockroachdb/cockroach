#!/usr/bin/env bash

set -xeuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"
source "$dir/teamcity-support.sh"
source "$dir/teamcity-bazel-support.sh"

tc_start_block "Run compose tests"

bazel build //pkg/cmd/bazci //pkg/cmd/github-post //pkg/cmd/testfilter --config=ci
BAZEL_BIN=$(bazel info bazel-bin --config=ci)
BAZCI=$BAZEL_BIN/pkg/cmd/bazci/bazci_/bazci

bazel build //pkg/cmd/cockroach //pkg/compose/compare/compare:compare_test --config=ci --config=crosslinux --config=test --config=with_ui
CROSSBIN=$(bazel info bazel-bin --config=ci --config=crosslinux --config=test --config=with_ui)
COCKROACH=$CROSSBIN/pkg/cmd/cockroach/cockroach_/cockroach
COMPAREBIN=$(bazel run //pkg/compose/compare/compare:compare_test --config=ci --config=crosslinux --config=test --config=with_ui --run_under=realpath | grep '^/' | tail -n1)
ARTIFACTS_DIR=$PWD/artifacts
mkdir -p $ARTIFACTS_DIR
GO_TEST_JSON_OUTPUT_FILE=$ARTIFACTS_DIR/test.json.txt

exit_status=0
$BAZCI --artifacts_dir=$ARTIFACTS_DIR -- \
       test --config=ci //pkg/compose:compose_test \
       "--sandbox_writable_path=$ARTIFACTSDIR" \
       "--test_tmpdir=$ARTIFACTSDIR" \
       --test_env=GO_TEST_WRAP_TESTV=1 \
       --test_env=GO_TEST_JSON_OUTPUT_FILE=$GO_TEST_JSON_OUTPUT_FILE \
       --test_arg -cockroach --test_arg $COCKROACH \
       --test_arg -compare --test_arg $COMPAREBIN \
       --test_timeout=1800 || exit_status=$?
process_test_json \
        $BAZEL_BIN/pkg/cmd/testfilter/testfilter_/testfilter \
        $BAZEL_BIN/pkg/cmd/github-post/github-post_/github-post \
        $ARTIFACTS_DIR \
        $GO_TEST_JSON_OUTPUT_FILE \
        $exit_status

tc_end_block "Run compose tests"
exit $exit_status

#!/usr/bin/env bash

# Copyright 2025 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

set -euxo pipefail

# TEST_PACKAGE_TARGETS is a space-separated list of package containing the
# tests in test_to_run.
export TEST_PACKAGE_TARGETS="//pkg/kv/kvnemesis:kvnemesis_test"

# tests_to_run is a hand-curated list of tests that we want to run more often
# under stress.
#
# PLEASE KEEP THIS LIST SHORT.
declare -a tests_to_run=(
  "TestKVNemesisMultiNode"
  "TestKVNemesisMultiNode_BufferedWrites"
)

# These are provided as variables to test the script locally.
ARTIFACTS_DIR="${ARTIFACTS_DIR:-/artifacts/extra_stress}"
BAZEL_CONFIG="${BAZEL_CONFIG:-ci}"

mkdir -p $ARTIFACTS_DIR

echo "ARTIFACTS_DIR=${ARTIFACTS_DIR}"
echo "BAZEL_CONFIG=${BAZEL_CONFIG}"
echo "TC_SERVER_URL=${TC_SERVER_URL}"


# This builds a filter string in the form of ^(TestName1|TestName2|...)$ to
# ensure that we are only running the tests in the tests_to_run array.
test_filter="^("
sep=""
for t in "${tests_to_run[@]}"; do
  test_filter="${test_filter}${sep}${t}"
  sep="|"
done
test_filter="${test_filter})\$"

export RUNS_PER_TEST=100
export TEST_ARGS="--test_timeout=25200 --heavy --runs_per_test $RUNS_PER_TEST --test_filter='${test_filter}'"

bazel build //pkg/cmd/bazci

BAZEL_BIN=$(bazel info bazel-bin)

exit_status=0
$BAZEL_BIN/pkg/cmd/bazci/bazci_/bazci --artifacts_dir $ARTIFACTS_DIR -- test $TEST_PACKAGE_TARGETS \
                                      --config=$BAZEL_CONFIG \
                                      --test_env TC_SERVER_URL=$TC_SERVER_URL \
                                      $TEST_ARGS \
                                      --define gotags=bazel,crdb_test \
                                      --test_output streamed \
    || exit_status=$?

exit $exit_status

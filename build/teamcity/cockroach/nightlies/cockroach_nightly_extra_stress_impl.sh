#!/usr/bin/env bash

# Copyright 2025 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

set -euxo pipefail

# These are provided as variables to test the script locally.
BAZEL_CONFIG="${BAZEL_CONFIG:-ci}"
ARTIFACTS_DIR="${ARTIFACTS_DIR:-/artifacts}"

# TEST_PACKAGE_TARGETS is a space-separated list of package containing the
# tests in test_to_run.
export TEST_PACKAGE_TARGETS="//pkg/kv/kvnemesis:kvnemesis_test //pkg/backup:backup_test"

# tests_to_run is a hand-curated list of tests that we want to run more often
# under stress.
#
# PLEASE KEEP THIS LIST SHORT.
declare -a tests_to_run=(
  "TestKVNemesisMultiNode"
  "TestKVNemesisMultiNode_BufferedWritesLockDurabilityUpgrades"
  "TestBackupRestore_FlakyStorage"
)

test_exists() {
  git grep -q "func ${1}("
}

# This builds a filter string in the form of ^(TestName1|TestName2|...)$ to
# ensure that we are only running the tests in the tests_to_run array.
test_filter="^("
sep=""
for t in "${tests_to_run[@]}"; do
  # We look for this test somewhere in the repository so we can fail the test if
  # someone changes the name of a test without updating this file.
  if ! test_exists "$t"; then
    echo "could not find test ${t}"
    exit 1
  fi

  test_filter="${test_filter}${sep}${t}"
  sep="|"
done
test_filter="${test_filter})\$"

export RUNS_PER_TEST=1000
export TEST_ARGS="--test_timeout=300 --runs_per_test $RUNS_PER_TEST"

mkdir -p $ARTIFACTS_DIR

bazel build //pkg/cmd/bazci

BAZEL_BIN=$(bazel info bazel-bin)

exit_status=0

# Run TestDataDriven from the asim package once with COCKROACH_RUN_ASIM_TESTS enabled.
$BAZEL_BIN/pkg/cmd/bazci/bazci_/bazci -- test //pkg/kv/kvserver/asim/tests:tests_test \
                                      --config=$BAZEL_CONFIG \
                                      --test_env TC_SERVER_URL \
                                      --test_env COCKROACH_RUN_ASIM_TESTS=true \
                                      --test_filter="^TestDataDriven$" \
                                      --test_timeout=600 \
    || exit_status=$?

# Run the rest of the tests under stress.
$BAZEL_BIN/pkg/cmd/bazci/bazci_/bazci -- test $TEST_PACKAGE_TARGETS \
                                      --config=$BAZEL_CONFIG \
                                      --test_env TC_SERVER_URL \
                                      $TEST_ARGS \
                                      --test_filter="$test_filter" \
                                      --test_sharding_strategy=disabled \
    || exit_status=$?

exit $exit_status

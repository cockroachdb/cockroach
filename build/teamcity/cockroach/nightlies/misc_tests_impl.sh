#!/usr/bin/env bash

# Copyright 2025 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -xeuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"
source "$dir/teamcity/util.sh"

bazel build //pkg/cmd/bazci
BAZEL_BIN=$(bazel info bazel-bin)

tc_start_block "Run TestPrettyData"
ARTIFACTS_DIR=/artifacts/pretty
mkdir $ARTIFACTS_DIR
exit_status_pretty=0
$BAZEL_BIN/pkg/cmd/bazci/bazci_/bazci --artifacts_dir $ARTIFACTS_DIR -- \
    test //pkg/sql/sem/tree:tree_test \
    --test_env=COCKROACH_MISC_NIGHTLY=true \
    '--test_filter=TestPrettyData$' \
    --config=ci --define gotags=bazel,crdb_test \
    || exit_status_pretty=$?
tc_end_block "Run TestPrettyData"

tc_start_block "Run TestPebbleEquivalenceNightly"
ARTIFACTS_DIR=/artifacts/pebble_equivalence
mkdir $ARTIFACTS_DIR
exit_status_pebble_equivalence=0
$BAZEL_BIN/pkg/cmd/bazci/bazci_/bazci --artifacts_dir $ARTIFACTS_DIR -- \
    test //pkg/storage/metamorphic:metamorphic_test \
    --test_env=COCKROACH_MISC_NIGHTLY=true \
    --test_timeout=10800 \
    --test_filter=TestPebbleEquivalenceNightly \
    --config=ci --define gotags=bazel,crdb_test \
    || exit_status_pebble_equivalence=$?
tc_end_block "Run TestPebbleEquivalenceNightly"

if [ $exit_status_pebble_equivalence -ne 0 ]
then
    exit $exit_status_pebble_equivalence
fi

if [ $exit_status_pretty -ne 0 ]
then
    exit $exit_status_pretty
fi

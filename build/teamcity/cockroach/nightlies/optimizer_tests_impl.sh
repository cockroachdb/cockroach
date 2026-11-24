#!/usr/bin/env bash

# Copyright 2021 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -xeuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"
source "$dir/teamcity/util.sh"

bazel build //pkg/cmd/bazci
BAZEL_BIN=$(bazel info bazel-bin)

tc_start_block "Run opt tests with fast_int_set_large"
ARTIFACTS_DIR=/artifacts/fast_int_set_large
mkdir $ARTIFACTS_DIR
exit_status_large=0
$BAZEL_BIN/pkg/cmd/bazci/bazci_/bazci --artifacts_dir $ARTIFACTS_DIR -- \
    test //pkg/sql/opt:opt_test --config=ci \
    --define gotags=bazel,crdb_test,fast_int_set_large \
    || exit_status_large=$?
tc_end_block "Run opt tests with fast_int_set_large"

# NOTE(ricky): Running both tests in the same configuration with different
# gotags thrashes the cache. These tests are pretty quick so it shouldn't
# matter now but it is something to keep an eye on.
tc_start_block "Run opt tests with fast_int_set_small"
ARTIFACTS_DIR=/artifacts/fast_int_set_small
mkdir $ARTIFACTS_DIR
exit_status_small=0
$BAZEL_BIN/pkg/cmd/bazci/bazci_/bazci --artifacts_dir $ARTIFACTS_DIR -- \
    test --config=ci \
    //pkg/sql/opt:opt_test \
    --define gotags=bazel,crdb_test,fast_int_set_small \
    || exit_status_small=$?
tc_end_block "Run opt tests with fast_int_set_small"

if [ $exit_status_large -ne 0 ]
then
    exit $exit_status_large
fi

if [ $exit_status_small -ne 0 ]
then
    exit $exit_status_small
fi

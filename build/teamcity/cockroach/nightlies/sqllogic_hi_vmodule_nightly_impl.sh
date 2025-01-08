#!/usr/bin/env bash

# Copyright 2021 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -xeuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"

bazel build //pkg/cmd/bazci
BAZEL_BIN=$(bazel info bazel-bin)

ARTIFACTS_DIR=/artifacts

exit_status=0
$BAZEL_BIN/pkg/cmd/bazci/bazci_/bazci -- test --config=ci \
    //pkg/sql/logictest/tests/... \
    --test_arg=--vmodule=*=10 \
    --test_arg=-show-sql \
    --test_env=GO_TEST_WRAP_TESTV=1 \
    --test_env=GO_TEST_WRAP=1 \
    --test_timeout=7200 \
    || exit_status=$?

exit $exit_status

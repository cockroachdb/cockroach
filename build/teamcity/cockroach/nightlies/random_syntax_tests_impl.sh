#!/usr/bin/env bash

# Copyright 2021 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -xeuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"

bazel build //pkg/cmd/bazci
BAZEL_BIN=$(bazel info bazel-bin)
exit_status=0
$BAZEL_BIN/pkg/cmd/bazci/bazci_/bazci -- test --config=ci \
    //pkg/sql/tests:tests_test \
    --test_arg -rsg=5m --test_arg -rsg-routines=8 --test_arg -rsg-exec-timeout=1m --test_arg -rsg-exec-column-change-timeout=90s \
    --test_timeout 3600 --test_filter 'TestRandomSyntax' \
    --test_sharding_strategy=disabled \
    || exit_status=$?

exit $exit_status

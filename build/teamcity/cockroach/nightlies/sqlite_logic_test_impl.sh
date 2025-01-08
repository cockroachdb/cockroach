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
    //pkg/sql/sqlitelogictest/tests/... \
    --test_arg -bigtest --test_arg -flex-types --test_timeout 86400 \
    || exit_status=$?

exit $exit_status

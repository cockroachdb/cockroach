#!/usr/bin/env bash

set -xeuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"

bazel build //pkg/cmd/bazci --config=ci
BAZEL_BIN=$(bazel info bazel-bin --config=ci)

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

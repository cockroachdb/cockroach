#!/usr/bin/env bash

set -xeuo pipefail

bazel build //pkg/cmd/bazci --config=ci
BAZEL_BIN=$(bazel info bazel-bin --config=ci)

$BAZEL_BIN/pkg/cmd/bazci/bazci_/bazci --config=ci \
    test //pkg/sql/logictest:logictest_test -- \
    --test_arg=--vmodule=*=10 \
    --test_arg=-show-sql \
    --test_filter='^TestLogic$' \
    --test_timeout=7200

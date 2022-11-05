#!/usr/bin/env bash

set -xeuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"

bazel build //pkg/cmd/bazci --config=ci
BAZEL_BIN=$(bazel info bazel-bin --config=ci)
exit_status=0
$BAZEL_BIN/pkg/cmd/bazci/bazci_/bazci -- test --config=ci \
    //pkg/sql/tests:tests_test \
    --test_arg -rsg=5m --test_arg -rsg-routines=8 --test_arg -rsg-exec-timeout=1m \
    --test_timeout 3600 --test_filter 'TestRandomSyntax' \
    --test_sharding_strategy=disabled \
    || exit_status=$?

exit $exit_status

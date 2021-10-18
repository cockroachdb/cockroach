#!/usr/bin/env bash

set -xeuo pipefail

bazel build //pkg/cmd/bazci --config=ci
BAZEL_BIN=$(bazel info bazel-bin --config=ci)
$BAZEL_BIN/pkg/cmd/bazci/bazci_/bazci --config=ci \
    test //pkg/sql/logictest:logictest_test -- \
    --test_arg -bigtest --test_arg -flex-types \
    --define gotags=bazel,crdb_test_off --test_timeout 86400 \
    --test_filter '^TestSqlLiteLogic$|^TestTenantSQLLiteLogic$'

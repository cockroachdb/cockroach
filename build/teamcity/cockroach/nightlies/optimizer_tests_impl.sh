#!/usr/bin/env bash

set -xeuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"
source "$dir/teamcity/util.sh"

bazel build //pkg/cmd/bazci --config=ci
BAZEL_BIN=$(bazel info bazel-bin --config=ci)

tc_start_block "Run opt tests with fast_int_set_large"
$BAZEL_BIN/pkg/cmd/bazci/bazci_/bazci --config=ci \
    test //pkg/sql/opt:opt_test -- \
    --define gotags=bazel,crdb_test,fast_int_set_large
mkdir /artifacts/fast_int_set_large
for FILE in $(ls artifacts | grep -v '^fast_int_set_large$'); do mv /artifacts/$FILE /artifacts/fast_int_set_large; done
tc_end_block "Run opt tests with fast_int_set_large"

# NOTE(ricky): Running both tests in the same configuration with different
# gotags thrashes the cache. These tests are pretty quick so it shouldn't
# matter now but it is something to keep an eye on.
tc_start_block "Run opt tests with fast_int_set_small"
$BAZEL_BIN/pkg/cmd/bazci/bazci_/bazci --config=ci \
    test //pkg/sql/opt:opt_test -- \
    --define gotags=bazel,crdb_test,fast_int_set_small
mkdir /artifacts/fast_int_set_small
for FILE in $(ls artifacts | grep -v '^fast_int_set'); do mv /artifacts/$FILE /artifacts/fast_int_set_small; done
tc_end_block "Run opt tests with fast_int_set_small"

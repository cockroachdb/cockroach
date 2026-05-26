#!/usr/bin/env bash

# Copyright 2022 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -xeuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"
source "$dir/teamcity-support.sh"
source "$dir/teamcity-bazel-support.sh"

tc_start_block "Run compose tests"

bazel build //pkg/cmd/bazci
BAZEL_BIN=$(bazel info bazel-bin)
BAZCI=$BAZEL_BIN/pkg/cmd/bazci/bazci_/bazci

bazel build //pkg/cmd/cockroach //pkg/compose/compare/compare:compare_test //c-deps:libgeos --config=crosslinux --config=test
CROSSBIN=$(bazel info bazel-bin --config=crosslinux --config=test)
COCKROACH=$CROSSBIN/pkg/cmd/cockroach/cockroach_/cockroach
COMPAREBIN=$CROSSBIN/pkg/compose/compare/compare/compare_test_/compare_test
LIBGEOSDIR=$(bazel info execution_root --config=crosslinux --config=test)/external/archived_cdep_libgeos_linux/lib/
ARTIFACTS_DIR=$PWD/artifacts
mkdir -p $ARTIFACTS_DIR

exit_status=0
$BAZCI --artifacts_dir=$ARTIFACTS_DIR -- \
       test --config=ci //pkg/compose:compose_test \
       "--sandbox_writable_path=$ARTIFACTS_DIR" \
       "--test_tmpdir=$ARTIFACTS_DIR" \
       --test_env=GO_TEST_WRAP_TESTV=1 \
       --test_env=COCKROACH_DEV_LICENSE=$COCKROACH_DEV_LICENSE \
       --test_env=COCKROACH_RUN_COMPOSE=true \
       --test_arg -cockroach --test_arg $COCKROACH \
       --test_arg -compare --test_arg $COMPAREBIN \
       --test_arg -libgeosdir --test_arg $LIBGEOSDIR \
       --test_timeout=1800 || exit_status=$?

tc_end_block "Run compose tests"
exit $exit_status

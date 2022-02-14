#!/usr/bin/env bash

set -xeuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"
source "$dir/teamcity-support.sh"

tc_start_block "Run compose tests"

bazel build //pkg/cmd/bazci --config=ci
BAZCI=$(bazel info bazel-bin --config=ci)/pkg/cmd/bazci/bazci_/bazci

bazel build //pkg/cmd/cockroach //pkg/compose/compare/compare:compare_test --config=ci --config=crosslinux --config=test --config=with_ui
CROSSBIN=$(bazel info bazel-bin --config=ci --config=crosslinux --config=test --config=with_ui)
COCKROACH=$CROSSBIN/pkg/cmd/cockroach/cockroach_/cockroach
COMPAREBIN=$(bazel run //pkg/compose/compare/compare:compare_test --config=ci --config=crosslinux --config=test --config=with_ui --run_under=realpath | grep '^/' | tail -n1)

$BAZCI run --config=ci --config=test --artifacts_dir=$PWD/artifacts \
       //pkg/compose:compose_test -- \
       --test_env=GO_TEST_WRAP_TESTV=1 \
       --test_arg -cockroach --test_arg $COCKROACH \
       --test_arg -compare --test_arg $COMPAREBIN \
       --test_timeout=1800

tc_end_block "Run compose tests"

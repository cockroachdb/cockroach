#!/usr/bin/env bash

set -xeuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"
source "$dir/teamcity-support.sh"

tc_start_block "Run compose tests"

bazel build //pkg/cmd/bazci --config=ci
BAZCI=$(bazel info bazel-bin --config=ci)/pkg/cmd/bazci/bazci_/bazci

$BAZCI run --config=crosslinux --config=test --config=with_ui --artifacts_dir=$PWD/artifacts \
       //pkg/compose:compose_test -- \
       --test_env=GO_TEST_WRAP_TESTV=1 \
       --test_timeout=1800

tc_end_block "Run compose tests"

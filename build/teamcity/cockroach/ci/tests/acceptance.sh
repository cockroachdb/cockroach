#!/usr/bin/env bash

set -xeuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname $(dirname "${0}")))))"
source "$dir/teamcity-support.sh"

export ARTIFACTSDIR=$PWD/artifacts/acceptance
mkdir -p "$ARTIFACTSDIR"

tc_start_block "Run acceptance tests"
status=0

bazel build //pkg/cmd/bazci --config=ci
BAZCI=$(bazel info bazel-bin --config=ci)/pkg/cmd/bazci/bazci_/bazci

$BAZCI run --config=crosslinux --config=test --artifacts_dir=$PWD/artifacts \
  //pkg/acceptance:acceptance_test -- \
  --test_arg=-l="$ARTIFACTSDIR" \
  --test_env=TZ=America/New_York \
  --test_env=GO_TEST_WRAP_TESTV=1 \
  --test_env=LANG \
  --test_timeout=1800 || status=$?

# Some unit tests test automatic ballast creation. These ballasts can be
# larger than the maximum artifact size. Remove any artifacts with the
# EMERGENCY_BALLAST filename.
find "$ARTIFACTSDIR" -name "EMERGENCY_BALLAST" -delete

tc_end_block "Run acceptance tests"
exit $status

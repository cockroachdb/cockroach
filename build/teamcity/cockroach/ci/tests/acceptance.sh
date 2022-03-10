#!/usr/bin/env bash

set -xeuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname $(dirname "${0}")))))"
source "$dir/teamcity-support.sh"
source "$dir/teamcity-bazel-support.sh"  # For run_bazel

tc_start_block "Build cockroach"
run_bazel /usr/bin/bash -c 'bazel build --config crosslinux --config ci //pkg/cmd/cockroach-short && cp $(bazel info bazel-bin --config crosslinux --config ci)/pkg/cmd/cockroach-short/cockroach-short_/cockroach-short /artifacts/cockroach && chmod a+w /artifacts/cockroach'
tc_end_block "Build cockroach"

export ARTIFACTSDIR=$PWD/artifacts/acceptance
mkdir -p "$ARTIFACTSDIR"

tc_start_block "Run acceptance tests"
status=0

bazel build //pkg/cmd/bazci --config=ci --remote_cache='https://storage.googleapis.com/test-build-cache-cockroachlabs' \
                                                   --cache_test_results=no
BAZCI=$(bazel info bazel-bin --config=ci)/pkg/cmd/bazci/bazci_/bazci

$BAZCI run --config=crosslinux --config=test --artifacts_dir=$PWD/artifacts \
  //pkg/acceptance:acceptance_test -- \
  --test_arg=-l="$ARTIFACTSDIR" \
  --test_arg=-b=$PWD/artifacts/cockroach \
  --test_env=TZ=America/New_York \
  --test_env=GO_TEST_WRAP_TESTV=1 \
  --test_timeout=1800 \
  --remote_cache='https://storage.googleapis.com/test-build-cache-cockroachlabs' \
  --cache_test_results=no || status=$?

# Some unit tests test automatic ballast creation. These ballasts can be
# larger than the maximum artifact size. Remove any artifacts with the
# EMERGENCY_BALLAST filename.
find "$ARTIFACTSDIR" -name "EMERGENCY_BALLAST" -delete

tc_end_block "Run acceptance tests"
exit $status

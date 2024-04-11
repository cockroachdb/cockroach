#!/usr/bin/env bash

set -xeuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname $(dirname "${0}")))))"
source "$dir/teamcity-support.sh"
source "$dir/teamcity-bazel-support.sh"  # For run_bazel

if [[ "$(uname -m)" =~ (arm64|aarch64)$ ]]; then
  export CROSSLINUX_CONFIG="crosslinuxarm"
else
  export CROSSLINUX_CONFIG="crosslinux"
fi

tc_start_block "Build cockroach"
build_script='bazel build --config $1 --config ci //pkg/cmd/cockroach-short && cp $(bazel info bazel-bin --config $1 --config ci)/pkg/cmd/cockroach-short/cockroach-short_/cockroach-short /artifacts/cockroach && chmod a+w /artifacts/cockroach'
run_bazel /usr/bin/bash -c "$build_script" -- "$CROSSLINUX_CONFIG"
tc_end_block "Build cockroach"

export ARTIFACTSDIR=$PWD/artifacts/acceptance
mkdir -p "$ARTIFACTSDIR"

tc_start_block "Run acceptance tests"
status=0

bazel build //pkg/cmd/bazci --config=ci
BAZCI=$(bazel info bazel-bin --config=ci)/pkg/cmd/bazci/bazci_/bazci

$BAZCI --artifacts_dir=$PWD/artifacts -- \
  test //pkg/acceptance:acceptance_test \
  --config=$CROSSLINUX_CONFIG --config=ci \
  "--sandbox_writable_path=$ARTIFACTSDIR" \
  "--test_tmpdir=$ARTIFACTSDIR" \
  --test_arg=-l="$ARTIFACTSDIR" \
  --test_arg=-b=$PWD/artifacts/cockroach \
  --test_env=TZ=America/New_York \
  --profile=$PWD/artifacts/profile.gz \
  --test_timeout=1800 || status=$?

# Some unit tests test automatic ballast creation. These ballasts can be
# larger than the maximum artifact size. Remove any artifacts with the
# EMERGENCY_BALLAST filename.
find "$ARTIFACTSDIR" -name "EMERGENCY_BALLAST" -delete

tc_end_block "Run acceptance tests"
exit $status

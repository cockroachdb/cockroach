#!/usr/bin/env bash

set -xeuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname $(dirname "${0}")))))"
source "$dir/teamcity-support.sh"

tc_prepare

export ARTIFACTSDIR=$PWD/artifacts/acceptance
mkdir -p "$ARTIFACTSDIR"
XML_OUTPUT_FILE=$PWD/artifacts/test.xml

tc_start_block "Run acceptance tests"
status=0
bazel run \
  //pkg/acceptance:acceptance_test \
  --config=crosslinux --config=test \
  --test_arg=-l="$ARTIFACTSDIR" \
  --test_env=TZ=America/New_York \
  "--test_env=XML_OUTPUT_FILE=$XML_OUTPUT_FILE" \
  --test_env=GO_TEST_WRAP_TESTV=1 \
  --test_env=GO_TEST_WRAP=1 \
  --test_timeout=1800 || status=$?

# Some unit tests test automatic ballast creation. These ballasts can be
# larger than the maximum artifact size. Remove any artifacts with the
# EMERGENCY_BALLAST filename.
find "$ARTIFACTSDIR" -name "EMERGENCY_BALLAST" -delete

# The schema of the output test.xml will be slightly wrong -- ask `bazci` to fix
# it up.
bazel build //pkg/cmd/bazci --config=ci
$(bazel info bazel-bin --config=ci)/pkg/cmd/bazci/bazci_/bazci munge-test-xml $XML_OUTPUT_FILE

tc_end_block "Run acceptance tests"
exit $status

#!/usr/bin/env bash

set -euo pipefail

dir="$(dirname $(dirname $(dirname $(dirname $(dirname "${0}")))))"

source "$dir/teamcity-support.sh"  # For $root
source "$dir/teamcity-bazel-support.sh"  # For run_bazel

tc_prepare
set -x

tc_start_block "Run acceptance"
run_bazel build/teamcity/cockroach/ci/tests/acceptance_impl.sh
# bazel build //pkg/cmd/bazci --config=ci
XML_OUTPUT_FILE=artifacts/test.xml GO_TEST_WRAP_TESTV=1 GO_TEST_WRAP=1 bazel \
	       run --config=ci --config=test //build/bazelutil:acceptance_test
# The schema of the output test.xml will be slightly wrong -- ask `bazci` to fix
# it up.
artifacts/bazci munge-test-xml artifacts/test.xml
tc_end_block "Run acceptance"

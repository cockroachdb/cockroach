#!/usr/bin/env bash

set -euo pipefail

source "$(dirname "${0}")/teamcity-support.sh"

tc_prepare

export TMPDIR=$PWD/artifacts/test
mkdir -p "$TMPDIR"

tc_start_block "Compile C dependencies"
# Buffer noisy output and only print it on failure.
run build/builder.sh make -Otarget c-deps &> artifacts/c-build.log || (cat artifacts/c-build.log && false)
rm artifacts/c-build.log
tc_end_block "Compile C dependencies"

tc_start_block "Maybe stress pull request"
run build/builder.sh go install ./pkg/cmd/github-pull-request-make
run build/builder.sh env BUILD_VCS_NUMBER="$BUILD_VCS_NUMBER" TARGET=stress github-pull-request-make
tc_end_block "Maybe stress pull request"

tc_start_block "Run Go tests"
run_json_test build/builder.sh stdbuf -oL -eL make test GOTESTFLAGS=-json TESTFLAGS='-v'
tc_end_block "Run Go tests"

tc_start_block "Run C++ tests"
# Buffer noisy output and only print it on failure.
run build/builder.sh make check-libroach &> artifacts/c-tests.log || (cat artifacts/c-tests.log && false)
tc_end_block "Run C++ tests"

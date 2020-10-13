#!/usr/bin/env bash

# This test configuration runs all unit tests with the metamorphic build
# tag enabled, which varies some constants in the code to try to tickle edge
# conditions.

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

tc_start_block "Run Go tests with metamorphic build tag"
run_json_test build/builder.sh stdbuf -oL -eL make test GOTESTFLAGS=-json TESTFLAGS='-v' TAGS=metamorphic
tc_end_block "Run Go tests with metamorphic build tag"

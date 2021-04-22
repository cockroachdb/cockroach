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

maybe_stress stress

tc_start_block "Run Go tests"
run_json_test build/builder.sh stdbuf -oL -eL make test GOTESTFLAGS=-json TESTFLAGS='-v'
tc_end_block "Run Go tests"

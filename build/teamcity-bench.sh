#!/usr/bin/env bash

set -euo pipefail

source "$(dirname "${0}")/teamcity-support.sh"

tc_prepare

export TMPDIR=$PWD/artifacts/bench
mkdir -p "$TMPDIR"

tc_start_block "Compile C dependencies"
# Buffer noisy output and only print it on failure.
run build/builder.sh make -Otarget c-deps &> artifacts/bench-c-build.log || (cat artifacts/bench-c-build.log && false)
rm artifacts/bench-c-build.log
tc_end_block "Compile C dependencies"

tc_start_block "Run Benchmarks"
run_json_test build/builder.sh stdbuf -oL -eL \
	make benchshort GOTESTFLAGS=-json TESTFLAGS='-v'
tc_end_block "Run Benchmarks"

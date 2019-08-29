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
run build/builder.sh \
	stdbuf -oL -eL \
	make benchshort TESTFLAGS='-v' 2>&1 \
	| tee artifacts/bench.log \
	| go-test-teamcity
tc_end_block "Run Benchmarks"

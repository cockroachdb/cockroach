#!/usr/bin/env bash

set -euo pipefail

source "$(dirname "${0}")/teamcity-support.sh"

tc_prepare

export TMPDIR=$PWD/artifacts/bench
mkdir -p "$TMPDIR"

tc_start_block "Compile C dependencies"
run build/builder.sh make -Otarget c-deps
tc_end_block "Compile C dependencies"

tc_start_block "Run Benchmarks"
run build/builder.sh \
	stdbuf -oL -eL \
	make benchshort TESTFLAGS='-v' 2>&1 \
	| tee artifacts/bench.log \
	| go-test-teamcity
tc_end_block "Run Benchmarks"

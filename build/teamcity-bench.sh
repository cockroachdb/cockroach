#!/usr/bin/env bash

set -euo pipefail

source "$(dirname "${0}")/teamcity-support.sh"

tc_prepare

export TMPDIR=$PWD/artifacts/bench
mkdir -p "$TMPDIR"

tc_start_block "Compile C dependencies"
run script -t5 artifacts/bench-build.log \
	build/builder.sh \
	make -Otarget c-deps
tc_end_block "Compile C dependencies"

tc_start_block "Run Benchmarks"
run script -t5 artifacts/bench.log \
	build/builder.sh \
	make benchshort TESTFLAGS='-v' \
	| go-test-teamcity
tc_end_block "Run Benchmarks"

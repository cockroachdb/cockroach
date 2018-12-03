#!/usr/bin/env bash

set -euo pipefail

source "$(dirname "${0}")/teamcity-support.sh"

tc_prepare

export TMPDIR=$PWD/artifacts/test
mkdir -p "$TMPDIR"

tc_start_block "Compile C dependencies"
run script -t5 artifacts/test-deps.log \
	build/builder.sh \
	make -Otarget c-deps
tc_end_block "Compile C dependencies"

tc_start_block "Run TestLogic tests under verbose"
run script -t5 artifacts/test.log \
	build/builder.sh \
	make testlogic TESTTIMEOUT=1h TESTFLAGS='--vmodule=*=10 -show-sql -test.v' \
	| go-test-teamcity
tc_end_block "Run TestLogic tests under verbose"

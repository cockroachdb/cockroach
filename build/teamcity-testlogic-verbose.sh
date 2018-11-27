#!/usr/bin/env bash

set -euo pipefail

source "$(dirname "${0}")/teamcity-support.sh"

tc_prepare

export TMPDIR=$PWD/artifacts/test
mkdir -p "$TMPDIR"

tc_start_block "Compile C dependencies"
run build/builder.sh make -Otarget c-deps
tc_end_block "Compile C dependencies"

tc_start_block "Run TestLogic tests under verbose"
run build/builder.sh env TZ=America/New_York make testlogic TESTTIMEOUT=1h TESTFLAGS='--vmodule=*=10 -show-sql -test.v' 2>&1 \
	| tee artifacts/test.log \
	| go-test-teamcity
tc_end_block "Run TestLogic tests under verbose"

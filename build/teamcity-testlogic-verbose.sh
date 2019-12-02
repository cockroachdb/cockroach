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
run_json_test build/builder.sh \
  stdbuf -oL -eL \
  make testlogic GOTESTFLAGS=-json TESTTIMEOUT=1h TESTFLAGS='--vmodule=*=10 -show-sql -test.v'
tc_end_block "Run TestLogic tests under verbose"

#!/usr/bin/env bash

set -euo pipefail

source "$(dirname "${0}")/teamcity-support.sh"

tc_prepare
definitely_ccache

export TMPDIR=$PWD/artifacts
mkdir -p "$TMPDIR"

tc_start_block "Compile C dependencies"
run build/builder.sh make -Otarget c-deps
tc_end_block "Compile C dependencies"

tc_start_block "Run Go tests with deadlock detection enabled"
run_json_test build/builder.sh \
  stdbuf -oL -eL \
  make test GOTESTFLAGS=-json TAGS=deadlock TESTFLAGS='-v'
tc_end_block "Run Go tests with deadlock detection enabled"

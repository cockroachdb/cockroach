#!/usr/bin/env bash

set -euo pipefail

source "$(dirname "${0}")/teamcity-support.sh"

tc_prepare

export TMPDIR=$PWD/artifacts/test
mkdir -p "$TMPDIR"


tc_start_block "Run Random Syntax tests"
run_json_test build/builder.sh stdbuf -oL -eL make test \
  PKG=./pkg/sql/tests \
  TESTS=TestRandomSyntax \
  GOTESTFLAGS=-json \
  TESTFLAGS='-v -rsg=5m -rsg-routines=8 -rsg-exec-timeout=1m' \
  TESTTIMEOUT=1h
tc_end_block "Run Random Syntax tests"

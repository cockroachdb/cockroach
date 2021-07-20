#!/usr/bin/env bash
#
# This file contains tests specific to the opt (optimizer) package.

set -euxo pipefail

source "$(dirname "${0}")/teamcity-support.sh"

tc_prepare

export TMPDIR=$PWD/artifacts/opttest
mkdir -p "$TMPDIR"

tc_start_block "Compile C dependencies"
# Buffer noisy output and only print it on failure.
run build/builder.sh make -Otarget c-deps GOFLAGS=-race &> artifacts/race-c-build.log || (cat artifacts/race-c-build.log && false)
rm artifacts/race-c-build.log
tc_end_block "Compile C dependencies"

# Run with the fast_int_set_large tag.
tc_start_block "Run opt tests with fast_int_set_large"
run_json_test build/builder.sh \
  stdbuf -oL -eL \
  make test \
  GOTESTFLAGS=-json \
  TAGS=fast_int_set_large \
  PKG=./pkg/sql/opt... \
  TESTFLAGS='-v'
tc_end_block "Run opt tests with fast_int_set_large"

# Run with the fast_int_set_small tag.
tc_start_block "Run opt tests with fast_int_set_small"
run_json_test build/builder.sh \
  stdbuf -oL -eL \
  make test \
  GOTESTFLAGS=-json \
  TAGS=fast_int_set_small \
  PKG=./pkg/sql/opt... \
  TESTFLAGS='-v'
tc_end_block "Run opt tests with fast_int_set_small"

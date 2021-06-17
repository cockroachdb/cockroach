#!/usr/bin/env bash

set -euo pipefail

source "$(dirname "${0}")/teamcity-support.sh"

tc_prepare

export TMPDIR=$PWD/artifacts/testrace
mkdir -p "$TMPDIR"

tc_start_block "Determine changed packages"
pkgspec=./pkg/...
tc_end_block "Determine changed packages"

tc_start_block "Compile C dependencies"
# Buffer noisy output and only print it on failure.
run make -Otarget c-deps GOFLAGS=-race &> artifacts/race-c-build.log || (cat artifacts/race-c-build.log && false)
rm artifacts/race-c-build.log
tc_end_block "Compile C dependencies"

for pkg in $pkgspec; do
  tc_start_block "Run ${pkg} under race detector"
  run_json_test env \
    COCKROACH_LOGIC_TESTS_SKIP=true \
    GOMAXPROCS=8 \
    stdbuf -oL -eL \
    make testrace \
    GOTESTFLAGS=-json \
    PKG="$pkg" \
    TESTFLAGS="-v $TESTFLAGS" \
    ENABLE_LIBROACH_ASSERTIONS=1
  tc_end_block "Run ${pkg} under race detector"
done

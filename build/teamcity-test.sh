#!/usr/bin/env bash

set -euo pipefail

source "$(dirname "${0}")/teamcity-support.sh"

tc_prepare

export TMPDIR=$PWD/artifacts/test
mkdir -p "$TMPDIR"

tc_start_block "Compile C dependencies"
# Buffer noisy output and only print it on failure.
run build/builder.sh make -Otarget c-deps &> artifacts/c-build.log || (cat artifacts/c-build.log && false)
rm artifacts/c-build.log
tc_end_block "Compile C dependencies"

maybe_stress stress

tc_start_block "Run Go tests"
run build/builder.sh \
	stdbuf -oL -eL \
	make test TESTFLAGS='-v' 2>&1 \
	| tee artifacts/test.log \
	| go-test-teamcity
tc_end_block "Run Go tests"

echo "Slow test packages:"
grep "ok  " artifacts/test.log | sort -n -k3 | tail -n25

echo "Slow individual tests:"
grep "^--- PASS" artifacts/test.log | sed 's/(//; s/)//' | sort -n -k4 | tail -n25

tc_start_block "Run C++ tests"
# Buffer noisy output and only print it on failure.
run build/builder.sh make check-libroach &> artifacts/c-tests.log || (cat artifacts/c-tests.log && false)
tc_end_block "Run C++ tests"

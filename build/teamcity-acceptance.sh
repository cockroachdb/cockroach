#!/usr/bin/env bash

set -euo pipefail

source "$(dirname "${0}")/teamcity-support.sh"

tc_prepare

tc_start_block "Prepare environment for acceptance tests"
# The log files that should be created by -l below can only
# be created if the parent directory already exists. Ensure
# that it exists before running the test.
export TMPDIR=$PWD/artifacts/acceptance
mkdir -p "$TMPDIR"
type=$(go env GOOS)
tc_end_block "Prepare environment for acceptance tests"

tc_start_block "Compile CockroachDB"
# Buffer noisy output and only print it on failure.
run pkg/acceptance/prepare.sh &> artifacts/acceptance-compile.log || (cat artifacts/acceptance-compile.log && false)
rm artifacts/acceptance-compile.log
run ln -s cockroach-linux-2.6.32-gnu-amd64 cockroach  # For the tests that run without Docker.
tc_end_block "Compile CockroachDB"

tc_start_block "Compile acceptance tests"
run build/builder.sh mkrelease "$type" -Otarget testbuild TAGS=acceptance PKG=./pkg/acceptance
tc_end_block "Compile acceptance tests"

tc_start_block "Run acceptance tests"
run cd pkg/acceptance
run env TZ=America/New_York \
	stdbuf -eL -oL \
	./acceptance.test -l "$TMPDIR" -test.v -test.timeout 30m 2>&1 \
	| tee "$TMPDIR/acceptance.log" \
	| go-test-teamcity
run cd ../..
tc_end_block "Run acceptance tests"

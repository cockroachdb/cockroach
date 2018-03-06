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
type=release-$(go env GOOS)
if [[ "$type" = *-linux ]]; then
  type+=-gnu
fi
tc_end_block "Prepare environment for acceptance tests"

tc_start_block "Compile CockroachDB"
run pkg/acceptance/prepare.sh
run ln -s cockroach-linux-2.6.32-gnu-amd64 cockroach  # For the tests that run without Docker.
tc_end_block "Compile CockroachDB"

tc_start_block "Compile acceptance tests"
run build/builder.sh make -Otarget TYPE="$type" testbuild TAGS=acceptance PKG=./pkg/acceptance
tc_end_block "Compile acceptance tests"

tc_start_block "Run acceptance tests"
run cd pkg/acceptance
run ./acceptance.test -nodes 4 -l "$TMPDIR" -test.v -test.timeout 30m 2>&1 | tee "$TMPDIR/acceptance.log" | go-test-teamcity
tc_end_block "Run acceptance tests"

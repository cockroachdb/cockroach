#!/usr/bin/env bash
set -euxo pipefail

# Ensure that no stale binary remains.
rm -f pkg/acceptance/acceptance.test

export BUILDER_HIDE_GOPATH_SRC=1
build/builder.sh make build TYPE=release
build/builder.sh make install TYPE=release
build/builder.sh make testbuild TAGS=acceptance PKG=./pkg/acceptance TYPE=release

# The log files that should be created by -l below can only
# be created if the parent directory already exists. Ensure
# that it exists before running the test.
mkdir -p artifacts/acceptance
export TMPDIR=$PWD/artifacts/acceptance

cd pkg/acceptance
./acceptance.test -nodes 3 -l "$TMPDIR" -test.v -test.timeout 10m 2>&1 | tee "$TMPDIR/acceptance.log" | go-test-teamcity

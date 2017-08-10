#!/usr/bin/env bash

set -euxo pipefail

export BUILDER_HIDE_GOPATH_SRC=1

"$(dirname "${0}")"/../pkg/acceptance/prepare.sh

# The log files that should be created by -l below can only
# be created if the parent directory already exists. Ensure
# that it exists before running the test.
mkdir -p artifacts/acceptance
export TMPDIR=$PWD/artifacts/acceptance

TYPE=release-$(go env GOOS)
case $TYPE in
  *-linux)
    TYPE+=-gnu
    ;;
esac

build/builder.sh make TYPE=$TYPE testbuild TAGS=acceptance PKG=./pkg/acceptance
cd pkg/acceptance
./acceptance.test -nodes 4 -l "$TMPDIR" -test.v -test.timeout 10m 2>&1 | tee "$TMPDIR/acceptance.log" | go-test-teamcity

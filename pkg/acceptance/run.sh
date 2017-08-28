#!/usr/bin/env bash

set -euxo pipefail

"$(dirname "${0}")"/prepare.sh

# The log files that should be created by -l below can only
# be created if the parent directory already exists. Ensure
# that it exists before running the test.
mkdir -p artifacts/acceptance
export TMPDIR=$PWD/artifacts/acceptance

# Build the standard binary to ./cockroach for those tests that don't use
# Docker.
echo "Building local binary"
make build
make test PKG=./pkg/acceptance TESTTIMEOUT="${TESTTIMEOUT-30m}" TAGS=acceptance TESTFLAGS="${TESTFLAGS--v -nodes 4} -l $TMPDIR"

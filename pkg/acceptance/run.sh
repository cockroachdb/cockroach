#!/usr/bin/env bash

set -euxo pipefail

"$(dirname "${0}")"/prepare.sh

# The log files that should be created by -l below can only
# be created if the parent directory already exists. Ensure
# that it exists before running the test.
mkdir -p artifacts/acceptance
export TMPDIR=$PWD/artifacts/acceptance

# For the acceptance tests that run without Docker.
make build
make test PKG=./pkg/acceptance TESTTIMEOUT="${TESTTIMEOUT-30m}" TAGS=acceptance TESTFLAGS="${TESTFLAGS--v} -l $TMPDIR"

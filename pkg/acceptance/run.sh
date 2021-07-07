#!/usr/bin/env bash

set -euxo pipefail

"$(dirname "${0}")"/prepare.sh

# The log files that should be created by -l below can only
# be created if the parent directory already exists. Ensure
# that it exists before running the test.
mkdir -p artifacts/acceptance/foo
export TMPDIR=$PWD/artifacts/acceptance

# For the acceptance tests that run without Docker.
make build
make test PKG=./pkg/acceptance TESTTIMEOUT="${TESTTIMEOUT-30m}" TAGS=acceptance TESTFLAGS="${TESTFLAGS--v} -l $TMPDIR"
echo '<html><body><script>console.log("insert script here in the future")</script></body></html>' > artifacts/acceptance/test.html
mkdir -p artifacts/testing

#!/usr/bin/env bash

set -xeuo pipefail

source "$(dirname "${0}")"/../../build/init-docker.sh
"$(dirname "${0}")"/../../build/builder.sh make install TAGS=clockoffset

# The log files that should be created by -l below can only
# be created if the parent directory already exists. Ensure
# that it exists before running the test.
mkdir -p artifacts/acceptance
export TMPDIR=$PWD/artifacts/acceptance

go test -tags acceptance ./pkg/acceptance ${GOFLAGS-} -run "${TESTS-.}" -timeout ${TESTTIMEOUT-10m} ${TESTFLAGS--v -nodes 3} -l "$TMPDIR"

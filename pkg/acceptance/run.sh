#!/usr/bin/env bash

set -euxo pipefail

# We must make a release build here because the binary needs to work
# in both the builder image and the postgres-test image, which have
# different libstc++ versions.
"$(dirname "${0}")"/../../build/builder.sh make install TAGS=clockoffset TYPE=release-linux-gnu

# The log files that should be created by -l below can only
# be created if the parent directory already exists. Ensure
# that it exists before running the test.
mkdir -p artifacts/acceptance
export TMPDIR=$PWD/artifacts/acceptance

make test PKG=./pkg/acceptance TAGS=acceptance TESTFLAGS="${TESTFLAGS--v -nodes 3} -l $TMPDIR"

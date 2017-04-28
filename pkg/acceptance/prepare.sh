#!/usr/bin/env bash

set -euxo pipefail

# Ensure that no stale binary remains.
rm -f pkg/acceptance/acceptance.test

# We must make a release build here because the binary needs to work in both
# the builder image and the postgres-test image, which have different libstc++
# versions.
build/builder.sh make build TAGS=clockoffset TYPE=release-linux-gnu

# The log files that should be created by the caller can only be created if
# the parent directory already exists. Ensure that it exists before running
# the test.
mkdir -p artifacts/acceptance
export TMPDIR=$PWD/artifacts/acceptance

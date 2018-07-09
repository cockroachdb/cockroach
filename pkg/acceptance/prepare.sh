#!/usr/bin/env bash

set -euxo pipefail

# Ensure that no stale binary remains.
rm -f cockroach-linux-2.6.32-gnu-amd64 pkg/acceptance/acceptance.test

# We must make a release build here because the binary needs to work in both
# the builder image and the postgres-test image, which have different libstc++
# versions.
build/builder.sh mkrelease linux-gnu

#!/usr/bin/env bash

set -euxo pipefail

# Ensure that no stale binary remains.
rm -f cockroach-linux-2.6.32-gnu-amd64 pkg/compose/compose.test pkg/compose/compare/compare/compare.test

# Build the release and test binaries so they can be used during docker compose execution.
build/builder.sh mkrelease linux-gnu
build/builder.sh mkrelease linux-gnu -Otarget testbuild PKG=./pkg/compose/compare/compare TAGS=compose

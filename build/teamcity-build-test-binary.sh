#!/usr/bin/env bash

# This file builds a cockroach binary that's used by integration tests external
# to this repository.

set -euxo pipefail

# We don't want to build a proper release binary here, because we don't want
# this binary to operate in release mode and e.g. report errors.
build/builder.sh make build TYPE=portable
mkdir -p artifacts
mv cockroach artifacts/

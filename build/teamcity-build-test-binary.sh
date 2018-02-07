#!/usr/bin/env bash

# This file builds a cockroach binary that's used by integration tests external
# to this repository.

set -euo pipefail

source "$(dirname "${0}")/teamcity-support.sh"

tc_prepare

tc_start_block "Build test binary"
# We don't want to build a proper release binary here, because we don't want
# this binary to operate in release mode and e.g. report errors.
run build/builder.sh make build -Otarget TYPE=portable
run mv cockroach artifacts/
tc_end_block "Build test binary"

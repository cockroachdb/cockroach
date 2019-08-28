#!/usr/bin/env bash

# This file builds a cockroach binary that's used by integration tests external
# to this repository.

set -euo pipefail

source "$(dirname "${0}")/teamcity-support.sh"

tc_prepare

tc_start_block "Build test binary"
# Buffer noisy output and only print it on failure.
run build/builder.sh mkrelease linux-gnu -Otarget &> artifacts/build-binary.log || (cat artifacts/build-binary.log && false)
rm artifacts/build-binary.log
run mv cockroach-linux-2.6.32-gnu-amd64 artifacts/cockroach
tc_end_block "Build test binary"

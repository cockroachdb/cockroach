#!/usr/bin/env bash

# This file builds a cockroach binary that's used by integration tests external
# to this repository.

set -euo pipefail

source "$(dirname "${0}")/teamcity-support.sh"

tc_prepare

tc_start_block "Build test binary"
run build/builder.sh make build -Otarget TYPE=release-linux-gnu
run mv cockroach-linux-2.6.32-gnu-amd64 artifacts/cockroach
tc_end_block "Build test binary"

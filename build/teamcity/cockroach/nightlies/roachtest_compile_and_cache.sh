#!/usr/bin/env bash

# Copyright 2026 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

# Standalone entry point for compiling all roachtest artifacts and populating
# the GCS build cache. Intended to run as its own TeamCity build so that
# downstream cloud-specific roachtest jobs (GCE, AWS, Azure) can skip
# compilation by downloading cached artifacts for the same SHA.

set -exuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"

set -a
source "$dir/teamcity-support.sh"
set +a

export ROACHTEST_BUILD_CACHE=true

$root/build/teamcity/cockroach/nightlies/roachtest_compile_bits.sh amd64 arm64 amd64-fips

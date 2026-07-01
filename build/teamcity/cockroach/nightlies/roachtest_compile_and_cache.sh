#!/usr/bin/env bash

# Copyright 2026 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

# Standalone entry point for compiling roachtest artifacts and populating the
# GCS build cache. Intended to run as its own TeamCity build so that downstream
# cloud-specific roachtest jobs (GCE, AWS, Azure) can skip compilation by
# downloading cached artifacts for the same SHA.
#
# Usage: roachtest_compile_and_cache.sh [arch...]
#   where arch is one of: amd64, arm64, amd64-fips, s390x.
# With no arguments, the default set (amd64, arm64, amd64-fips) is built. Each
# arch is cached in GCS independently, so callers may split the work across
# parallel builds (one arch each) to shorten the critical path before nightly
# tests can start.

set -exuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"

set -a
source "$dir/teamcity-support.sh"
set +a

export ROACHTEST_BUILD_CACHE=true

# Compile the requested architectures, defaulting to all of them. Passed
# through from roachtest_compile.sh; roachtest_compile_bits.sh validates each
# arch and fails on an unknown one.
arches=("$@")
if [[ ${#arches[@]} -eq 0 ]]; then
  arches=(amd64 arm64 amd64-fips)
fi

$root/build/teamcity/cockroach/nightlies/roachtest_compile_bits.sh "${arches[@]}"

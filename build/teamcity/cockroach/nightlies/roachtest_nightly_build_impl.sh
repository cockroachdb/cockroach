#!/usr/bin/env bash

# Copyright 2024 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -exuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"

# N.B. export variables like `root` s.t. they can be used by scripts called below.
set -a
source "$dir/teamcity-support.sh"
set +a

$root/build/teamcity/cockroach/nightlies/roachtest_compile_bits.sh ${WITH_COVERAGE:+--with-code-coverage} amd64
$root/build/teamcity/cockroach/nightlies/roachtest_compile_bits.sh ${WITH_COVERAGE:+--with-code-coverage} amd64-fips 
$root/build/teamcity/cockroach/nightlies/roachtest_compile_bits.sh ${WITH_COVERAGE:+--with-code-coverage} arm64

artifacts=/artifacts

mkdir artifacts/bin
mkdir artifacts/lib
mv bin/* artifacts/bin
mv lib/* artifacts/lib

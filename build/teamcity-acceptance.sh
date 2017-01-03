#!/usr/bin/env bash
set -euxo pipefail

build/builder.sh make build
build/builder.sh make install
build/builder.sh go test -v -c -tags acceptance ./pkg/acceptance

# The log files that should be created by -l below can only
# be created if the parent directory already exists. Ensure
# that it exists before running the test.
mkdir -p artifacts/acceptance

cd pkg/acceptance
../../acceptance.test -nodes 3 -l ../../artifacts/acceptance -test.v -test.timeout 10m 2>&1 | go-test-teamcity

#!/usr/bin/env bash
set -euxo pipefail


build/builder.sh make build
build/builder.sh make install
build/builder.sh go test -v -c -tags acceptance ./pkg/acceptance
cd pkg/acceptance
COCKROACH_SKIP_FLAKY_TESTS=true ../../acceptance.test -nodes 3 -l ../../artifacts/acceptance -test.v -test.timeout 10m 2>&1 | go-test-teamcity

#!/usr/bin/env bash
set -euxo pipefail

build/builder.sh make testrace TESTFLAGS='-v' COCKROACH_SKIP_FLAKY_TESTS=true 2>&1 | go-test-teamcity

#!/usr/bin/env bash
set -euxo pipefail

export COCKROACH_SKIP_FLAKY_TESTS=true

build/builder.sh make test TESTFLAGS='-v' 2>&1 | go-test-teamcity

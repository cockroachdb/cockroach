#!/usr/bin/env bash
set -euxo pipefail
build/builder.sh make test COCKROACH_PROPOSER_EVALUATED_KV="${COCKROACH_PROPOSER_EVALUATED_KV-}" TESTFLAGS='-v' 2>&1 | go-test-teamcity

build/builder.sh env BUILD_VCS_NUMBER="$BUILD_VCS_NUMBER" TARGET=stress github-pull-request-make

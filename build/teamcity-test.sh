#!/usr/bin/env bash
set -euxo pipefail
build/builder.sh make test TESTFLAGS='-v' 2>&1 | go-test-teamcity

build/builder.sh env BUILD_VCS_NUMBER="$BUILD_VCS_NUMBER" TARGET=stress github-pull-request-make

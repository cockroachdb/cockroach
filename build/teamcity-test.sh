#!/usr/bin/env bash
set -euxo pipefail
build/builder.sh make test TESTFLAGS='-v' 2>&1 | go-test-teamcity

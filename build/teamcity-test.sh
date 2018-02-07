#!/usr/bin/env bash

set -euo pipefail

source "$(dirname "${0}")/teamcity-support.sh"

tc_prepare

tc_start_block "Maybe stress pull request"
run build/builder.sh go install ./pkg/cmd/github-pull-request-make
run build/builder.sh env BUILD_VCS_NUMBER="$BUILD_VCS_NUMBER" TARGET=stress github-pull-request-make
tc_end_block "Maybe stress pull request"

tc_start_block "Compile"
run build/builder.sh make -Otarget gotestdashi
tc_end_block "Compile"

tc_start_block "Run Go tests"
run build/builder.sh env TZ=America/New_York make test TESTFLAGS='-v' 2>&1 \
	| tee artifacts/test.log \
	| go-test-teamcity
tc_end_block "Run Go tests"

tc_start_block "Run C++ tests"
run build/builder.sh make check-libroach
tc_end_block "Run C++ tests"

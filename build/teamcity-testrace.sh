#!/usr/bin/env bash
set -euxo pipefail

export BUILDER_HIDE_GOPATH_SRC=1

mkdir -p artifacts

build/builder.sh go install ./pkg/cmd/github-pull-request-make

build/builder.sh env \
	BUILD_VCS_NUMBER="$BUILD_VCS_NUMBER" \
	TARGET=stressrace \
	github-pull-request-make

# Due to a limit on the number of running goroutines in the race
# detector, testrace fails when run on 16-CPU machines. Set GOMAXPROCS
# to work around this.
build/builder.sh env \
        GOMAXPROCS=8 \
	make testrace \
	TESTFLAGS='-v' \
	2>&1 \
	| tee artifacts/testrace.log \
	| go-test-teamcity

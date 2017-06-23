#!/usr/bin/env bash
set -euxo pipefail

export BUILDER_HIDE_GOPATH_SRC=1

mkdir -p artifacts

build/builder.sh go install ./pkg/cmd/github-pull-request-make

build/builder.sh env \
	BUILD_VCS_NUMBER="$BUILD_VCS_NUMBER" \
	TARGET=stressrace \
	github-pull-request-make

build/builder.sh env \
    make testrace PKG=./pkg/sql/logictest TESTS=TestLogic/5node/distsql_agg TESTFLAGS='-count 100 -v -show-logs' \
	2>&1 \
	| tee artifacts/testrace.log \
	| go-test-teamcity

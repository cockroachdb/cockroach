#!/usr/bin/env bash
set -euxo pipefail

export BUILDER_HIDE_GOPATH_SRC=1

mkdir -p artifacts

build/builder.sh go install ./pkg/cmd/github-pull-request-make

build/builder.sh env \
	BUILD_VCS_NUMBER="$BUILD_VCS_NUMBER" \
	TARGET=stress \
	github-pull-request-make

build/builder.sh bash -c 'env TZ=America/New_York make test TESTFLAGS=-v &> artifacts/test.log'
go-test-teamcity <artifacts/test.log

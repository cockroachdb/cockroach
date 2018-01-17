#!/usr/bin/env bash
set -euxo pipefail

export BUILDER_HIDE_GOPATH_SRC=1

source "$(dirname "${0}")/teamcity-support.sh"
maybe_ccache

mkdir -p artifacts

build/builder.sh go install ./pkg/cmd/github-pull-request-make

build/builder.sh env \
	BUILD_VCS_NUMBER="$BUILD_VCS_NUMBER" \
	TARGET=stress \
	github-pull-request-make

build/builder.sh env \
	TZ=America/New_York \
	make test \
	TESTFLAGS='-v' \
	2>&1 \
	| tee artifacts/test.log \
	| go-test-teamcity

build/builder.sh make check-libroach

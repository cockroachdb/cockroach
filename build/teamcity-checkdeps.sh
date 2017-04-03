#!/usr/bin/env bash
set -euxo pipefail

mkdir -p artifacts

export BUILDER_HIDE_GOPATH_SRC=1

build/builder.sh go install ./vendor/github.com/Masterminds/glide ./pkg/cmd/github-pull-request-make

build/builder.sh env \
	BUILD_VCS_NUMBER="$BUILD_VCS_NUMBER" \
	TARGET=checkdeps \
	github-pull-request-make

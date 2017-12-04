#!/usr/bin/env bash

set -euxo pipefail

export GCEWORKER_NAME=gceworker-teamcity-$(date +%Y%m%d%H%M%S)

build/builder.sh go install ./pkg/cmd/github-pull-request-make
build/builder.sh env \
	BUILD_VCS_NUMBER="$BUILD_VCS_NUMBER" \
	TARGET=test-gceworker \
	github-pull-request-make

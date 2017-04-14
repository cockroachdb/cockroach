#!/usr/bin/env bash
set -euxo pipefail

mkdir -p artifacts

export BUILDER_HIDE_GOPATH_SRC=1

build/builder.sh go install ./pkg/cmd/github-pull-request-make

build/builder.sh env \
	COCKROACH_PROPOSER_EVALUATED_KV="${COCKROACH_PROPOSER_EVALUATED_KV:-false}" \
	BUILD_VCS_NUMBER="$BUILD_VCS_NUMBER" \
	TARGET=stressrace \
	github-pull-request-make

build/builder.sh env \
	COCKROACH_PROPOSER_EVALUATED_KV="${COCKROACH_PROPOSER_EVALUATED_KV:-false}" \
	make testrace \
	TESTFLAGS='-v -show-logs' \
	2>&1 \
	| tee artifacts/testrace.log \
	| go-test-teamcity

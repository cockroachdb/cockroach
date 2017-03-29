#!/usr/bin/env bash
set -euxo pipefail

mkdir -p artifacts

export BUILDER_HIDE_UNVENDORED=1
build/builder.sh env \
		 COCKROACH_PROPOSER_EVALUATED_KV="${COCKROACH_PROPOSER_EVALUATED_KV:-false}" \
		 make testrace \
		 TESTFLAGS='-v' \
		 2>&1 \
    | tee artifacts/testrace.log \
    | go-test-teamcity

build/builder.sh env \
		 BUILD_VCS_NUMBER="$BUILD_VCS_NUMBER" \
		 TARGET=stressrace \
		 github-pull-request-make

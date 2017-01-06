#!/usr/bin/env bash
set -euxo pipefail

mkdir -p artifacts

build/builder.sh env \
		 COCKROACH_PROPOSER_EVALUATED_KV="${COCKROACH_PROPOSER_EVALUATED_KV:-false}" \
		 make test \
		 TESTFLAGS='-v' \
		 2>&1 \
    | tee artifacts/test.log \
    | go-test-teamcity

build/builder.sh env \
		 BUILD_VCS_NUMBER="$BUILD_VCS_NUMBER" \
		 TARGET=stress \
		 github-pull-request-make

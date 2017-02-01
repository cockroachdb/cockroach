#!/usr/bin/env bash
set -euxo pipefail

build_dir="$(dirname $0)"

mkdir -p artifacts

"${build_dir}"/builder.sh env \
		 COCKROACH_PROPOSER_EVALUATED_KV="${COCKROACH_PROPOSER_EVALUATED_KV:-false}" \
		 make test \
		 TESTFLAGS='-v' \
		 2>&1 \
    | tee artifacts/test.log \
    | go-test-teamcity

"${build_dir}"/builder.sh env \
		 BUILD_VCS_NUMBER="$BUILD_VCS_NUMBER" \
		 TARGET=stress \
		 github-pull-request-make

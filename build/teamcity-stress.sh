#!/usr/bin/env bash
set -euxo pipefail

mkdir -p artifacts

exit_status=0
build/builder.sh make stress COCKROACH_NIGHTLY_STRESS="${COCKROACH_NIGHTLY_STRESS-}" COCKROACH_PROPOSER_EVALUATED_KV="${COCKROACH_PROPOSER_EVALUATED_KV-}" PKG="$PKG" GOFLAGS="${GOFLAGS-}" TAGS="${TAGS-}" TESTTIMEOUT=30m TESTFLAGS='-test.v' STRESSFLAGS='-maxtime 15m -maxfails 1 -stderr' 2>&1 | tee artifacts/stress.log || exit_status=$?

if [ $exit_status -ne 0 ]; then
  build/builder.sh env GITHUB_API_TOKEN="$GITHUB_API_TOKEN" BUILD_VCS_NUMBER="$BUILD_VCS_NUMBER" TC_BUILD_ID="$TC_BUILD_ID" TC_SERVER_URL="$TC_SERVER_URL" COCKROACH_PROPOSER_EVALUATED_KV="${COCKROACH_PROPOSER_EVALUATED_KV-}" PKG="$PKG" GOFLAGS="${GOFLAGS-}" TAGS="${TAGS-}" github-post < artifacts/stress.log
fi;

exit $exit_status

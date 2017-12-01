#!/usr/bin/env bash
set -euxo pipefail

export BUILDER_HIDE_GOPATH_SRC=1

source "$(dirname "${0}")/teamcity-support.sh"
maybe_ccache

mkdir -p artifacts

exit_status=0

build/builder.sh go install ./pkg/cmd/github-post

build/builder.sh env COCKROACH_NIGHTLY_STRESS=true \
		 make stress \
		 PKG="$PKG" GOFLAGS="${GOFLAGS:-}" TAGS="${TAGS:-}" \
		 TESTTIMEOUT=30m TESTFLAGS='-test.v' \
		 STRESSFLAGS='-maxtime 15m -maxfails 1 -stderr' \
		 2>&1 \
    | tee artifacts/stress.log \
    || exit_status=$?

if [ $exit_status -ne 0 ]; then
    build/builder.sh env \
		     GITHUB_API_TOKEN="$GITHUB_API_TOKEN" \
		     BUILD_VCS_NUMBER="$BUILD_VCS_NUMBER" \
		     TC_BUILD_ID="$TC_BUILD_ID" \
		     TC_SERVER_URL="$TC_SERVER_URL" \
		     PKG="$PKG" GOFLAGS="${GOFLAGS:-}" TAGS="${TAGS:-}" \
		     github-post < artifacts/stress.log
fi;

exit $exit_status

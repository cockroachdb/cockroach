#!/usr/bin/env bash
set -euxo pipefail

export BUILDER_HIDE_GOPATH_SRC=1

source "$(dirname "${0}")/teamcity-support.sh"
definitely_ccache

mkdir -p artifacts

exit_status=0

go install ./pkg/cmd/github-post

build/builder.sh env COCKROACH_NIGHTLY_STRESS=true \
		 make stress \
		 PKG="$PKG" GOFLAGS="${GOFLAGS:-}" TAGS="${TAGS:-}" \
		 TESTTIMEOUT=30m \
		 STRESSFLAGS='-maxruns 100 -maxfails 1 -stderr' \
		 2>&1 \
	| tee artifacts/stress.log \
	|| exit_status=$?

if [ $exit_status -ne 0 ]; then
	build/builder.sh go tool test2json < artifacts/stress.log | github-post
fi;

exit $exit_status

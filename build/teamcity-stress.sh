#!/usr/bin/env bash
set -euxo pipefail

mkdir artifacts

exit_status=0
build/builder.sh make stress PKG="$PKG" GOFLAGS="${GOFLAGS-}" TAGS="${TAGS-}" TESTTIMEOUT=0 TESTFLAGS='-test.v' STRESSFLAGS='-maxtime 15m -maxfails 1 -stderr' 2>&1 | tee artifacts/stress.log || exit_status=$?

if [ $exit_status -ne 0 ]; then
  build/builder.sh env GITHUB_API_TOKEN="$GITHUB_API_TOKEN" BUILD_VCS_NUMBER="$BUILD_VCS_NUMBER" github-post < artifacts/stress.log
fi;

exit $exit_status

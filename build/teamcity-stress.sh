#!/usr/bin/env bash

set -euo pipefail

source "$(dirname "${0}")/teamcity-support.sh"

run export BUILDER_HIDE_GOPATH_SRC=1
run mkdir -p artifacts
definitely_ccache

env=(
  "GITHUB_API_TOKEN=$GITHUB_API_TOKEN"
  "BUILD_VCS_NUMBER=$BUILD_VCS_NUMBER"
  "TC_BUILD_ID=$TC_BUILD_ID"
  "TC_SERVER_URL=$TC_SERVER_URL"
  "COCKROACH_NIGHTLY_STRESS=true"
  "PKG=$PKG"
  "GOFLAGS=${GOFLAGS:-}"
  "TAGS=${TAGS:-}"
  "STRESSFLAGS=${STRESSFLAGS:-}"
)

build/builder.sh env "${env[@]}" bash <<'EOF'
set -euxo pipefail
go install ./pkg/cmd/github-post
# We're going to run stress, pipe its output to test2json to add a bit of
# structure, then pipe that to our github-post script which creates issues for
# failed tests.
# Note that, even though we stream the results into test2json, this streaming is
# meaningless for the purposes of the timing information recorded by test2json
# because stress captures the test output and prints it all at once. github-post
# compensates for this.
#
# We've set pipefail, so the exit status is going to come from stress if there
# are test failures.
make stress PKG="$PKG" TESTTIMEOUT=40m GOFLAGS="$GOFLAGS" TAGS="$TAGS" STRESSFLAGS="-maxruns 100 -maxfails 1 -stderr $STRESSFLAGS" 2>&1 \
  | tee artifacts/stress.log \
  | go tool test2json -t | github-post
EOF

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
  "TC_BUILD_BRANCH=$TC_BUILD_BRANCH"
  "COCKROACH_NIGHTLY_STRESS=true"
  "PKG=$PKG"
  "GOFLAGS=${GOFLAGS:-}"
  "TAGS=${TAGS:-}"
  "STRESSFLAGS=${STRESSFLAGS:-}"
  "TESTTIMEOUT=${TESTTIMEOUT:-}"
  "TZ=America/New_York"
)

build/builder.sh env "${env[@]}" bash <<'EOF'
set -euxo pipefail
go install ./pkg/cmd/github-post

# We're going to run stress, pipe its output to test2json to add a bit of
# structure, then pipe that to our github-post script which creates issues for
# failed tests.
# Note that we don't stream the results into test2json, as the tool generally
# expects. We don't stream because we want the failing stress output to make it
# to Team City's build log (i.e. to the stdout of this file).
# The streaming would be meaningless for the purposes of the timing information
# recorded by test2json because stress captures the test output and prints it
# all at once. github-post compensates for this.
# TODO(andrei): Something we could do is teach the `make stress` target (or a
# new target) to pass the test2son invocation to the `go test -exec` flag (note
# that test2json knows how to take a test binary and flags). This way we could
# get rid of some logic specific to supporting stress output in github-post.
# But then the problem would be that the TC build log would contain json, which
# is not the most readable. So I guess we'd also need a json2test tool to strip
# the json and go back to raw logs.
#
# We've set pipefail, so the exit status is going to come from stress if there
# are test failures.
# Use an `if` so that the `-e` option doesn't stop the script on error.
if ! stdbuf -oL -eL \
  make stress PKG="$PKG" TESTTIMEOUT="$TESTTIMEOUT" GOFLAGS="$GOFLAGS" TAGS="$TAGS" STRESSFLAGS="-stderr $STRESSFLAGS" 2>&1 \
  | tee artifacts/stress.log; then
  exit_status=${PIPESTATUS[0]}
  go tool test2json -t -p "${PKG}" < artifacts/stress.log | github-post
  exit $exit_status
fi

EOF

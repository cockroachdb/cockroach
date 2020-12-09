#!/usr/bin/env bash
#
# This script is run by the Pebble Nightly Metamorphic - TeamCity build
# configuration.

set -euo pipefail

source "$(dirname "${0}")/teamcity-support.sh"

run export BUILDER_HIDE_GOPATH_SRC=1
run mkdir -p artifacts
definitely_ccache

# Replace the vendored Pebble with the most current SHA,
# with test files too.
rm -fr vendor/github.com/cockroachdb/pebble
git clone https://github.com/cockroachdb/pebble vendor/github.com/cockroachdb/pebble

PEBBLE_SHA=$(git -C vendor/github.com/cockroachdb/pebble rev-parse HEAD)
echo "Pebble SHA: $PEBBLE_SHA"

env=(
  "GITHUB_REPO=$GITHUB_REPO"
  "GITHUB_API_TOKEN=$GITHUB_API_TOKEN"
  "BUILD_VCS_NUMBER=$PEBBLE_SHA"
  "TC_BUILD_ID=$TC_BUILD_ID"
  "TC_SERVER_URL=$TC_SERVER_URL"
  "TC_BUILD_BRANCH=$TC_BUILD_BRANCH"
  "PKG=internal/metamorphic"
  "STRESSFLAGS=-maxtime 1h -maxfails 1 -stderr -p 1"
  "TZ=America/New_York"
)

build/builder.sh env "${env[@]}" bash <<'EOF'
set -euxo pipefail
go install github.com/cockroachdb/stress
go install ./pkg/cmd/github-post
ARTIFACTS_DIR=`pwd`/artifacts/meta
mkdir -p $ARTIFACTS_DIR

# We've set pipefail, so the exit status is going to come from stress if there
# are test failures.
# Use an `if` so that the `-e` option doesn't stop the script on error.
pushd ./vendor/github.com/cockroachdb/pebble/internal/metamorphic
if ! stdbuf -oL -eL \
  go test -mod=vendor -exec "stress ${STRESSFLAGS}" -run 'TestMeta$$' \
  -timeout 0 -tags 'invariants' -test.v \
  -dir $ARTIFACTS_DIR -ops "uniform:5000-25000" 2>&1 | tee $ARTIFACTS_DIR/metamorphic.log; then
    exit_status=${PIPESTATUS[0]}
    go tool test2json -t -p "${PKG}" < $ARTIFACTS_DIR/metamorphic.log | github-post --formatter=pebble-metamorphic
    exit $exit_status
  fi
popd
EOF

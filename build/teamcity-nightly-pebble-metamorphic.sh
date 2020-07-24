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

env=(
  "BUILD_VCS_NUMBER=$BUILD_VCS_NUMBER"
  "TC_BUILD_ID=$TC_BUILD_ID"
  "TC_SERVER_URL=$TC_SERVER_URL"
  "TC_BUILD_BRANCH=$TC_BUILD_BRANCH"
  "PKG=./vendor/github.com/cockroachdb/pebble/internal/metamorphic"
  "STRESSFLAGS=-maxtime 1h -maxfails 1 -stderr -p 1"
  "TZ=America/New_York"
)

build/builder.sh env "${env[@]}" bash <<'EOF'
set -euxo pipefail
go install github.com/cockroachdb/stress

# We've set pipefail, so the exit status is going to come from stress if there
# are test failures.
# Use an `if` so that the `-e` option doesn't stop the script on error.
if ! stdbuf -oL -eL \
  go test -mod=vendor -exec "stress ${STRESSFLAGS}" -run 'TestMeta$$' -timeout 0 -tags 'invariants' -test.v $PKG 2>&1 | tee artifacts/stress.log; then
    exit_status=${PIPESTATUS[0]}
    exit $exit_status
  fi
EOF

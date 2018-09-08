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
make stress PKG="$PKG" TESTTIMEOUT=40m GOFLAGS="$GOFLAGS" TAGS="$TAGS" STRESSFLAGS="-maxruns 100 -maxfails 1 -stderr $STRESSFLAGS" \
  | tee artifacts/stress.log \
  || { go tool test2json < artifacts/stress.log | github-post; exit 1; }
EOF

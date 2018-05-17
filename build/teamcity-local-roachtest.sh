#!/usr/bin/env bash

set -euo pipefail

source "$(dirname "${0}")/teamcity-support.sh"

tc_start_block "Prepare environment"
run mkdir -p artifacts
maybe_ccache
tc_end_block "Prepare environment"

tc_start_block "Install roachprod"
run build/builder.sh go get -u -v github.com/cockroachdb/roachprod
tc_end_block "Install roachprod"

tc_start_block "Compile CockroachDB"
run build/builder.sh make build
tc_end_block "Compile CockroachDB"

tc_start_block "Compile workload/roachtest"
run build/builder.sh make bin/workload bin/roachtest
tc_end_block "Compile workload/roachtest"

tc_start_block "Run local roachtests"
# TODO(dan): Run kv/splits as a proof of concept of running roachtest on every
# PR. After we're sure this is stable, curate a suite of the tests that work
# locally.
run build/builder.sh \
  env \
    GITHUB_API_TOKEN="$GITHUB_API_TOKEN" \
    BUILD_VCS_NUMBER="$BUILD_VCS_NUMBER" \
    TC_BUILD_ID="$TC_BUILD_ID" \
    TC_SERVER_URL="$TC_SERVER_URL" \
    PKG="${PKG:-}" GOFLAGS="${GOFLAGS:-}" TAGS="${TAGS:-}" \
  ./bin/roachtest run kv/splits \
    --local \
    --cockroach "cockroach" \
    --workload "bin/workload" \
    --artifacts artifacts \
    --teamcity
tc_end_block "Run local roachtests"

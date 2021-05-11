#!/usr/bin/env bash

set -euo pipefail

source "$(dirname "${0}")/teamcity-support.sh"

tc_start_block "Prepare environment"
# Grab a testing license good for one hour.
COCKROACH_DEV_LICENSE=$(curl --retry 3 --retry-connrefused -f "https://register.cockroachdb.com/api/prodtest")
run mkdir -p artifacts
maybe_ccache
tc_end_block "Prepare environment"

tc_start_block "Compile CockroachDB"
# Buffer noisy output and only print it on failure.
run build/builder.sh make build &> artifacts/roachtests-compile.log || (cat artifacts/roachtests-compile.log && false)
rm artifacts/roachtests-compile.log
tc_end_block "Compile CockroachDB"

tc_start_block "Compile roachprod/workload/roachtest"
run build/builder.sh make bin/roachprod bin/workload bin/roachtest
tc_end_block "Compile roachprod/workload/roachtest"

tc_start_block "Run local roachtests"
# TODO(peter,dan): curate a suite of the tests that works locally.
# NB: roachtest has its own teamcity output format and posts issues (whenever
# GITHUB_API_TOKEN) is set, so it doesn't fit into the streamlined process
# around the run_json_test helper.
build/builder.sh env \
    COCKROACH_DEV_LICENSE="${COCKROACH_DEV_LICENSE}" \
    GITHUB_API_TOKEN="${GITHUB_API_TOKEN-}" \
    BUILD_VCS_NUMBER="${BUILD_VCS_NUMBER-}" \
    TC_BUILD_ID="${TC_BUILD_ID-}" \
    TC_SERVER_URL="${TC_SERVER_URL-}" \
    TC_BUILD_BRANCH="${TC_BUILD_BRANCH-}" \
  stdbuf -oL -eL \
  ./bin/roachtest run acceptance kv/splits cdc/bank \
  --local \
  --parallelism=1 \
  --cockroach "cockroach" \
  --roachprod "bin/roachprod" \
  --workload "bin/workload" \
  --artifacts artifacts \
  --teamcity
tc_end_block "Run local roachtests"

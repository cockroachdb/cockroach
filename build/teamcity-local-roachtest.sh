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
# TODO(peter,dan): curate a suite of the tests that works locally.
run build/builder.sh ./bin/roachtest run '(acceptance|kv/splits)' \
  --local \
  --cockroach "cockroach" \
  --workload "bin/workload" \
  --artifacts artifacts \
  --teamcity 2>&1 | tee artifacts/roachtest.log
tc_end_block "Run local roachtests"

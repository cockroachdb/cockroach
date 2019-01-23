#!/usr/bin/env bash

set -euo pipefail

source "$(dirname "${0}")/teamcity-support.sh"

tc_start_block "Prepare environment"
run mkdir -p artifacts
maybe_ccache
tc_end_block "Prepare environment"

tc_start_block "Compile CockroachDB"
run build/builder.sh make build
tc_end_block "Compile CockroachDB"

tc_start_block "Compile roachprod/workload/roachtest"
run build/builder.sh make bin/roachprod bin/workload bin/roachtest
tc_end_block "Compile roachprod/workload/roachtest"

tc_start_block "Run local roachtests"
# Grab a testing license good for one hour.
COCKROACH_DEV_LICENSE=$(curl "https://register.cockroachdb.com/api/prodtest")
# TODO(peter,dan): curate a suite of the tests that works locally.
run build/builder.sh env \
  COCKROACH_DEV_LICENSE="$COCKROACH_DEV_LICENSE" \
	stdbuf -oL -eL \
	./bin/roachtest run '(acceptance|kv/splits|cdc/bank)' \
  --local \
  --cockroach "cockroach" \
  --roachprod "bin/roachprod" \
  --workload "bin/workload" \
  --artifacts artifacts \
  --teamcity 2>&1 \
	| tee artifacts/roachtest.log
tc_end_block "Run local roachtests"

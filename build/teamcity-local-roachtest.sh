#!/usr/bin/env bash

set -euo pipefail

source "$(dirname "${0}")/teamcity-support.sh"

tc_start_block "Prepare environment"
run mkdir -p artifacts
maybe_ccache
tc_end_block "Prepare environment"

tc_start_block "Compile CockroachDB"
run script -t5 artifacts/roachtest-compile-cockroach.log \
	build/builder.sh \
	make build
tc_end_block "Compile CockroachDB"

tc_start_block "Compile roachprod/workload/roachtest"
run script -t5 artifacts/roachtest-compile.log \
	build/builder.sh \
	make bin/roachprod bin/workload bin/roachtest
tc_end_block "Compile roachprod/workload/roachtest"

tc_start_block "Run local roachtests"
# TODO(peter,dan): curate a suite of the tests that works locally.
run script -t5 artifacts/roachtest.log \
	build/builder.sh \
	./bin/roachtest run '(acceptance|kv/splits)' \
  --local \
  --cockroach "cockroach" \
  --roachprod "bin/roachprod" \
  --workload "bin/workload" \
  --artifacts artifacts \
  --teamcity
tc_end_block "Run local roachtests"

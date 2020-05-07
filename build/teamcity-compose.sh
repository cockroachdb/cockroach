#!/usr/bin/env bash

set -euo pipefail

source "$(dirname "${0}")/teamcity-support.sh"

tc_prepare

tc_start_block "Prepare environment for compose tests"
# Disable global -json flag.
type=$(GOFLAGS=; go env GOOS)
tc_end_block "Prepare environment for compose tests"

tc_start_block "Compile CockroachDB"
# Buffer noisy output and only print it on failure.
run pkg/compose/prepare.sh &> artifacts/compose-compile.log || (cat artifacts/compose-compile.log && false)
rm artifacts/compose-compile.log
tc_end_block "Compile CockroachDB"

tc_start_block "Compile compose tests"
run build/builder.sh mkrelease "$type" -Otarget testbuild PKG=./pkg/compose TAGS=compose
tc_end_block "Compile compose tests"

tc_start_block "Run compose tests"
run cd pkg/compose
# run_text_test needs ./artifacts to be the artifacts folder.
ln -s ../../artifacts artifacts
run_text_test github.com/cockroachdb/cockroach/pkg/compose ./compose.test -test.v -test.timeout 30m
run cd ../..
tc_end_block "Run compose tests"

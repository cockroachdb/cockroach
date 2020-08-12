#!/usr/bin/env bash

set -euo pipefail

source "$(dirname "${0}")/teamcity-support.sh"

tc_prepare

tc_start_block "Prepare environment for acceptance tests"
# The log files that should be created by -l below can only
# be created if the parent directory already exists. Ensure
# that it exists before running the test.
export TMPDIR=$PWD/artifacts/acceptance
mkdir -p "$TMPDIR"
# Disable global -json flag.
type=$(GOFLAGS=; go env GOOS)
tc_end_block "Prepare environment for acceptance tests"

# TODO(tbg): this is unnecessary, just use the "compile test binary" build step artifact. See:
# https://github.com/cockroachdb/dev-inf/issues/128
tc_start_block "Compile CockroachDB"
# Buffer noisy output and only print it on failure.
run pkg/acceptance/prepare.sh &> artifacts/acceptance-compile.log || (cat artifacts/acceptance-compile.log && false)
rm artifacts/acceptance-compile.log
run ln -s cockroach-linux-2.6.32-gnu-amd64 cockroach  # For the tests that run without Docker.
tc_end_block "Compile CockroachDB"

# We need to compile the test binary because we can't invoke it in builder.sh (recursive use of Docker, though
# actually it might just work - I think I remember "privileged mode" was the sticking point in the past).
tc_start_block "Compile acceptance tests"
run build/builder.sh mkrelease "$type" -Otarget testbuild TAGS=acceptance PKG=./pkg/acceptance
tc_end_block "Compile acceptance tests"

tc_start_block "Run acceptance tests"
# Note the trick here - we're running `go test` but it won't actually invoke the test binary it
# built itself, but the one we built with the acceptance tag. We do this to avoid invoking
# `make` outside of the builder. At the same time, we need to go through `go test` because that
# is the only way to get sane `-json` behavior (test2json has various shortcomings when tests
# time out, etc). So we have it build `emptypkg` (which has no deps, duh) but really will run
# the thing we care about, 'acceptance.test'.
#
# Note that ./pkg/acceptance without the tag is an empty test package, so it's fine to compile
# outside of builder.
run_json_test env TZ=America/New_York stdbuf -eL -oL go test \
  -mod=vendor -json -timeout 30m -v \
	-exec "../../build/teamcity-go-test-precompiled.sh ./pkg/acceptance/acceptance.test" ./pkg/acceptance \
	-l "$TMPDIR"
tc_end_block "Run acceptance tests"

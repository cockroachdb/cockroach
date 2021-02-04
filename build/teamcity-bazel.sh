#!/usr/bin/env bash

set -euo pipefail

source "$(dirname "${0}")/teamcity-support.sh"

tc_prepare

# NB: $root is set by teamcity-support.sh.
export TMPDIR=$root/artifacts
mkdir -p "$TMPDIR"

# Sets `-test.v` in Go tests. (This is a no-op for now since we're not
# running tests with Bazel in CI, but we shortly will, and we'll want to
# make sure this is included when we do.)
# Ref: https://github.com/bazelbuild/rules_go/pull/2456
echo 'test --test_env=GO_TEST_WRAP_TESTV=1' > $root/.bazelrc.user

tc_start_block "Run Bazel build"
docker run -i ${tty-} --rm --init \
       --workdir="/go/src/github.com/cockroachdb/cockroach" \
       -v "$root:/go/src/github.com/cockroachdb/cockroach:ro" \
       -v "$TMPDIR:/artifacts" \
       cockroachdb/bazel:20210126-114949 bazelbuild.sh
tc_end_block "Run Bazel build"

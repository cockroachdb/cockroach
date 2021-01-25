#!/usr/bin/env bash

set -euo pipefail

source "$(dirname "${0}")/teamcity-support.sh"

tc_prepare

# NB: $root is set by teamcity-support.sh.
export TMPDIR=$root/artifacts
mkdir -p "$TMPDIR"

tc_start_block "Run Bazel build"
docker run -i ${tty-} --rm --init \
       --workdir="/go/src/github.com/cockroachdb/cockroach" \
       -v "$root:/go/src/github.com/cockroachdb/cockroach:ro" \
       -v "$TMPDIR:/artifacts" \
       cockroachdb/bazel:20210126-114949 bazelbuild.sh
tc_end_block "Run Bazel build"

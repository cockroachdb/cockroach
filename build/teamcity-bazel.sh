#!/usr/bin/env bash

set -euo pipefail

source "$(dirname "${0}")/teamcity-support.sh"

tc_prepare

export TMPDIR=$root/artifacts
mkdir -p "$TMPDIR"

tc_start_block "Run Bazel build"
docker run -i ${tty-} --rm --init \
       --workdir="/go/src/github.com/cockroachdb/cockroach" \
       -v "$root:/go/src/github.com/cockroachdb/cockroach:ro" \
       -v "$TMPDIR:/artifacts" \
       cockroachdb/bazel:20210125-122708 build/bazelbuild.sh
tc_end_block "Run Bazel build"

#!/usr/bin/env bash

set -euo pipefail

source "$(dirname "${0}")/teamcity-support.sh"
source "$(dirname "${0}")/teamcity-bazel-support.sh"  # For BAZEL_IMAGE

tc_prepare

# NB: $root is set by teamcity-support.sh.
export TMPDIR=$root/artifacts
mkdir -p "$TMPDIR"

# Bazel configuration for CI.
cp $root/.bazelrc.ci $root/.bazelrc.user

tc_start_block "Run Bazel test"
docker run -i ${tty-} --rm --init \
       --workdir="/go/src/github.com/cockroachdb/cockroach" \
       -v "$root:/go/src/github.com/cockroachdb/cockroach:ro" \
       -v "$TMPDIR:/artifacts" \
       $BAZEL_IMAGE bazeltest.sh
tc_end_block "Run Bazel test"

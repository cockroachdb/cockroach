#!/usr/bin/env bash

set -euo pipefail

# Bazel configuration for CI.
cp $root/.bazelrc.ci $root/.bazelrc.user

docker run -i ${tty-} --rm --init \
       --workdir="/go/src/github.com/cockroachdb/cockroach" \
       -v "$root:/go/src/github.com/cockroachdb/cockroach:ro" \
       -v "$TMPDIR:/artifacts" \
       cockroachdb/bazel:20210201-174432 bazelbuild.sh

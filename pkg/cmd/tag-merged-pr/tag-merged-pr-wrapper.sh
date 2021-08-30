#!/usr/bin/env bash

set -uo pipefail

this_dir=$(cd "$(dirname "$0")" && pwd)
cd $this_dir/..
mkdir -p artifacts
bazel build //pkg/cmd/tag-merged-pr &> artifacts/build.log
status=$?
if [ $status -eq 0 ]
then
    $(bazel info bazel-bin)/pkg/cmd/tag-merged-pr/tag-merged-pr_/tag-merged-pr "$@"
else
    echo 'Failed to build pkg/cmd/tag-merged-pr! Got output:'
    cat artifacts/build.log
    exit $status
fi

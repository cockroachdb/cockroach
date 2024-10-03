#!/usr/bin/env bash

# Copyright 2021 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -uo pipefail

this_dir=$(cd "$(dirname "$0")" && pwd)
echo $this_dir
cd $this_dir/..
mkdir -p artifacts
bazel build //pkg/cmd/label-merged-pr &> artifacts/build.log
status=$?
if [ $status -eq 0 ]
then
    $(bazel info bazel-bin)/pkg/cmd/label-merged-pr/label-merged-pr_/label-merged-pr "$@"
else
    echo 'Failed to build pkg/cmd/label-merged-pr! Got output:'
    cat artifacts/build.log
    exit $status
fi

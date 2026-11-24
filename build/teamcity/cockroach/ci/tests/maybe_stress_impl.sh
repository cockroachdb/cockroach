#!/usr/bin/env bash

# Copyright 2021 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -exuo pipefail

if [ -z "$1" ]
then
    echo 'Usage: maybe_stress_impl.sh stress|stressrace'
    exit 1
fi

TARGET="$1"

bazel build //pkg/cmd/github-pull-request-make //pkg/cmd/bazci @com_github_cockroachdb_stress//:stress
BAZEL_BIN=$(bazel info bazel-bin)
PATH=$PATH:$BAZEL_BIN/pkg/cmd/bazci/bazci_:$BAZEL_BIN/external/com_github_cockroachdb_stress/stress_ TARGET=$TARGET \
    $BAZEL_BIN/pkg/cmd/github-pull-request-make/github-pull-request-make_/github-pull-request-make

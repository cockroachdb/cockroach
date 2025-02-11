#!/bin/bash

# Copyright 2025 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

set -euxo pipefail

bazel build --config=crosslinux $(./build/github/engflow-args.sh) \
  --jobs 100 \
  --bes_keywords integration-test-artifact-build \
  //pkg/cmd/microbench-ci

bazel_bin=$(bazel info bazel-bin --config=crosslinux)

"$bazel_bin/pkg/cmd/microbench-ci/microbench-ci_/microbench-ci" "$@"

#!/usr/bin/env bash

# Copyright 2024 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -euxo pipefail

WORKSPACE=$(bazel info workspace)

# GCAssert and unused need generated files in the workspace to work properly.
bazel run //pkg/gen:code \
    --config crosslinux --jobs 100 \
    --remote_download_minimal $(./build/github/engflow-args.sh)
bazel run //pkg/cmd/generate-cgo:generate-cgo \
    --run_under="cd $WORKSPACE && " \
    --config crosslinux --jobs 100 \
    --remote_download_minimal $(./build/github/engflow-args.sh)

bazel test \
  //pkg/testutils/lint:lint_test \
  --config crosslinux \
  --test_env=CC=$(which gcc) \
  --test_env=CXX=$(which gcc) \
  --test_env=HOME \
  --sandbox_writable_path=$HOME \
  --test_env=GO_SDK=$(dirname $(dirname $(bazel run @go_sdk//:bin/go --run_under=realpath))) \
  --test_env=COCKROACH_WORKSPACE=$WORKSPACE \
  --test_timeout=1800 \
  --build_event_binary_file=bes.bin \
  --jobs 100 \
  --remote_download_minimal $(./build/github/engflow-args.sh)

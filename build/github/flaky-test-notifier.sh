#!/usr/bin/env bash

# Copyright 2026 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

set -euxo pipefail

bazel build //pkg/cmd/flaky-test-notifier:flaky-test-notifier \
    --config crosslinux --jobs 50 \
    --bes_keywords flaky-test-notifier \
    $(./build/github/engflow-args.sh)

BAZEL_BIN=$(bazel info bazel-bin --config crosslinux)
"$BAZEL_BIN/pkg/cmd/flaky-test-notifier/flaky-test-notifier_/flaky-test-notifier"

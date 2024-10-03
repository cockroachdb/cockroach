#!/usr/bin/env bash

# Copyright 2021 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -xeuo pipefail

bazel build //pkg/cmd/teamcity-trigger --config=ci
BAZEL_BIN=$(bazel info bazel-bin --config=ci)
$BAZEL_BIN/pkg/cmd/teamcity-trigger/teamcity-trigger_/teamcity-trigger "$@"

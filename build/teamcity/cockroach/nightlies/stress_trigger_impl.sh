#!/usr/bin/env bash

set -xeuo pipefail

bazel build //pkg/cmd/teamcity-trigger --config=ci
BAZEL_BIN=$(bazel info bazel-bin --config=ci)
$BAZEL_BIN/pkg/cmd/teamcity-trigger/teamcity-trigger_/teamcity-trigger "$@"

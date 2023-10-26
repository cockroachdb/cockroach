#!/usr/bin/env bash

set -euo pipefail

export RUNS_PER_TEST=5
export EXTRA_TEST_ARGS="--@io_bazel_rules_go//go/config:race --test_env=GORACE=halt_on_error=1"

THIS_DIR=$(cd "$(dirname "$0")" && pwd)

$THIS_DIR/stress_engflow_impl.sh

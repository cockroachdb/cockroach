#!/usr/bin/env bash

set -euo pipefail

export RUNS_PER_TEST=3
export EXTRA_TEST_ARGS="--define gotags=bazel,gss,deadlock --test_timeout=300,1000,1500,2400 --heavy"
export EXTRA_ISSUE_PARAMS=deadlock

THIS_DIR=$(cd "$(dirname "$0")" && pwd)

$THIS_DIR/stress_engflow_impl.sh

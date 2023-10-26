#!/usr/bin/env bash

set -euo pipefail

export EXTRA_TEST_ARGS="--define gotags=bazel,gss,deadlock"

THIS_DIR=$(cd "$(dirname "$0")" && pwd)

$THIS_DIR/stress_engflow_impl.sh

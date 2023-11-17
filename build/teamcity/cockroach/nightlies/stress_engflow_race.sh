#!/usr/bin/env bash

set -euo pipefail

export RUNS_PER_TEST=3
export EXTRA_TEST_ARGS="--config=race --test_timeout=1800,3600,5395,5395"

THIS_DIR=$(cd "$(dirname "$0")" && pwd)

unset GITHUB_API_TOKEN

$THIS_DIR/stress_engflow_impl.sh

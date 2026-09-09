#!/usr/bin/env bash

# Copyright 2023 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -euo pipefail

export RUNS_PER_TEST=3
export EXTRA_TEST_ARGS="--config=race --test_env=COCKROACH_NIGHTLY_STRESS=true --test_timeout=1200,2500,3200,4600"
export EXTRA_ISSUE_PARAMS=race

# Race runs are what report data races; the non-race runs report everything
# else. Teams that set filter_race_runs_to_race_issues in TEAMS.yaml get
# issues from this run only for the former.
export COCKROACH_FILTER_RACE_RUNS_TO_RACE_ISSUES=1

THIS_DIR=$(cd "$(dirname "$0")" && pwd)

$THIS_DIR/stress_engflow_impl.sh

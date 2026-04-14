#!/usr/bin/env bash

# Copyright 2024 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -euxo pipefail

EXTRA_PARAMS=""

if [[ "$GITHUB_ACTIONS_BRANCH" == "staging" || "$GITHUB_ACTIONS_BRANCH" == trunk-merge/* ]]; then
  # enable up to 1 retry (2 attempts, worst-case) per test executable to report flakes but only on release branches and staging.
  EXTRA_PARAMS=" --flaky_test_attempts=2"
fi

# Determine test targets: use affected targets if a base SHA is provided,
# otherwise fall back to the full test suite.
TEST_TARGETS="//pkg:all_tests"
if [ -n "${BASE_SHA:-}" ]; then
  AFFECTED=$(./build/github/affected-targets.sh "$BASE_SHA")
  if [ -z "$AFFECTED" ]; then
    echo "No test-relevant files changed, skipping unit tests."
    exit 0
  elif [ "$AFFECTED" != "FULL" ]; then
    TEST_TARGETS="$AFFECTED"
  fi
fi

bazel test $TEST_TARGETS //pkg/ui:lint //pkg/ui:test \
    --config crosslinux --jobs 200 --remote_download_minimal \
    --bes_keywords ci-unit-test --config=use_ci_timeouts \
    --build_event_binary_file=bes.bin $(./build/github/engflow-args.sh) \
    $EXTRA_PARAMS
